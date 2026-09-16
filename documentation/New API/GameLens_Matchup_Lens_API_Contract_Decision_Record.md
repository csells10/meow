# GameLens Matchup Lens API Contract Decision Record

**Status:** post-freeze amendment recorded; implementation repair required; Chunk E blocked  
**Contract:** `matchup_lens_v1`  
**Branch:** `feature/matchup-lens-api`  
**Starting commit:** `08192f10a2cfd07d2b3b711f6265607e60ef70b9`  
**Chunk:** C only  
**Assigned model / effort:** `gpt-5.6-sol` / High  
**Post-freeze amendment:** 2026-09-16 — live BigQuery/Lovable smoke repair contract

## 1. Decision summary

This record freezes the smallest implementable contract for:

```http
GET /game/<encoded-game-id>/lens-context
```

The endpoint is authenticated, read-only, game-ID authoritative, dynamically cataloged, pregame-safe, and limited to the canonical away and home teams. It does not calculate new lens scores, modify the six existing formulas, or alter `/game/<game_id>`.

The endpoint returns `200` with `available: false` for expected evidence absence, `400` for a structurally invalid ID, `404` for an unknown game, `409` for evidence that exists but fails an integrity or safety check, `504` for an upstream deadline, and `500` for an unexpected failure. Existing Firebase authentication owns `401` and `403` bodies.

A post-freeze amendment was required after a live read-only smoke request failed with `409 INVALID_METRIC_EVIDENCE`. Verified production evidence includes the transported signal `context`, and the production `numerator`/`denominator` values are provenance strings unused by Lovable. The amendment accepts `context` without making it scoring-eligible and removes those two provenance fields from the response contract rather than coercing them.

One additional endpoint-specific raw ranking read is required before any metric-keyed dictionary is trusted. It provides the runtime league catalog and preserves raw canonical-team rows long enough to detect duplicates. A separate source-aligned windowed read supplies `games_in_window` and `latest_included_game_id` for the exact source date of each selected ranking row.

The response contains only the two scheduled teams. Because the current frontend computes lens standings across `LensSnapshot.teams`, the later adapter must suppress League Standing and trace ordinal/rank output. It must never display “out of 2.” This is intentionally narrower than adding a league-wide evidence product.

## 2. Accepted evidence basis

Chunk C accepts Chunk A and Chunk B without rerunning or extending their discovery.

| Evidence | Accepted result |
|---|---|
| Plan sample / registry / code-eligible / live ranking counts | 59 / 73 / 70 / 63 |
| Canonical matchup | DET `11` at BUF `4`, game `20260917_DET@BUF` |
| BUF / DET / shared metric counts | 63 / 62 / 62 |
| DET-only gap | `fourth_down_pct` |
| Ranking date | `2026-09-14` |
| Source-data date | `2026-09-13` for every selected DET/BUF row |
| Source-aligned games | 1 per team |
| Latest included games | DET `20260913_NO@DET`; BUF `20260913_BUF@HOU` |
| Selected duplicate ranking grain | 0 groups at the accepted snapshot |
| Live selected tag vocabulary | 90 tags; all current frontend-required tags present |
| Lens scoring | Six ANY-tag weighted means; strong/supporting 2/1; `volume-sensitive` ×0.5; `volatility` ×0.75 |
| Exclusions | Global `rare-event`; existing per-lens exclusions retained |
| Missing percentile behavior | Skip; never zero-fill; renormalize remaining weights |
| Minimum score evidence | One eligible numeric metric |
| Percentile contract | Polarity-corrected 0–100 |
| Valid transported signal strengths | `strong`, `supporting`, `context` |
| Live smoke result | `409 INVALID_METRIC_EVIDENCE`; contract/implementation mismatch, not an accepted endpoint pass |
| Live raw/helper agreement | Canonical raw and helper metric keys match; no catalog definition conflicts |

None of 59, 63, 70, or 73 is a runtime allowlist or fixed denominator.

## 3. Request contract

### 3.1 Authentication

The handler uses the existing `@require_firebase_auth` decorator. It introduces no second authentication or authorization policy.

### 3.2 Structural game-ID validation

After URL decoding, the handler accepts only a Python string matching:

```text
^[0-9]{8}_[A-Z0-9]{2,4}@[A-Z0-9]{2,4}$
```

The decoded value must be at most 32 characters. This validation is only a shape and resource-boundary check. The handler must not parse or trust the date or team abbreviations in the ID. `get_game_header(game_id)` remains the authority for existence, date, season, phase, and both teams.

A URL with no `<game_id>` path segment does not match this endpoint and remains Flask's ordinary route-level `404`; it is not an `INVALID_GAME_ID` response from this handler.

### 3.3 Canonical execution order

1. Authenticate through the existing decorator.
2. Validate the decoded game-ID shape.
3. Call `get_game_header(game_id)` and stop with `GAME_NOT_FOUND` if empty.
4. Call `select_window_type(header)`.
5. Run the endpoint-specific raw ranking boundary read for the exact latest `as_of_date < game_date`, selected window, canonical season, and canonical team IDs. Keep rows as rows until duplicate and definition checks finish.
6. If the raw boundary passes, call `get_team_rankings_for_game(game_id)` without a metric allowlist. Compare its team keys and counts with the validated raw rows before serialization.
7. Run the source-aligned windowed lookup for every selected canonical-team ranking row.
8. Validate dates, window, teams, catalog definitions, numbers, tags, signal strength, coverage, and source alignment.
9. Construct the deterministic response. Never return partially validated metric evidence.

## 4. State and HTTP matrix

All messages below are frozen, safe, and free of internal exceptions, SQL, table contents, credentials, or stack traces.

| State | HTTP | `available` | Reason code | Frozen message | `game` | All evidence fields |
|---|---:|---:|---|---|---|---|
| Complete success | 200 | true | null | null | populated | Fully populated |
| Partial lens evidence | 200 | true | null | null | populated | Fully populated plus readiness/warnings |
| Expected no ranking evidence | 200 | false | `NO_EVIDENCE` | `Matchup Lens evidence is not available for this game.` | populated | Cleared as defined below |
| One scheduled team has no rows | 200 | false | `MISSING_TEAM_EVIDENCE` | `Matchup Lens evidence is unavailable because one scheduled team has no ranking evidence.` | populated | Cleared as defined below |
| Exact historical window row unavailable | 200 | false | `SOURCE_ALIGNMENT_UNAVAILABLE` | `Matchup Lens evidence is unavailable because its exact historical window context is missing.` | populated | Cleared as defined below |
| Structurally invalid ID | 400 | false | `INVALID_GAME_ID` | `Game ID is missing or structurally invalid.` | null | Cleared as defined below |
| Unknown game | 404 | false | `GAME_NOT_FOUND` | `No scheduled game exists for this game ID.` | null | Cleared as defined below |
| Unsafe evidence dates | 409 | false | `UNSAFE_EVIDENCE_DATES` | `Matchup Lens evidence failed the pregame date safety check.` | populated | Cleared as defined below |
| Ranking/window mismatch | 409 | false | `WINDOW_MISMATCH` | `Matchup Lens evidence does not match the selected game window.` | populated | Cleared as defined below |
| Window counts/latest game disagree | 409 | false | `SOURCE_ALIGNMENT_CONFLICT` | `Matchup Lens evidence has conflicting historical window context.` | populated | Cleared as defined below |
| Duplicate raw ranking grain | 409 | false | `DUPLICATE_RANKING_ROWS` | `Matchup Lens evidence contains duplicate ranking rows.` | populated | Cleared as defined below |
| Invalid metric/catalog field | 409 | false | `INVALID_METRIC_EVIDENCE` | `Matchup Lens evidence contains invalid metric data.` | populated | Cleared as defined below |
| Upstream deadline | 504 | false | `UPSTREAM_TIMEOUT` | `Matchup Lens evidence could not be loaded before the request timed out.` | null | Cleared as defined below |
| Unexpected server failure | 500 | false | `UNEXPECTED_SERVER_ERROR` | `Matchup Lens evidence could not be loaded.` | null | Cleared as defined below |

For every non-auth `available: false` body, `display` and `basis` are null, `metric_catalog` is `[]`, both `teams` entries are null, and `coverage` is null. The `game` column above is the only state-dependent evidence field. `league_context` and `method` remain the frozen literal objects.

Authentication responses are owned by the existing decorator and intentionally do not use the lens envelope:

| State | HTTP | Exact body |
|---|---:|---|
| Missing bearer token | 401 | `{"error":"unauthorized","message":"Missing Authorization Bearer token"}` |
| Invalid/expired token | 401 | `{"error":"unauthorized","message":"Invalid or expired Firebase token"}` |
| Token has no email | 403 | `{"error":"forbidden","message":"Firebase token does not include an email address"}` |
| User not allowed/active | 403 | `{"error":"forbidden","message":"This Google account is not allowed to access GameLens"}` |

`409` is reserved for evidence that cannot be safely or deterministically represented. Expected absence is not a conflict and stays `200`/`available: false`.

## 5. Exact `matchup_lens_v1` schema

### 5.1 Type notation

- `ISODate` is a string matching `^[0-9]{4}-[0-9]{2}-[0-9]{2}$` and representing a real calendar date.
- `FiniteNumber` is a JSON number for which `math.isfinite(value)` is true. Booleans are not numbers.
- `NonNegativeInteger` is an integer `>= 0`; booleans are rejected.
- `PositiveInteger` is an integer `>= 1`; booleans are rejected.
- `Side` is `"away" | "home"`.
- `LensStatus` is `"complete" | "partial" | "unavailable"`.
- Every field shown below is required. Nullability is expressed explicitly with `| null`. There are no optional keys in the v1 envelope.

```ts
type ReasonCode =
  | "NO_EVIDENCE"
  | "MISSING_TEAM_EVIDENCE"
  | "SOURCE_ALIGNMENT_UNAVAILABLE"
  | "INVALID_GAME_ID"
  | "GAME_NOT_FOUND"
  | "UNSAFE_EVIDENCE_DATES"
  | "WINDOW_MISMATCH"
  | "SOURCE_ALIGNMENT_CONFLICT"
  | "DUPLICATE_RANKING_ROWS"
  | "INVALID_METRIC_EVIDENCE"
  | "UPSTREAM_TIMEOUT"
  | "UNEXPECTED_SERVER_ERROR";

interface MatchupLensReason {
  code: ReasonCode;
  message: string;
}

interface MatchupLensTeamIdentity {
  team_id: string;
  team_abv: string;
  logo_url: string | null;
}

interface MatchupLensGame {
  game_id: string;
  game_date: ISODate;
  game_time: string | null;
  game_status: string | null;
  season: string;
  game_week: string | null;
  season_type: string;
  away_team: MatchupLensTeamIdentity;
  home_team: MatchupLensTeamIdentity;
}

interface MatchupLensDisplay {
  window_label: string;
  games_label: string;
  context_label: string;
}

interface MatchupLensBasis {
  window_type:
    | "preseason_to_date"
    | "regular_season_to_date"
    | "regular_plus_postseason_to_date";
  as_of_date: ISODate;
  source_data_dates: ISODate[];
  max_data_lag_days: NonNegativeInteger;
  pregame_safe: true;
  comparison_not_forecast: true;
  rankings_source: string;
  window_source: string;
}

type SignalStrength = "strong" | "supporting" | "context";

interface MatchupLensMetric {
  metric: string;
  value: FiniteNumber | null;
  label: string;
  definition: string | null;
  category: string | null;
  core_area: string | null;
  comparison_direction: string | null;
  higher_is_better: boolean | null;
  raw_or_derived: string | null;
  aggregation_method: string | null;
  format: string | null;
  decimals: NonNegativeInteger | null;
  notes: string | null;
  ranking_usage: string | null;
  signal_strength: SignalStrength;
  edge_language_allowed: boolean | null;
  include_in_core_area_advantage: boolean | null;
  confidence_eligible: boolean | null;
  data_quality_status: string | null;
  lens_tags: string[];
  league_rank: PositiveInteger | null;
  league_percentile: FiniteNumber | null;
  tier: string | null;
  tier_label: string | null;
  teams_ranked: PositiveInteger | null;
  ranking_kind: string | null;
  rank_direction: string | null;
  rank_interpretation: string | null;
  rank_tie_method: string | null;
  source_data_date: ISODate;
  data_lag_days: NonNegativeInteger;
}

interface MatchupLensTeam {
  team_id: string;
  team_abv: string;
  games_in_window: NonNegativeInteger;
  latest_included_game_id: string | null;
  latest_source_date: ISODate;
  data_lag_days: NonNegativeInteger;
  metrics: Record<string, MatchupLensMetric>;
}

interface LensSideReadiness {
  status: LensStatus;
  catalog_eligible_metric_count: NonNegativeInteger;
  eligible_numeric_metric_count: NonNegativeInteger;
  missing_metrics: string[];
}

interface LensReadiness {
  lens_key:
    | "explosiveness"
    | "drive-control"
    | "scoring-finish"
    | "defensive-resistance"
    | "disruption-protection"
    | "turnover-balance";
  display_name: string;
  away: LensSideReadiness;
  home: LensSideReadiness;
  comparison_status: LensStatus;
}

interface NamedMetricCoverage {
  consumers: string[];
  available_away_metrics: string[];
  available_home_metrics: string[];
  missing_away_metrics: string[];
  missing_home_metrics: string[];
}

interface MatchupLensWarning {
  code:
    | "PARTIAL_LENS_EVIDENCE"
    | "ASYMMETRIC_LENS_EVIDENCE"
    | "UNAVAILABLE_LENS_EVIDENCE"
    | "MULTIPLE_SOURCE_DATES"
    | "NAMED_METRIC_GAPS"
    | "LEAGUE_RANK_OUTPUT_SUPPRESSED";
  message: string;
  lens_key: string | null;
  team_side: Side | null;
  metrics: string[];
}

interface MatchupLensCoverage {
  catalog_metric_count: NonNegativeInteger;
  away_metric_count: NonNegativeInteger;
  home_metric_count: NonNegativeInteger;
  shared_metric_count: NonNegativeInteger;
  missing_away_metrics: string[];
  missing_home_metrics: string[];
  lens_readiness: LensReadiness[];
  named_metric_coverage: NamedMetricCoverage;
  warnings: MatchupLensWarning[];
}

interface MatchupLensLeagueContext {
  mode: "suppressed";
  reason_code: "LEAGUE_CONTEXT_NOT_INCLUDED";
  message: "League-wide evidence is not included; League Standing and trace rank output must be hidden.";
}

interface MatchupLensMethod {
  selection: "Latest phase-appropriate ranking snapshot strictly before the scheduled game date.";
  frontend_role: "Existing Matchup Lens formulas transform this evidence into lens scores and comparison language.";
  forecast: false;
}

interface MatchupLensV1Response {
  schema_version: "matchup_lens_v1";
  available: boolean;
  reason: MatchupLensReason | null;
  game: MatchupLensGame | null;
  display: MatchupLensDisplay | null;
  basis: MatchupLensBasis | null;
  metric_catalog: string[];
  teams: {
    away: MatchupLensTeam | null;
    home: MatchupLensTeam | null;
  };
  coverage: MatchupLensCoverage | null;
  league_context: MatchupLensLeagueContext;
  method: MatchupLensMethod;
}
```

### 5.2 Envelope invariants

- `available: true` requires `reason: null` and non-null `game`, `display`, `basis`, both teams, and `coverage`.
- `available: false` requires non-null `reason`. `display` and `basis` are null, `metric_catalog` is `[]`, both team entries are null, and `coverage` is null. `game` is populated exactly for the three expected evidence states and five `409` states in §4; it is null for `400`, `404`, `500`, and `504`.
- `league_context` and `method` are always present and exactly match their literal values above.
- Team IDs are canonical decimal strings. The adapter converts them to finite safe integers and must reject conversion failure.
- The key of every `metrics` entry equals its nested `.metric` value.
- `latest_included_game_id` is null only when `games_in_window` is zero; it is required when the count is positive.

## 6. Runtime catalog and coverage denominators

The runtime `metric_catalog` is the lexicographically sorted set of distinct, non-empty metric names present anywhere in the league ranking table at the exact selected `as_of_date` and `window_type`. It is not the Endpoint Plan's 59-name sample, the 73-name registry, the 70 code-eligible set, or a fixed 63-name snapshot.

The endpoint-specific raw-boundary read must also establish one consistent catalog definition per metric for the transported fields `label`, `signal_strength`, and `lens_tags`. Conflicting values across raw rows for the same metric produce `409 INVALID_METRIC_EVIDENCE`; the builder must not pick an arbitrary row.

Coverage is defined as follows:

- `catalog_metric_count`: size of `metric_catalog`.
- `away_metric_count`: distinct validated metric keys returned for the canonical away team, including metrics whose percentile is null.
- `home_metric_count`: the equivalent home count.
- `shared_metric_count`: size of the intersection of away and home metric-key sets.
- `missing_away_metrics`: `metric_catalog - away_metric_keys`.
- `missing_home_metrics`: `metric_catalog - home_metric_keys`.

The catalog and every top-level missing list are ordered lexicographically. Counts are computed after raw duplicate validation, never from an overwrite-prone dictionary alone. These full catalog and team coverage values include valid `context` metrics. The verified live inventory therefore retains its dynamic 63/62/62 semantics for BUF/DET/shared metric keys; none of those counts is a fixed denominator.

## 7. Raw duplicate protection

Chunk A's zero-duplicate result is accepted as a one-time observation, not a runtime guarantee. `get_team_rankings_for_game()` creates dictionaries keyed by metric and can overwrite duplicates before the serializer sees them.

Chunk D must add one endpoint-specific, read-only raw ranking boundary query/job. It must:

- select the same exact `season`, `as_of_date`, `window_type`, and canonical team IDs used by the endpoint;
- retain canonical-team ranking rows without dictionary reshaping;
- make the league-wide runtime catalog and its adapter fields available in the same read;
- test uniqueness at `season + as_of_date + window_type + metric + team_id` before calling or trusting the dictionary helper;
- return `409 DUPLICATE_RANKING_ROWS` if any selected canonical-team grain count exceeds one;
- compare validated raw canonical-team key sets/counts with the later helper result.

This is an additional read. Existing `get_team_rankings_for_game()` must not be edited to change its return shape or behavior.

## 8. Source-aligned games lookup

For every validated away/home ranking row, the windowed lookup must match all of:

- canonical `team_id` from the game header;
- exact `metric`;
- canonical `season`;
- selected `window_type`;
- `data_date = ranking.source_data_date`.

It must not select merely the latest windowed row before the target game. It must never attach a newer pregame count to older ranking evidence.

For each team:

- every ranking metric must have exactly one matching historical windowed row;
- every matched row must provide a non-negative integer `games_in_window`;
- all matched rows must agree on `games_in_window`;
- all matched rows must agree on `latest_included_game_id` after normalizing blank values to null;
- `latest_included_game_id` must be non-null when `games_in_window > 0`.

Any missing exact row yields `200 SOURCE_ALIGNMENT_UNAVAILABLE`. More than one exact row for a team/metric or disagreement across a team's matched rows yields `409 SOURCE_ALIGNMENT_CONFLICT`.

Team freshness fields are deterministic:

- `latest_source_date` is the maximum source date across that team's returned metrics.
- `data_lag_days` is the maximum calendar-day difference `as_of_date - source_data_date` across that team's returned metrics.
- `basis.source_data_dates` is the sorted distinct union across both teams.
- `basis.max_data_lag_days` is the maximum team lag.
- Multiple safe source dates do not fail the response, but add `MULTIPLE_SOURCE_DATES`.

Every source date must be `<= as_of_date` and `< game_date`; `as_of_date` must be `< game_date`. Failure yields `409 UNSAFE_EVIDENCE_DATES`.

## 9. Six-lens readiness and ownership

The frontend remains the sole owner of numeric lens scores, weighting, winner selection, Biggest Edge, observations, and comparison language. The backend calculates no score and creates no formula.

The backend owns only readiness metadata, using the exact existing eligibility rules accepted from Chunk B:

| Order | Lens key | Display | ANY include tags | Additional excludes |
|---:|---|---|---|---|
| 1 | `explosiveness` | Explosiveness | `explosiveness`, `offensive-efficiency`, `passing-efficiency`, `rushing-efficiency` | none |
| 2 | `drive-control` | Drive Control | `drive-sustainability`, `third-down`, `fourth-down`, `drive-efficiency`, `drive-conversion` | none |
| 3 | `scoring-finish` | Scoring Finish | `scoring-efficiency`, `scoring`, `touchdowns`, `red-zone`, `touchdown-efficiency`, `efficiency` | none |
| 4 | `defensive-resistance` | Defensive Resistance | `defense`, `scoring-suppression`, `scoring-efficiency-allowed` | `defensive-scoring`, `swing-play` |
| 5 | `disruption-protection` | Disruption & Protection | `disruption`, `negative-plays`, `protection`, `pressure-allowed` | `blocked-kicks`, `special-teams` |
| 6 | `turnover-balance` | Turnover Balance | `turnovers`, `giveaways`, `takeaways`, `takeaway-margin` | none |

`rare-event` is excluded from every lens. Exclusion wins over inclusion. Readiness ignores weights/modifiers because it does not calculate a score.

A metric is readiness-eligible only when its consistent catalog tags pass the existing include/exclude rules **and** `signal_strength` is `strong` or `supporting`. A valid `context` metric remains in transport, `metric_catalog`, team metric maps, and top-level/team coverage, but it is non-scoring and non-readiness-eligible.

For each lens, `catalog_eligible_metric_count` is the number of runtime catalog metrics that pass both the tag rule and the scoring-signal rule. A team metric is numerically eligible only when that readiness-eligible metric is present and has a finite `league_percentile` from 0 through 100. Lens `missing_metrics` is computed only from this readiness-eligible set; `context` metrics are excluded from lens denominators and lens missing lists.

- `complete`: expected count is positive and every expected metric is numerically eligible.
- `partial`: at least one but fewer than all expected metrics are numerically eligible.
- `unavailable`: the expected count is zero or the team has zero numerically eligible metrics.
- `comparison_status` is `unavailable` if either side is unavailable, `complete` if both sides are complete, otherwise `partial`.

Partial status does not change the existing score. The frontend continues to skip missing/null inputs and renormalize surviving weights, but must display the partial/asymmetric warning. An unavailable side produces no comparison for that lens. No missing value is converted to zero.

For the accepted DET/BUF evidence, missing `fourth_down_pct` makes DET Drive Control `partial`, BUF Drive Control `complete`, and the comparison `partial`. Drive Control remains calculable from DET's remaining eligible metrics. This preserves the established formula while ending the silent asymmetric denominator.

## 10. Adapter-required fields and display rules

The later adapter maps:

- canonical `team_id` and `team_abv` from `game`/`teams`, never URL `a` or `b`;
- only metrics whose `signal_strength` is `strong` or `supporting` into `MetricDefinition`, mapping `metric`, `label`, `signal_strength`, and `lens_tags`;
- only finite, in-range, non-null `league_percentile` values for those same strong/supporting metrics into `TeamMetricRow.percentiles`;
- `basis.as_of_date` into `LensSnapshot.asOfDate`;
- backend `display.window_label`, `games_label`, and `context_label` directly;
- team `latest_source_date` and `data_lag_days` directly.

The backend derives display values exactly:

- `window_label`: `Preseason to date`, `Regular season to date`, or `Regular season + postseason to date` according to `window_type`.
- `games_label`: `<AWAY>: <n> game|games | <HOME>: <n> game|games`, with singular only for 1. Example: `DET: 1 game | BUF: 1 game`.
- `context_label`: `Pregame evidence through <as_of_date>; comparison, not forecast.`

The adapter must omit a metric key from `percentiles` when `league_percentile` is null. It must omit `context` metrics from both frontend definitions and percentiles, never coerce `context` to another signal, and never send numeric weights. The existing 2/1 weights and 0.5/0.75 modifiers remain frontend-owned. It must not map null to zero.

## 11. Numeric and field validation

- `league_percentile = null`: valid transport value; omitted from adapted percentiles and reflected in readiness.
- `league_percentile = NaN` or ±Infinity: `409 INVALID_METRIC_EVIDENCE`.
- `league_percentile < 0` or `> 100`: `409 INVALID_METRIC_EVIDENCE`.
- Raw `value`: finite JSON number or null; no 0–100 bound.
- `numerator` and `denominator` are not fields of `MatchupLensMetric`. Their live production values are provenance strings unused by Lovable; the endpoint must not transport, numerically validate, or coerce them.
- `signal_strength`: exactly `strong`, `supporting`, or `context`; null, unknown, or differently cased values fail with `409 INVALID_METRIC_EVIDENCE`.
- `lens_tags`: an array of non-empty strings. Trim, de-duplicate, and sort; a null/non-array/member of another type fails with `409 INVALID_METRIC_EVIDENCE`.
- `games_in_window`, `data_lag_days`, and `decimals`: integer bounds defined by the schema; booleans fail.
- `league_rank` and `teams_ranked`: positive integers or null. If both are present, `league_rank <= teams_ranked`.
- `team_id`, `metric`, `label`, and `source_data_date`: non-empty and type-valid.
- All serialized numeric values must pass `math.isfinite`; JSON serialization must use `allow_nan=False`.

## 12. League-standing blocker

### Options evaluated

| Option | Benefit | Cost/risk | Decision |
|---|---|---|---|
| Return league-wide percentile context | Preserves current rank output | Broadens the endpoint from two teams to a league data product; requires more freshness/count semantics and a materially larger read/payload | Not selected for v1 |
| Return explicit precomputed league-standing metadata | Small response | Requires backend duplication of the frontend's six scoring formulas or a new frontend ranking contract | Rejected |
| Suppress only rank-dependent output | Smallest honest slice; no new formula or league query | League Standing and trace ordinal text are temporarily absent | Selected |

### Frozen behavior

`matchup_lens_v1` contains only away and home evidence and always carries `league_context.mode = "suppressed"`. The adapter may create a two-team `LensSnapshot` for existing scoring, but the later UI wiring must:

- hide League Standing output derived from `LensSnapshot.teams`;
- hide ordinal and `out of N` trace-rank output;
- keep non-rank trace evidence, cards, tabs, constellation, lens scores, Biggest Edge, observations, comparison language, and `view` navigation unchanged;
- never substitute per-metric `league_rank` for a lens-level league standing;
- never display a rank calculated across only DET and BUF.

This is a bounded correctness guard, not a Matchup Lens redesign. A later version may add league context under a separately approved contract.

## 13. Collision and Turnover Watch coverage

These features are metric-name consumers, not six-lens tag scores. Their fixed consumer set is recorded separately and does not filter the endpoint's dynamic metric selection:

1. `sacks_taken`
2. `sack_yards_lost`
3. `sacks`
4. `interceptions_thrown`
5. `fumbles_lost`
6. `turnovers`
7. `defensive_interceptions`
8. `fumbles_recovered`
9. `points_per_play`
10. `td_rate`
11. `red_zone_efficiency`
12. `points_allowed_per_play`

Because Chunk B established the combined consumer set but not a new backend-owned grouping/formula for each feature, the endpoint must not invent feature scores or declare either feature globally ready. It returns `named_metric_coverage` for every consumer and side, using presence of a finite 0–100 percentile as availability. The frontend retains its existing per-feature coverage rules. Missing named consumers add `NAMED_METRIC_GAPS` and do not change six-lens readiness.

## 14. Deterministic serialization and ordering

The serializer emits top-level fields in the interface order in §5. Within objects, fields follow their interface order.

- `metric_catalog`: lexicographic metric order.
- `teams`: `away`, then `home`.
- `teams.<side>.metrics`: keys in `metric_catalog` order, omitting absent team metrics.
- `lens_tags`: trimmed, de-duplicated, lexicographic.
- `source_data_dates`: ascending ISO date.
- `lens_readiness`: the six fixed lens rows in §9 order.
- Named consumer arrays: the fixed order in §13.
- Other missing/available metric arrays: catalog order.
- Warnings: sort by `code`, then null-as-empty `lens_key`, `team_side`, and joined `metrics`.

Serialization uses UTF-8 JSON, `ensure_ascii=False`, compact separators, no NaN/Infinity, and a trailing newline. The same validated input must produce byte-identical output.

## 15. Later frontend expectations

- Initial loading: live skeleton/shell only; no preseason or prior-game values.
- Invalid/unknown game: controlled unavailable screen from `400`/`404`; no static fallback.
- `available: false`: show the safe reason and canonical game context when supplied by the state matrix; do not score.
- Partial lens: keep the existing renormalized score, visibly label partial/asymmetric evidence, and never zero-fill.
- Authentication failure: preserve existing 401 sign-out/sign-in behavior; no retry loop.
- Access denied: preserve the safe 403 state; no retry loop.
- Timeout: treat `504` as retryable and show the safe timeout message.
- Retry: at most one automatic retry for network/`500`/`504`; no automatic retry for `400`, `401`, `403`, `404`, `409`, or `200 available:false`; retain inline manual retry for transient failure and unavailable evidence.
- Game change: query key `['matchup-lens-context', gameId]`; no `keepPreviousData`, no placeholder evidence, and `meta: { persist: false }`.
- Current API primitives have no client timeout/AbortSignal. Chunk D adds no frontend/client timeout. Backend upstream timeout mapping is in scope; active cancellation remains deferred.
- Static data remains only an explicitly labeled manual/development mode when no live game ID is requested.

## 16. Synthetic fixtures

**Every value in this section is synthetic. None is a live observation.** The fictional `20990101_AAA@BBB` examples exist only to freeze test behavior.

### 16.1 Synthetic successful/complete core

The full response must include all fields from §5. This compact fixture shows the minimum evidence-bearing portions; E's file fixture must expand all six `lens_readiness` entries and every required nullable metric field.

```json
{
  "schema_version": "matchup_lens_v1",
  "available": true,
  "reason": null,
  "game": {
    "game_id": "20990101_AAA@BBB",
    "game_date": "2099-01-01",
    "game_time": "1:00p",
    "game_status": "Scheduled",
    "season": "2098",
    "game_week": "Week 18",
    "season_type": "Regular Season",
    "away_team": {"team_id": "101", "team_abv": "AAA", "logo_url": null},
    "home_team": {"team_id": "202", "team_abv": "BBB", "logo_url": null}
  },
  "display": {
    "window_label": "Regular season to date",
    "games_label": "AAA: 1 game | BBB: 1 game",
    "context_label": "Pregame evidence through 2098-12-29; comparison, not forecast."
  },
  "basis": {
    "window_type": "regular_season_to_date",
    "as_of_date": "2098-12-29",
    "source_data_dates": ["2098-12-28"],
    "max_data_lag_days": 1,
    "pregame_safe": true,
    "comparison_not_forecast": true,
    "rankings_source": "synthetic.Analytics.team_metric_rankings_2098",
    "window_source": "synthetic.Analytics.team_metrics_windowed_2098"
  },
  "metric_catalog": ["yards_per_play"],
  "teams": {
    "away": {
      "team_id": "101",
      "team_abv": "AAA",
      "games_in_window": 1,
      "latest_included_game_id": "20981228_AAA@CCC",
      "latest_source_date": "2098-12-28",
      "data_lag_days": 1,
      "metrics": {"yards_per_play": {"metric": "yards_per_play", "league_percentile": 60.0}}
    },
    "home": {
      "team_id": "202",
      "team_abv": "BBB",
      "games_in_window": 1,
      "latest_included_game_id": "20981228_DDD@BBB",
      "latest_source_date": "2098-12-28",
      "data_lag_days": 1,
      "metrics": {"yards_per_play": {"metric": "yards_per_play", "league_percentile": 55.0}}
    }
  },
  "coverage": {
    "catalog_metric_count": 1,
    "away_metric_count": 1,
    "home_metric_count": 1,
    "shared_metric_count": 1,
    "missing_away_metrics": [],
    "missing_home_metrics": [],
    "lens_readiness": [],
    "named_metric_coverage": {
      "consumers": [],
      "available_away_metrics": [],
      "available_home_metrics": [],
      "missing_away_metrics": [],
      "missing_home_metrics": []
    },
    "warnings": [{
      "code": "LEAGUE_RANK_OUTPUT_SUPPRESSED",
      "message": "League Standing and trace rank output are hidden because league-wide evidence is not included.",
      "lens_key": null,
      "team_side": null,
      "metrics": []
    }]
  },
  "league_context": {
    "mode": "suppressed",
    "reason_code": "LEAGUE_CONTEXT_NOT_INCLUDED",
    "message": "League-wide evidence is not included; League Standing and trace rank output must be hidden."
  },
  "method": {
    "selection": "Latest phase-appropriate ranking snapshot strictly before the scheduled game date.",
    "frontend_role": "Existing Matchup Lens formulas transform this evidence into lens scores and comparison language.",
    "forecast": false
  }
}
```

The abbreviated nested metric and readiness values above are documentation shorthand, not a schema-valid production payload. The schema in §5 is authoritative; E must create the complete schema-valid fixture.

### 16.2 Synthetic partial-lens fixture

Build from the complete synthetic success fixture with catalog metrics `fourth_down_pct` and `third_down_pct`. Omit `fourth_down_pct` from the away metric map, retain it for home, and freeze Drive Control as:

```json
{
  "lens_key": "drive-control",
  "display_name": "Drive Control",
  "away": {
    "status": "partial",
    "catalog_eligible_metric_count": 2,
    "eligible_numeric_metric_count": 1,
    "missing_metrics": ["fourth_down_pct"]
  },
  "home": {
    "status": "complete",
    "catalog_eligible_metric_count": 2,
    "eligible_numeric_metric_count": 2,
    "missing_metrics": []
  },
  "comparison_status": "partial"
}
```

The response remains `200`, `available: true`, `reason: null`, and includes `ASYMMETRIC_LENS_EVIDENCE`. This is the synthetic analogue of the accepted DET Drive Control shape; the fixture values are not live DET/BUF values.

### 16.3 Synthetic unavailable/error fixture manifest

Each non-auth fixture uses the full envelope from §5, `metric_catalog: []`, `teams.away/home: null`, `coverage: null`, the literal league/method objects, and the exact state values below. `game`/`basis` presence follows the state matrix.

| Fixture name | HTTP | `available` | Reason code | Synthetic setup |
|---|---:|---:|---|---|
| `no_evidence` | 200 | false | `NO_EVIDENCE` | Known AAA/BBB game; no ranking snapshot |
| `missing_home_team` | 200 | false | `MISSING_TEAM_EVIDENCE` | Raw ranking rows exist only for AAA |
| `source_alignment_unavailable` | 200 | false | `SOURCE_ALIGNMENT_UNAVAILABLE` | One exact AAA metric/date window row absent |
| `invalid_game_id` | 400 | false | `INVALID_GAME_ID` | `not-a-game` |
| `unknown_game` | 404 | false | `GAME_NOT_FOUND` | Valid shape `20990101_AAA@BBB`; empty header lookup |
| `unsafe_evidence_dates` | 409 | false | `UNSAFE_EVIDENCE_DATES` | Synthetic source date equals target game date |
| `window_mismatch` | 409 | false | `WINDOW_MISMATCH` | Ranking row window differs from selected window |
| `source_alignment_conflict` | 409 | false | `SOURCE_ALIGNMENT_CONFLICT` | Same team has conflicting synthetic game counts |
| `duplicate_ranking_rows` | 409 | false | `DUPLICATE_RANKING_ROWS` | Two raw rows at the same protected grain |
| `valid_context_signal_strength` | 200 | true | null | `signal_strength: context`; transported and counted in catalog/team coverage, excluded from lens readiness and later adapter definitions/percentiles |
| `invalid_null_signal_strength` | 409 | false | `INVALID_METRIC_EVIDENCE` | `signal_strength: null` |
| `invalid_unknown_signal_strength` | 409 | false | `INVALID_METRIC_EVIDENCE` | `signal_strength: other` |
| `invalid_cased_signal_strength` | 409 | false | `INVALID_METRIC_EVIDENCE` | `signal_strength: Context` |
| `invalid_nan_percentile` | 409 | false | `INVALID_METRIC_EVIDENCE` | Python input is NaN before JSON serialization |
| `invalid_infinite_percentile` | 409 | false | `INVALID_METRIC_EVIDENCE` | Python input is Infinity before JSON serialization |
| `invalid_low_percentile` | 409 | false | `INVALID_METRIC_EVIDENCE` | `league_percentile: -0.01` |
| `invalid_high_percentile` | 409 | false | `INVALID_METRIC_EVIDENCE` | `league_percentile: 100.01` |
| `upstream_timeout` | 504 | false | `UPSTREAM_TIMEOUT` | Synthetic `DeadlineExceeded` from a read |
| `unexpected_server_error` | 500 | false | `UNEXPECTED_SERVER_ERROR` | Synthetic unexpected exception |

Authentication fixtures use the four exact bodies in §4 and never invoke the endpoint builder. A nullable-percentile fixture must also prove that `league_percentile: null` remains valid, is omitted from adapted percentiles, and yields partial/unavailable readiness rather than `409`.

## 17. Exact Chunk D and Chunk E ownership

### Chunk D may add or edit only

- **Add** `services/matchup_lens_service.py` for validation, response building, deterministic serialization, safe endpoint exceptions, readiness metadata, and display derivation.
- **Edit** `queries/game_queries.py` only to append isolated, read-only endpoint helpers for:
  - the one raw ranking boundary/catalog read;
  - the source-date-aligned windowed lookup.
- **Edit** `routes/game_routes.py` only to add the authenticated `/game/<path:game_id>/lens-context` handler and its service import. Existing `/game/<path:game_id>` behavior must remain byte/semantically unchanged.
- **Add** `documentation/New API/handoffs/Chunk_D_Backend_Implementation_Handoff.md`.

Chunk D must not touch `app.py`; the existing blueprint is already registered. It must not edit existing query functions, route behavior, services, tests, fixtures, runtime configuration, dependencies, pipelines, schemas, tables, schedules, learning/orchestrator code, the Endpoint Plan, Product Roadmap, this decision record, or A/B/C handoffs.

All endpoint-specific test ownership remains with Chunk E. D may run existing checks and perform non-persisted local exercises, but it may not add or edit tests or fixtures.

### Chunk E may add only

- **Add** `tests/test_matchup_lens_endpoint.py`.
- **Optionally add** schema-valid files under `tests/fixtures/matchup_lens/`, with every synthetic file clearly labeled in-file or by fixture metadata as synthetic.
- **Add** `documentation/New API/handoffs/Chunk_E_Verification_Handoff.md`.

Chunk E must not edit implementation files or existing tests. Defects return to D with a narrow reproduction; E then verifies the repair. E must cover handler dispatch versus the existing catch-all game route, authentication, every state in §4, every critical fixture in §16, deterministic bytes, dynamic catalog membership, raw duplicate rejection, source-date alignment, six-lens readiness, named metric coverage, and unchanged existing `/game` and `/games` behavior.

## 18. Boundaries and deferred risks

- No application or test code is changed by Chunk C.
- No query, pipeline, table, schema, schedule, learning system, or orchestrator state is changed by Chunk C.
- No deploy, merge, Lovable contact, or BigQuery write is authorized.
- Client-side AbortSignal/timeout remains deferred; backend upstream timeout mapping is included.
- League Standing and trace rank output remain suppressed until a separately approved league-context contract exists.
- Chunk D must implement this post-freeze amendment. Chunk E remains blocked until that repair is complete and a live read-only BigQuery smoke request succeeds.
- The endpoint's runtime catalog depends on the additional raw ranking read. If its cost or latency is unacceptable in D/E measurement, that is a review blocker; it is not permission to fall back to fixed catalogs or dictionary-only uniqueness claims.

## 19. Post-freeze amendment: live signal and provenance correction

This amendment is prompted by verified live BigQuery and Lovable evidence after the original freeze. The read-only smoke request returned `409 INVALID_METRIC_EVIDENCE`; it did not pass. The aligned evidence was snapshot `2026-09-14`, source date `2026-09-13`, DET 62 metrics, BUF 63 metrics, 62 shared metrics, matching raw/helper keys, and no catalog definition conflicts.

The controlling corrections are exact:

- transported `signal_strength` accepts only `strong`, `supporting`, or `context`;
- null, unknown, and differently cased signals still return `409 INVALID_METRIC_EVIDENCE`;
- `context` is valid transported evidence included in the dynamic catalog, team metric maps, and top-level/team coverage, but excluded from lens eligibility, denominators, and lens missing lists;
- the later adapter creates frontend metric definitions and percentile entries only for `strong`/`supporting`, omits `context`, performs no signal coercion, and sends no numeric weights;
- `numerator` and `denominator` are removed from `MatchupLensMetric` and its exact field order and numeric-validation rules; no other metric field changes;
- live `numerator`/`denominator` values are provenance strings unused by Lovable and must not be coerced;
- the existing frontend weights, modifiers, formulas, and all league-context suppression behavior remain unchanged.

Chunk D must implement these corrections before verification can continue. Chunk E remains blocked until the D repair is complete and the live read-only BigQuery smoke request succeeds. This documentation amendment is a PASS for contract alignment only; it does not claim that the endpoint or BigQuery validation passed.

## 20. Rationale

The frozen design prioritizes honest evidence over apparent completeness. Expected absence remains a usable `200` state, malformed or unsafe evidence cannot masquerade as valid, DET's Drive Control remains calculable without hiding its denominator difference, and the two-team endpoint cannot generate false league ranks. The added reads are the minimum required to support the guarantees already requested: one preserves ranking grain/catalog truth, and one aligns game counts to the exact historical source date.
