# Chunk B — Frontend Contract Handoff

**Status:** complete  
**Branch:** `feature/matchup-lens-api`  
**Starting commit:** `9d6b6c45d104ae79dd0970e67c3861d45ef4a058`  
**Assigned model / effort:** `gpt-5.6-sol` / Medium  
**Scope:** Chunk B only — exact current Lovable frontend contract, read-only documentation

## Outcome

Chunk B is **complete**. Christian sent the bounded question packet to Lovable and returned a detailed read-only inspection of the current source. The adapter's required fields, six-lens scoring and eligibility rules, percentile expectations, authenticated request pattern, cache identity, source-replacement seam, route behavior, and UI-state gaps are explicit enough for Chunk C to freeze the backend response contract without inventing frontend behavior.

Seven findings remain unresolved, but they are contract/product decisions assigned to Chunk C rather than missing frontend-discovery answers. The most material is that the current league-standing helpers rank over every row in `LensSnapshot.teams`; a response containing only DET and BUF would turn league standings into ranks out of two. Chunk C must resolve this before implementation.

No application code, existing test, service, route, query, pipeline, schema, table, scheduled job, learning system, orchestrator state, Endpoint Plan, or Product Roadmap was changed. No deployment, merge, BigQuery write, or direct agent-to-Lovable communication occurred.

## Scope completed

- Read the Endpoint Plan, Product Roadmap, and completed Chunk A handoff completely in the required order.
- Confirmed Chunk A was the only existing handoff at the start of Chunk B.
- Prepared one consolidated question packet for Christian to send to Lovable.
- Analyzed Christian's returned Lovable inspection against the accepted Chunk A evidence.
- Normalized the backend-to-frontend field mapping.
- Recorded the exact six-lens definitions, weights, modifiers, exclusions, and eligibility rules.
- Recorded missing/null behavior, percentile scale, authenticated request pattern, query/cache behavior, static-source seam, URL behavior, and current UI states.
- Separated resolved frontend facts from decisions Chunk C must freeze.

## Accepted Chunk A evidence

Chunk B accepts the following completed evidence basis without re-querying or changing it:

| Evidence | Accepted result |
|---|---|
| Plan sample / registry / code eligible / live ranking counts | 59 / 73 / 70 / 63 |
| BUF coverage | 63 metrics |
| DET coverage | 62 metrics |
| Shared coverage | 62 metrics |
| DET missing metric | `fourth_down_pct` |
| Ranking date | `2026-09-14` |
| Source-data date | `2026-09-13` for every returned DET/BUF metric |
| Games in evidence window | 1 for each team |
| BUF latest included game | `20260913_BUF@HOU` |
| DET latest included game | `20260913_NO@DET` |
| Duplicate ranking-grain groups | 0 |
| Live DET/BUF lens-tag vocabulary | 90 distinct tags |

The endpoint must stay dynamic. None of 59, 63, 70, or 73 is a runtime allowlist.

## Exact Lovable question packet

The following packet was given to Christian for Lovable. The fenced block is the exact request content.

```text
We are preparing a narrow, read-only backend endpoint for the existing GameLens Matchup Lens. This request is frontend contract discovery only.

Do not change any code, create files, implement the endpoint, redesign Matchup Lens, or modify its scoring or UI. Inspect the current Lovable source and report the exact current contract and behavior.

The planned backend endpoint is:

GET /game/<encoded-game-id>/lens-context

It will return versioned `matchup_lens_v1` evidence for the canonical away and home teams. The frontend will eventually transform that evidence through a small adapter into the existing Matchup Lens data structure. The existing six-lens scoring and rendering must remain unchanged.

Accepted backend evidence for the initial verification matchup:

- Game ID: `20260917_DET@BUF`
- Away: DET, canonical team ID `11`
- Home: BUF, canonical team ID `4`
- Window: `regular_season_to_date`
- Ranking `as_of_date`: `2026-09-14`
- Source-data date for every returned DET/BUF metric: `2026-09-13`
- DET: 62 metrics
- BUF: 63 metrics
- Shared metrics: 62
- DET’s only missing metric: `fourth_down_pct`
- BUF missing metrics: none
- Both teams: `games_in_window = 1`
- DET latest included game: `20260913_NO@DET`
- BUF latest included game: `20260913_BUF@HOU`
- Percentiles are currently supplied on a 0–100 scale
- No null or out-of-range percentiles were found in this selected evidence
- No duplicate ranking-grain groups were found
- Every returned metric has at least one lens tag
- The live DET/BUF evidence contains 90 distinct tags

The live tag vocabulary is:

aggression, ball-security, blocked-kicks, coaching-tendency,
combined-source-metric, context, control-profile, cumulative,
data-quality-watch, defense, defensive-exposure, defensive-scoring,
discipline, disruption, drive-context, drive-conversion,
drive-efficiency, drive-killers, drive-sustainability, drive-volume,
efficiency, explosiveness, extra-point-defense, field-goal-defense,
field-position, fourth-down, game-script, giveaways, naming-review,
negative-plays, non-offensive-scoring, offensive-context,
offensive-efficiency, offensive-output, offensive-style,
opponent-penalties, opponent-volume, opportunity, outcome-total,
overlap-risk, pace, pass-heavy, passing-efficiency,
passing-production, passing-td-environment, passing-volume, penalties,
per-game, play-volume, possession, pressure, pressure-allowed,
production, protection, punt-pressure, rare-event, red-zone, risk,
run-heavy, rushing-efficiency, rushing-production,
rushing-td-environment, rushing-volume, safeties, scoring,
scoring-chances, scoring-efficiency, scoring-efficiency-allowed,
scoring-suppression, situational, small-sample, special-teams,
strong-signal, supporting, swing-play, takeaway-margin, takeaways,
td-share, team-wide, third-down, touchdown-efficiency, touchdowns,
turnovers, two-point-conversions, two-point-return, volatility,
volume-sensitive, yardage, yardage-efficiency, yardage-suppression

Please answer every section below from the current source. Include exact file paths and exported symbol, function, hook, or constant names. When requested, provide the exact TypeScript or logic currently in use. If something does not exist or cannot be determined, say “unresolved” rather than proposing a new design.

1. Current data interfaces

Provide the exact current TypeScript definitions for:

- `LensSnapshot`
- The away/home team-row or team-evidence interface used inside it
- The metric-row or metric-evidence interface consumed by lens calculations
- Any lens-result, lens-definition, readiness, warning, or observation interfaces needed by the adapter
- Any supporting types or enums referenced by those interfaces

For each definition, provide:

- Exact source file
- Exact exported or local symbol name
- Exact TypeScript definition
- Which fields are required versus optional
- Which fields permit `null`
- Whether metrics are represented as arrays, records keyed by metric name, or another structure

If `LensSnapshot` is inferred rather than explicitly declared, identify the source value/function that establishes its shape and show the exact inferred structure.

2. Six-lens definitions and formulas

List the six lenses in their exact current display and internal order.

For each lens, provide:

- Display name
- Internal key/ID
- Exact current formula or calculation steps
- Exact metric names directly referenced
- Exact lens tags used for inclusion
- Whether tag matching uses ANY, ALL, prioritized, fallback, or another rule
- Weight assigned to each metric or tag group
- Any normalization or averaging behavior
- Any offense-versus-defense pairing or inversion
- Any directionality handling such as `higher_is_better`
- Any percentile transformation or rescaling
- Any rounding, clamping, threshold, tier, or winner-selection rules
- Any modifiers, bonuses, penalties, caps, or minimum sample rules
- Any special handling based on team side, category, signal strength, ranking usage, or data quality

Quote the exact current constants or functions where practical. Do not summarize a formula in a way that omits operational details.

3. Metric inclusion, exclusion, and eligibility

Describe the exact current eligibility rules used before a metric contributes to a lens.

Confirm whether any of these fields are read and how each affects eligibility or weight:

- `lens_tags`
- `ranking_usage`
- `signal_strength`
- `edge_language_allowed`
- `include_in_core_area_advantage`
- `confidence_eligible`
- `data_quality_status`
- `comparison_direction`
- `higher_is_better`
- `league_percentile`
- `league_rank`
- `teams_ranked`
- `ranking_kind`
- `rank_direction`
- `rank_interpretation`
- `source_data_date`
- `data_lag_days`

Also provide:

- Exact excluded metric names
- Exact excluded tags
- Duplicate/overlap prevention rules
- Whether one metric may contribute to multiple lenses
- Whether unrecognized metrics are ignored, included through tags, or treated as errors
- Whether unrecognized tags are ignored or treated as errors
- Whether eligibility is based on metric names, tags, both, or another structure

Compare the current frontend’s required metric/tag rules with the supplied 90-tag vocabulary and identify any required frontend tag that is absent from that vocabulary.

4. Missing, null, excluded, and warning behavior

For each condition below, state the exact current calculation and UI behavior:

- Metric is entirely absent for one team
- Metric exists for one team but not the other
- Metric value is `null`
- Percentile is `null`
- Rank is `null`
- Metric is excluded
- Metric has a data-quality warning
- Metric is marked small-sample
- Metric has an unrecognized tag
- A lens has only some eligible inputs
- A lens has no eligible inputs
- One entire team row is unavailable

Specifically confirm:

- Whether any missing or null value is converted to zero
- Whether missing inputs are dropped from a denominator
- Whether weights are renormalized after dropping an input
- The exact minimum evidence required to calculate each lens
- The exact condition that makes a lens incomplete or unavailable
- Whether incomplete evidence can still produce Biggest Edge, observations, or comparison language
- Whether warnings are displayed, propagated, ignored, or used to disqualify evidence

Please assess the concrete DET case where `fourth_down_pct` is absent rather than zero. State exactly which lens or lenses would be affected under the current rules and whether each affected lens would remain calculable.

5. Percentile contract

Confirm the exact percentile scale expected by the current Matchup Lens:

- 0–100
- 0–1
- Another scale

Identify:

- The field name the code reads
- Any rescaling currently performed
- Whether a higher percentile always represents better performance
- How inverse or lower-is-better metrics are handled
- Accepted minimum and maximum
- Behavior for out-of-range or non-finite values

The proposed backend supplies `league_percentile` as either `null` or a number from 0 through 100. Confirm whether that can be passed through unchanged.

6. Authenticated API helper and base URL

Identify the established authenticated API request mechanism used elsewhere in the current Lovable application.

Provide:

- Exact source file
- Exact helper/hook/client name
- Exact call pattern from the closest existing authenticated GET request
- How the Firebase/auth token is obtained and attached
- Header names used
- Base-URL environment variable or configuration name
- URL construction pattern
- Response parsing behavior
- Non-2xx error handling
- Authentication-failure handling
- Abort/timeout support
- Retry behavior

Do not expose credential values, secrets, or live tokens. Configuration and environment-variable names are sufficient.

7. Query and cache behavior

Describe the exact current data-fetching and caching approach for Matchup Lens and the closest comparable authenticated page.

Confirm:

- Whether TanStack Query/React Query, SWR, a custom hook, or another mechanism is used
- Exact query/hook name
- Existing query key, if any
- Whether game details are already queried and cached on this page
- Whether the new lens-context request should be independent from the game-details query to avoid changing that existing contract
- The smallest consistent game-ID cache identity

Evaluate this proposed key:

`['matchup-lens-context', gameId]`

Report:

- Whether it matches current project conventions
- Exact recommended key if the convention differs
- `enabled` conditions
- Stale time
- Garbage-collection/cache time
- Retry count and delay
- Refetch-on-focus/reconnect/mount behavior
- Cancellation behavior when `gameId` changes
- How stale data from the previous game is prevented from appearing under a new game ID

8. Current static source and adapter insertion point

Identify the exact current source of Matchup Lens data.

Provide:

- File path
- Symbol name, including `staticLensSnapshotSource` if that is the current name
- How it reaches the scoring/rendering code
- All transformations between the static source and the six-lens calculations
- Whether the static source includes already-computed lens scores or raw metric evidence

Then identify the smallest safe adapter insertion point that would:

- Accept a future `matchup_lens_v1` response
- Produce the exact current `LensSnapshot` shape
- Leave all existing scoring and rendering code unchanged
- Preserve the current cards, tabs, constellation, Biggest Edge, observations, comparison language, and URL `view` handling

State the recommended adapter function signature and location based only on existing architecture. Do not implement it.

Also identify every current use of the static/preseason snapshot that must be prevented from acting as a silent live-game fallback.

9. Route parameters and canonical team handling

Describe how the page currently reads:

- `game`
- `a`
- `b`
- `view`

The existing URL format is:

`/matchup-lens?a=DET&b=BUF&view=overview&game=20260917_DET@BUF`

Confirm:

- Exact router/search-parameter code
- Which parameters currently select data
- Whether `a` and `b` are used only for display or currently affect calculation
- What must change so the endpoint’s canonical game header and team IDs become authoritative
- What current behavior must remain unchanged for `view`
- Behavior when `game` is absent, empty, or malformed

Do not propose parsing team identities out of `gameId`; the backend game header will be authoritative.

10. Loading, unavailable, authentication, timeout, and retry states

Report the exact current components, hooks, or patterns used for each state:

- Initial loading
- Invalid or unknown game
- `available: false`
- Partial/incomplete lens
- Authentication failure
- Access denied
- Request timeout
- General server/network failure
- Manual retry
- Retry exhausted

For each state, provide:

- Existing component or pattern to reuse
- Exact condition that triggers it
- Whether prior/static data remains visible
- Whether an inline retry is already available
- Whether the page redirects, displays an error, or invokes sign-in behavior

Required live behavior for later implementation:

- Never display preseason values while a live request is loading.
- Never silently substitute preseason data after a live request fails.
- An unavailable or incomplete lens must not calculate missing inputs as zero.
- Static data may remain only as an explicitly labeled manual/development baseline when no live game ID is requested.

Identify any current behavior that conflicts with these requirements.

11. Exact response format

Return your answer using these headings:

A. Source files inspected
B. Exact TypeScript interfaces
C. Six-lens rule matrix
D. Metric/tag eligibility and exclusions
E. Missing/null/warning behavior
F. Percentile contract
G. Authenticated API helper and base URL
H. Query/cache behavior
I. Static source and adapter insertion point
J. Route-parameter behavior
K. UI state behavior
L. Backend-to-frontend field needs
M. Resolved answers
N. Unresolved questions or blockers

For the six-lens rule matrix, use one row per lens with these columns:

- Lens key
- Display name
- Required metrics
- Included tags
- Formula
- Weights
- Modifiers
- Exclusions
- Minimum eligibility
- Missing/null behavior

For “Backend-to-frontend field needs,” enumerate every backend field the adapter must receive. Distinguish:

- Required
- Optional
- Nullable
- Ignored by current frontend
- Needed only for UI freshness/warnings
- Needed for scoring eligibility

Do not implement changes. Do not redesign Matchup Lens. Do not invent missing formulas, types, fields, or eligibility rules. Clearly mark anything the current source does not establish as unresolved.
```

## Lovable's returned answers — normalized summary

Lovable inspected the current source without changing, creating, or publishing anything. It reported explicit types rather than inferred shapes:

```ts
export type SignalStrength = "strong" | "supporting";

export interface MetricDefinition {
  metric: string;
  label: string;
  signalStrength: SignalStrength;
  lensTags: string[];
}

export interface TeamMetricRow {
  teamId: number;
  teamAbv: string;
  gamesInWindow: number;
  latestSourceDate: string;
  dataLagDays: number;
  percentiles: Record<string, number>;
}

export interface LensSnapshot {
  asOfDate: string;
  windowLabel: string;
  gamesLabel: string;
  contextLabel: string;
  metrics: MetricDefinition[];
  teams: TeamMetricRow[];
}
```

All fields are required and non-null in the current type. Missing percentiles are absent record keys. A runtime null would be skipped by the numeric guard but would violate the declared type.

The source path is `src/lib/matchup-lens-source.ts` → `staticLensSnapshotSource` / module-level `activeSource` → `getLensSnapshotSource()` → `MatchupLens.tsx` TanStack query → `LensSnapshot` → unchanged scoring and rendering. The static data is raw evidence in `PRESEASON_2026_SNAPSHOT`, not precomputed scores.

The authenticated request pattern is the Firebase-bearer client in `src/lib/nfl-api.ts`, using `getAuthToken()` and the current production `API_BASE`. The nearest GET encodes the game ID, adds `Authorization: Bearer …`, and passes the response through `handleApiResponse`. HTTP 401 signs out; 403 is forbidden; server/network/invalid-response failures map to existing safe error types. No current request primitive supports `AbortSignal` or a timeout.

TanStack Query is used globally. The proposed key `['matchup-lens-context', gameId]` matches the project convention. The live query must be independent of `['nfl-game', gameId]`, must not use `keepPreviousData`, and should use `meta: { persist: false }` so frozen or prior-game evidence is not rehydrated as current evidence. The current lens query is game-independent, infinitely fresh, and persisted, which conflicts with the live-evidence requirement.

The page currently ignores `game`; `a` and `b` select the team rows and therefore drive calculations. `view` and its navigation behavior must remain unchanged. Canonical team identity must come from the future response header, with `a`/`b` no longer authoritative for a live game.

Current UI support exists for loading skeleton, missing team rows, authentication sign-out, access denied, safe general error, retry, and exhausted retries. There is no current invalid-game, `available: false`, partial-lens, warning, or timeout concept. Those are contract/integration gaps, not existing reusable behaviors.

## Normalized backend-to-frontend field mapping

### Required for current scoring or identity

| Backend field | Frontend target | Rule |
|---|---|---|
| `teams.<side>.team_id` | `TeamMetricRow.teamId` | Convert to finite number; canonical response value is authoritative. |
| `teams.<side>.team_abv` | `TeamMetricRow.teamAbv` | Canonical response value; live lookup must not be driven by URL `a`/`b`. |
| `teams.<side>.games_in_window` | `TeamMetricRow.gamesInWindow` | Required current type and used in brief/caveat text. |
| metric-object key or `.metric` | `MetricDefinition.metric` and `percentiles` key | Preserve the exact runtime metric name. De-duplicate consistently before adaptation. |
| metric `.label` | `MetricDefinition.label` | Required for detail/trace display. |
| metric `.signal_strength` | `MetricDefinition.signalStrength` | Must normalize only to `strong` or `supporting`; invalid values require an explicit C decision. |
| metric `.lens_tags` | `MetricDefinition.lensTags` | Required for inclusion, exclusion, and modifiers; always an array. |
| metric `.league_percentile` | `TeamMetricRow.percentiles[metric]` | 0–100; omit key when null; reject/handle non-finite or out-of-range values per C. |
| `basis.as_of_date` | `LensSnapshot.asOfDate` | ISO date string. |

### Required by the current type or display, but contract construction must be frozen

| Backend source | Frontend target | Finding |
|---|---|---|
| `basis.window_type` plus display policy | `windowLabel` | No direct backend display field exists in the plan; C must choose backend value versus deterministic adapter formatting. |
| evidence counts plus display policy | `gamesLabel` | No direct backend counterpart; C must define deterministic construction or add an explicit field. |
| game/basis metadata plus display policy | `contextLabel` | No direct backend counterpart; C must define deterministic construction or add an explicit field. |
| team/source metadata | `latestSourceDate` | Current frontend type is team-level; source payload is metric-level. C must define aggregation and mismatch behavior. |
| team/source metadata | `dataLagDays` | Current frontend type is team-level and currently unrendered; C must define aggregation/calculation. |

### Needed for honest availability, freshness, and warnings even though current scoring ignores them

- Top-level `schema_version`, `available`, and safe `reason`.
- Canonical game header: game ID, date, status, season/week/type, away/home IDs and abbreviations.
- Basis metadata: window type, ranking date, source dates, pregame-safe flag, comparison-not-forecast marker, and source names as frozen by C.
- Team `latest_included_game_id` for auditability.
- Coverage counts, missing metrics, readiness/availability result, and safe warnings as frozen by C.
- Metric `data_quality_status`, ranking eligibility/exclusion metadata, and source freshness should be preserved by the backend even though the current lens engine does not read them. C must decide which fields the adapter/UI surfaces and which remain transport metadata.

### Currently ignored by lens scoring

`ranking_usage`, `edge_language_allowed`, `include_in_core_area_advantage`, `confidence_eligible`, `data_quality_status`, `comparison_direction`, `higher_is_better`, `league_rank`, `teams_ranked`, `ranking_kind`, `rank_direction`, `rank_interpretation`, per-metric source/lag metadata, and latest included game ID do not affect the six weighted means today. They must not be described as scoring inputs.

### Hardcoded metric-name consumers outside the six weighted means

Collision and Turnover Watch features directly reference:

- `sacks_taken`
- `sack_yards_lost`
- `sacks`
- `interceptions_thrown`
- `fumbles_lost`
- `turnovers`
- `defensive_interceptions`
- `fumbles_recovered`
- `points_per_play`
- `td_rate`
- `red_zone_efficiency`
- `points_allowed_per_play`

The endpoint must return these dynamically when the rankings helper supplies them. Missing collision inputs use their existing collision coverage rules; C must distinguish this from six-lens readiness.

## Confirmed rules by lens

All six lens scores use the same formula:

```text
score = sum(percentile × weight) / sum(weight)
```

Only metrics with a numeric percentile and at least one matching include tag contribute. Any matching exclusion tag wins. Duplicate metric definitions within one lens are suppressed by metric name. There is no frontend polarity correction, rescaling, clamping, rounding, threshold, minimum sample count, or data-quality eligibility rule.

Base weight is 2 for `strong` and 1 for `supporting`; multiply by 0.5 for `volume-sensitive` and by 0.75 for `volatility`. Both modifiers compound when both tags are present. `rare-event` is globally excluded.

| Lens key | Display name | ANY-match include tags | Additional exclude tags | Minimum |
|---|---|---|---|---|
| `explosiveness` | Explosiveness | `explosiveness`, `offensive-efficiency`, `passing-efficiency`, `rushing-efficiency` | none beyond `rare-event` | 1 numeric eligible metric |
| `drive-control` | Drive Control | `drive-sustainability`, `third-down`, `fourth-down`, `drive-efficiency`, `drive-conversion` | none beyond `rare-event` | 1 numeric eligible metric |
| `scoring-finish` | Scoring Finish | `scoring-efficiency`, `scoring`, `touchdowns`, `red-zone`, `touchdown-efficiency`, `efficiency` | none beyond `rare-event` | 1 numeric eligible metric |
| `defensive-resistance` | Defensive Resistance | `defense`, `scoring-suppression`, `scoring-efficiency-allowed` | `defensive-scoring`, `swing-play` | 1 numeric eligible metric |
| `disruption-protection` | Disruption & Protection | `disruption`, `negative-plays`, `protection`, `pressure-allowed` | `blocked-kicks`, `special-teams` | 1 numeric eligible metric |
| `turnover-balance` | Turnover Balance | `turnovers`, `giveaways`, `takeaways`, `takeaway-margin` | none beyond `rare-event` | 1 numeric eligible metric |

Every frontend-required inclusion, exclusion, and modifier tag is present in Chunk A's live 90-tag vocabulary. No required frontend tag is missing.

## Null, missing, exclusion, and percentile rules

- Missing metric key: skipped; never zero-filled.
- Runtime null percentile: skipped by `typeof value !== "number"`, although null is not type-valid.
- Missing inputs are dropped from both numerator and denominator, so surviving weights are renormalized.
- Zero eligible inputs: score is null.
- One or more eligible numeric inputs: score is produced with no completeness flag or warning.
- Missing entire team row: the page renders `DashboardEmpty` for the whole page.
- `rare-event`: excluded globally.
- A per-lens exclusion tag: excludes the metric before inclusion is considered.
- `small-sample` and data-quality tags: no current scoring or UI effect.
- Unrecognized tags: ignored.
- Unrecognized metrics: ignored unless they carry a recognized include tag.
- Percentile scale: 0–100; higher is assumed to mean better because polarity is corrected upstream.
- Frontend performs no range or finiteness validation. Chunk C/backend must prevent NaN, Infinity, and unsafe out-of-range values from reaching scoring.

DET's missing `fourth_down_pct` affects only Drive Control. The current frontend would compute DET from the remaining qualifying metrics and BUF from its larger set, without warning. That silent denominator asymmetry is existing behavior and must be explicitly accepted or made visible by the frozen contract; Chunk B does not redesign it.

## API helper, adapter, cache, and UI-state findings

### API helper

- Reuse `src/lib/nfl-api.ts` patterns and `getAuthToken()` from `src/lib/firebase.ts`.
- Encode `gameId` in `/game/${encodeURIComponent(gameId)}/lens-context`.
- Preserve the existing Firebase bearer header and safe error mapping.
- Do not introduce a second base-URL or authentication policy.
- Current primitives have no timeout or abort support.

### Adapter

The smallest safe seam is a pure `matchup_lens_v1` → `LensSnapshot` adapter plus a per-game `LensSnapshotSource`. The existing interface suggests:

```ts
export function adaptMatchupLensV1(payload: MatchupLensV1Response): LensSnapshot;
export function liveLensSnapshotSource(gameId: string): LensSnapshotSource;
```

This is a finding, not implementation authorization. Because `LensSnapshotSource.load()` accepts no arguments and the current source is a singleton, live per-game wiring necessarily requires a bounded change in `MatchupLens.tsx`. The scoring/rendering modules below `snapshot` need not change.

### Query/cache

- Use a separate live query key `['matchup-lens-context', gameId]`.
- Enable only for a valid, non-empty game ID under the frozen validation rule.
- Do not use `keepPreviousData` or placeholder data from another game.
- Set `meta: { persist: false }` so live evidence is not restored from localStorage.
- Preserve `refetchOnWindowFocus: false` unless C/G explicitly finds a reason to change it.
- Retry must exclude authentication/authorization/not-found responses; exact settings belong to C/G.
- Without AbortSignal plumbing, old in-flight responses remain isolated under their old game-ID key but are not actively cancelled.

### Static-source hazards

The following can silently show preseason evidence unless the later integration explicitly separates live-game and manual/static modes:

1. Module-level `activeSource` defaults to `staticLensSnapshotSource`.
2. The current query key is game-independent.
3. The current static query is infinitely fresh and persisted to localStorage.
4. Current `a`/`b` defaults normalize invalid input to preseason LAR/CLE.
5. Prior-game display would be possible if `keepPreviousData` were copied into the new query.

### UI states

Reusable today: initial skeleton, whole-page empty state, Firebase 401 sign-out/redirect, 403 safe message, general safe error, and manual retry. Not currently represented: invalid/unknown game, top-level `available: false`, partial lens, structured warning, timeout, or metric-quality state. These gaps require a frozen behavior contract before G, but do not authorize a redesign.

## Resolved answers

1. `LensSnapshot`, `TeamMetricRow`, and `MetricDefinition` are explicit and fully known.
2. The six-lens formulas, tags, ordering, exclusions, weights, and modifiers are exact.
3. Eligibility is tag-driven; only `signalStrength`, `lensTags`, metric identity/label, and percentiles affect lens scoring.
4. Percentiles are 0–100 and must arrive polarity-corrected.
5. Missing percentiles are omitted/skipped, never converted to zero.
6. A single surviving eligible metric produces a lens score; zero produces null.
7. All required frontend tags exist in the verified live vocabulary.
8. DET's missing `fourth_down_pct` affects only Drive Control and does not make it unscorable under current rules.
9. The established Firebase-authenticated API client pattern and base URL are known.
10. `['matchup-lens-context', gameId]` matches cache-key convention.
11. The current static source and smallest safe adapter seam are identified.
12. `game` is currently ignored; `a`/`b` currently drive evidence selection; `view` behavior is independent and must remain unchanged.

## Unresolved blockers and risks for Chunk C

1. **League standings:** Current standing and trace helpers rank across `snapshot.teams`. A two-team response would yield ranks out of 2 rather than league ranks out of 32. C must decide whether the endpoint supplies league-wide team rows, supplies explicit standing metadata and requires a bounded frontend adaptation, suppresses those features for two-team evidence, or another explicitly approved option. Do not let D invent this.
2. **Display fields:** Freeze how `windowLabel`, `gamesLabel`, and `contextLabel` are supplied or deterministically synthesized.
3. **Team-level freshness:** Freeze aggregation/mismatch rules for metric-level source dates and lag into current team-level `latestSourceDate`/`dataLagDays`, or change the frozen contract intentionally.
4. **Availability and partial evidence:** Define top-level unavailable, missing-team, partial-lens, warning, and safe-reason behavior. The current frontend has no such types.
5. **Timeout/cancellation:** No existing request primitive supports AbortSignal or timeout. C must state whether timeout is required in this slice and allocate ownership rather than assuming it exists.
6. **Numeric validation:** Define backend handling for null, non-finite, and out-of-range percentiles. Frontend validation is insufficient.
7. **Per-game source wiring:** The current no-argument singleton source cannot support game identity without a bounded `MatchupLens.tsx` change. This is expected later frontend scope through Christian, not backend implementation scope.
8. **Silent asymmetric denominators:** Decide whether existing behavior is accepted for DET's missing `fourth_down_pct` or must be surfaced through coverage/readiness metadata without changing scoring.
9. **Runtime duplicate guarantee:** Carry forward Chunk A's finding that dictionary reshaping can hide raw duplicates; C must settle the runtime protection policy.
10. **Source-aligned counts:** Carry forward Chunk A's requirement that `games_in_window` match each ranking row's source date and not a newer pregame window row.

Chunk B's acceptance gate passes because these questions can now be decided from explicit frontend facts. They must not be filled by assumption.

## Files changed

- Added `documentation/New API/handoffs/Chunk_B_Frontend_Contract_Handoff.md`

No other file was changed.

## Validation performed

- Confirmed the branch head before writing: `9d6b6c45d104ae79dd0970e67c3861d45ef4a058`.
- Confirmed the Chunk B handoff path did not already exist.
- Checked every user-requested frontend-contract topic against Lovable's returned inspection.
- Reconciled all six frontend-required tag sets with the accepted 90-tag live vocabulary.
- Reconciled DET's missing `fourth_down_pct` with current Drive Control eligibility and null behavior.
- Separated six-lens tag-driven scoring from metric-name-driven Collisions and Turnover Watch.
- Identified the two-team league-ranking semantic mismatch.
- Confirmed no application code or protected system was changed.
- Compared the resulting commit to the starting commit and verified only this handoff file changed.

## Next recommended chunk

**Chunk C — Freeze the smallest implementable contract.** Chunk C must use the completed A and B handoffs, resolve the roadmap concern table and the blockers above, produce a separate contract decision record, and stop for Christian's review before any implementation.

## Ready-to-copy prompt for Chunk C

```text
[@GitHub](plugin://github@openai-curated-remote) Continue GameLens on branch `feature/matchup-lens-api` in `csells10/meow`.

This is a controlled execution of Chunk C only: freeze the smallest implementable Matchup Lens endpoint contract.

Use the model and effort assigned to Chunk C in the Product Roadmap: `gpt-5.6-sol` / High. Keep the task narrow. Do not create additional agents. Do not begin Chunk D or implementation.

First read these files completely, in order:

1. `documentation/New API/GameLens_Matchup_Lens_API_Endpoint_Plan.md`
2. `documentation/New API/GameLens_Matchup_Lens_API_Product_Roadmap.md`
3. `documentation/New API/handoffs/Chunk_A_Evidence_Inventory_Handoff.md`
4. `documentation/New API/handoffs/Chunk_B_Frontend_Contract_Handoff.md`
5. Any later existing files under `documentation/New API/handoffs/`, ordered by chunk letter and date

Treat Chunk A and Chunk B as the accepted evidence basis. Do not rerun their work or infer facts they left unresolved.

Chunk A established:

- 59 plan-sample / 73 registry / 70 code-eligible / 63 live ranking metrics;
- BUF has 63 metrics, DET has 62, and DET lacks only `fourth_down_pct`;
- ranking date `2026-09-14`, source-data date `2026-09-13`;
- one source-aligned game per team;
- latest games `20260913_BUF@HOU` and `20260913_NO@DET`;
- zero selected duplicate grain groups;
- all frontend-required tags exist in the 90-tag live vocabulary.

Chunk B established:

- the exact `LensSnapshot`, `TeamMetricRow`, and `MetricDefinition` types;
- six ANY-tag weighted-mean lenses, global `rare-event` exclusion, per-lens exclusions, weights 2/1, modifiers 0.5/0.75;
- missing/null percentiles are skipped and remaining weights renormalize; one metric is enough for a score;
- percentiles are polarity-corrected 0–100;
- the Firebase-authenticated API helper, cache convention, static source, adapter seam, URL behavior, and current UI states;
- the endpoint must preserve metric-name consumers for Collisions and Turnover Watch;
- a two-team `LensSnapshot` would incorrectly change league standings/ranks to “out of 2.”

Use Endpoint Plan §§6–8 and 13–14 plus the roadmap's Chunk C brief and concern table. Produce a separate contract decision record under `documentation/New API`; do not edit the Endpoint Plan or Product Roadmap.

The contract record must explicitly freeze:

1. Success, expected no-evidence, one-missing-team, partial-lens, unsafe-date, invalid-ID, unknown-game, authentication, authorization, timeout if in scope, and unexpected-server-error behavior.
2. HTTP status plus body shape for every state, including safe reason codes/messages.
3. Exact `matchup_lens_v1` fields, nesting, types, required/optional/nullability rules, stable ordering, and valid JSON-number requirements.
4. Runtime metric-catalog semantics and every coverage denominator; no fixed 59/63/70/73 allowlist.
5. Exact source-date-aligned `games_in_window` and `latest_included_game_id` lookup semantics, including mismatch behavior.
6. Runtime duplicate-protection policy before dictionary reshaping.
7. Both-team availability versus individual lens incompleteness.
8. Six-lens readiness ownership and behavior, preserving current scoring and explicitly addressing DET's missing `fourth_down_pct` and asymmetric denominators.
9. Backend fields required by the adapter, including `windowLabel`, `gamesLabel`, `contextLabel`, team-level freshness aggregation, `signal_strength`, `lens_tags`, and nullable percentiles.
10. Numeric validation for null, NaN, Infinity, and values outside 0–100.
11. The two-team league-standing blocker. Choose and document a solution that preserves established Matchup Lens behavior without silently producing ranks out of two; do not redesign the product.
12. Treatment of collision/Turnover Watch hardcoded metric inputs separately from six-lens tag readiness.
13. Loading, unavailable, partial, authentication, access-denied, timeout, retry, and no-static-fallback expectations for the later frontend handoff.
14. Clearly labeled synthetic fixtures for all critical states. Do not present synthetic values as live observations.
15. Exact, narrowly bounded file ownership for Chunk D and Chunk E, including whether runtime duplicate detection requires an additional read.

Do not change application code, existing tests, services, routes, queries, pipelines, schemas, tables, scheduled jobs, learning systems, or orchestrator state. Do not deploy, merge, contact Lovable, or perform BigQuery writes. Do not begin implementation.

If any issue cannot be frozen without Christian's product decision, prepare one compact decision request and pause before creating a supposedly final contract. Do not choose silently.

Create the separate Chunk C contract decision record and a Chunk C handoff only after every required decision is resolved. Commit and push only the authorized Chunk C documentation to `feature/matchup-lens-api`. Report the commit SHA and provide local fast-forward commands. Stop after Chunk C and wait for explicit implementation authorization.
```
