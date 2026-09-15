# GameLens Matchup Lens API Endpoint Plan

**Status:** planning only  
**Planning branch:** `feature/matchup-lens-api`  
**Base branch:** `main`  
**Target merge:** short-lived branch back into `main`, only after explicit approval and verification  
**Proposed endpoint:** `GET /game/<game_id>/lens-context`

## 1. Decision

Create one narrow, read-only endpoint that accepts a GameLens `game_id` and returns the complete pregame evidence required by the existing Lovable Matchup Lens.

The endpoint will reuse the production-safe selection logic already implemented by `get_team_rankings_for_game(game_id)`. It will not create or modify BigQuery tables, views, materialized views, scheduled jobs, learning captures, orchestrator state, model outputs, confidence calculations, or the existing `/game` response.

This is an exposure and response-shaping effort, not a new analytics pipeline.

## 2. Why a dedicated endpoint

The current division of responsibility will remain clear:

- `/games` identifies the scheduled game and supplies its `gameID`.
- `/game/<game_id>` remains the existing production game analysis.
- `/game/<game_id>/lens-context` supplies the complete Matchup Lens evidence.

A dedicated endpoint avoids enlarging or changing the existing `/game` contract. It also gives Lovable one stable data source that can replace the frozen preseason snapshot without redesigning the Matchup Lens interface or calculations.

## 3. Hard scope boundaries

### This planning commit may change

- This Markdown plan only.

### A later implementation may add only what is necessary

- One additive GET route for the new endpoint.
- One isolated response builder/serializer for Matchup Lens evidence.
- One small read-only lookup for `games_in_window` if it cannot be obtained from information already loaded for `/game`.
- Endpoint-specific verification fixtures or a new isolated test file, but only after Christian explicitly approves the implementation phase.
- Frontend source/adaptor changes in Lovable, performed through Christian.

### This effort must not change

- BigQuery data or schemas.
- The facts, windowed-metrics, or rankings pipeline.
- Cloud Scheduler.
- Learning Lite, LL-2, LL-3, captures, training, validation, or orchestration.
- Existing `/games` behavior.
- Existing `/game/<game_id>` fields or behavior.
- Lean, confidence, Model Trust, matchup analysis, or pick logic.
- Existing tests merely to make a new implementation pass.
- Existing Matchup Lens scoring, weights, exclusions, language generation, constellation design, cards, tabs, or layouts.
- The frozen preseason snapshot except to stop using it as a silent live-data fallback.

## 4. Important constraint clarification

No route, service, query, or test file is changed by this planning branch.

Creating an actual HTTP endpoint later necessarily requires registering an additive route somewhere. That implementation must not rewrite or alter the behavior of existing routes. The preferred minimal implementation is one new handler added to the existing game blueprint plus an isolated lens-context builder. If Christian wants zero edits to the existing route module, a separate blueprint is possible, but that would require another file and blueprint registration and is therefore more machinery.

No implementation work begins from this document automatically.

## 5. Current verified starting point

The production code already contains:

- `get_game_header(game_id)` in `queries/game_queries.py`.
- `select_window_type(header)` for phase-aware selection.
- `get_team_rankings_for_game(game_id)` in `queries/game_queries.py`.
- Pregame protection using the latest ranking `as_of_date` strictly before the scheduled `game_date`.
- Ranking rows containing metric values, percentiles, ranks, lens tags, signal strength, quality controls, and freshness fields.

For `20260917_DET@BUF`, the expected basis is:

- season: `2026`
- phase: Regular Season
- game week: Week 2
- window type: `regular_season_to_date`
- ranking `as_of_date`: `2026-09-14`
- games in window: `1` for DET and `1` for BUF
- no source data from the target game or any later date

## 6. Request contract

```http
GET /game/20260917_DET@BUF/lens-context
```

The route must treat the URL `game_id` as authoritative. Away/home abbreviations supplied by the frontend are display hints only and must never determine which teams are queried.

### Suggested response codes

| Status | Meaning |
|---|---|
| `200` | A complete or explicitly incomplete lens-context response was built. |
| `400` | The game ID is empty or structurally invalid. |
| `404` | No schedule header exists for that game ID. |
| `409` | The game exists, but the available evidence cannot safely form the requested pregame context. |
| `500` | An unexpected server failure occurred. No frozen or fabricated data is returned. |

An alternative is to return `200` with `available: false` for expected data-availability conditions. The implementation should choose one convention and document it before frontend wiring. Authentication and CORS should follow the existing API policy rather than introduce a second policy.

## 7. Backend execution steps

### Step 1 — Validate the game ID

The handler receives `<game_id>` and performs inexpensive structural validation:

- value is present and is a string;
- value length is bounded;
- only the established GameLens game-ID character set is accepted;
- the value is passed to parameterized BigQuery code, never interpolated into SQL;
- no season, team, or date is trusted from the browser.

Structural validation is not proof the game exists. Existence is established by the header lookup.

### Step 2 — Retrieve the canonical game header

Call:

```python
header = get_game_header(game_id)
```

The header becomes the authority for:

- canonical game ID;
- game date and time;
- game status;
- season;
- game week and season type;
- away team ID and abbreviation;
- home team ID and abbreviation;
- optional team logos.

If no header exists, return the documented not-found response. Do not attempt to infer teams from the text of the game ID.

### Step 3 — Select the correct evidence window

Reuse:

```python
window_type = select_window_type(header)
```

For Week 2 of the regular season, this must resolve to `regular_season_to_date`.

Do not add independent frontend window-selection rules. The backend remains authoritative.

### Step 4 — Call the existing rankings function

Call:

```python
away_rankings, home_rankings, ranking_meta = get_team_rankings_for_game(game_id)
```

Do not pass a featured-metric allowlist. The Lens endpoint must serialize every eligible metric returned by the existing function.

The existing function already provides the key pregame safety behavior:

```text
latest as_of_date where as_of_date < game_date
and window_type = selected phase-aware window
and team_id is one of the two scheduled teams
```

### Step 5 — Retrieve `games_in_window`

`games_in_window` is stored in `Analytics.team_metrics_windowed_<season>` and is not currently included in the ranking payload shown by `get_team_rankings_for_game()`.

The implementation should use the smallest safe option:

1. Prefer carrying `games_in_window` through an existing windowed-metric lookup already executed for the requested game.
2. If it is not available there, add one small read-only parameterized lookup for the two canonical team IDs, selected `window_type`, and latest data date strictly before the game date.
3. Require a single unambiguous value per team for the selected snapshot.
4. Also capture `latest_included_game_id` when available for auditability.

Do not derive the count from the number of ranking rows. Do not hardcode Week 2 as one game.

### Step 6 — Validate evidence before serialization

At minimum, validate:

- both scheduled teams are represented;
- ranking metadata reports `available: true`;
- `as_of_date < game_date`;
- all source dates are on or before the selected `as_of_date` and before `game_date`;
- `window_type` matches `select_window_type(header)`;
- `games_in_window` is present and non-negative for both teams;
- each `team_id + metric` combination is unique;
- percentiles are null or numeric values from 0 through 100;
- `lens_tags` is always serialized as an array;
- metrics marked excluded or affected by data-quality rules retain those flags;
- missing metrics remain missing/null and are never converted to zero.

### Step 7 — Reshape the result

Build a versioned, lens-specific response. The response should contain:

- canonical game context;
- evidence window and freshness metadata;
- away and home team context;
- `games_in_window` for each team;
- every ranking metric returned for each team;
- the metadata necessary for existing Lovable lens scoring;
- coverage counts and explicit warnings;
- a method statement explaining that the response is a comparison, not a forecast.

The endpoint supplies evidence. It does not modify the production lean, confidence, pick, or Model Trust outputs.

### Step 8 — Return JSON

Return a deterministic response:

- stable field names;
- stable `schema_version`;
- metrics sorted consistently or represented as objects keyed by metric name;
- dates serialized as ISO `YYYY-MM-DD` strings;
- no Python-specific values;
- no NaN or Infinity;
- no credentials, SQL, internal exception details, or stack traces.

## 8. Proposed JSON response

This is the proposed output contract. Numeric metric values are deliberately not fabricated in this planning document; the live endpoint must populate them from BigQuery. The `metric_catalog` is populated from the 59 metric names verified in the checked-in production registry. The `teams.*.metrics` objects are dynamically populated from every row returned by `get_team_rankings_for_game()` rather than a hardcoded allowlist.

```json
{
  "schema_version": "matchup_lens_v1",
  "available": true,
  "reason": null,
  "game": {
    "game_id": "20260917_DET@BUF",
    "game_date": "2026-09-17",
    "game_time": "8:15p",
    "game_status": "Scheduled",
    "season": "2026",
    "game_week": "Week 2",
    "season_type": "Regular Season",
    "away_team_id": "populated_from_game_header",
    "away_team_abv": "DET",
    "home_team_id": "populated_from_game_header",
    "home_team_abv": "BUF"
  },
  "basis": {
    "window_type": "regular_season_to_date",
    "as_of_date": "2026-09-14",
    "source_data_dates": ["populated_from_ranking_meta"],
    "max_data_lag_days": 5,
    "pregame_safe": true,
    "comparison_not_forecast": true,
    "rankings_source": "Analytics.team_metric_rankings_2026",
    "window_source": "Analytics.team_metrics_windowed_2026"
  },
  "metric_catalog": [
    "passing_yards",
    "pass_completions",
    "pass_attempts",
    "passing_tds",
    "yards_per_pass",
    "rushing_yards",
    "rushing_attempts",
    "rushing_tds",
    "yards_per_rush",
    "total_yards",
    "total_plays",
    "yards_per_play",
    "first_downs",
    "1st_down_rate",
    "total_drives",
    "time_of_possession",
    "run_play_pct",
    "pass_play_pct",
    "pass_run_ratio",
    "passing_tds_rushing_tds_sum",
    "pass_td_share",
    "rush_td_share",
    "total_offensive_snaps",
    "offensive_snap_load",
    "actual_points",
    "points_per_play",
    "td_rate",
    "red_zone_tds",
    "red_zone_attempts",
    "red_zone_efficiency",
    "third_down_conversions",
    "third_down_attempts",
    "third_down_pct",
    "fourth_down_conversions",
    "fourth_down_attempts",
    "fourth_down_pct",
    "yards_allowed",
    "opponent_total_plays",
    "defensive_success_rate",
    "points_allowed_per_yard",
    "points_allowed",
    "points_allowed_per_play",
    "total_defensive_snaps",
    "defensive_snap_load",
    "sacks",
    "pressure_rate",
    "sacks_taken",
    "sack_yards_lost",
    "sacks_plus_sacks_taken",
    "sack_to_turnover_ratio",
    "defensive_interceptions",
    "fumbles_recovered",
    "interceptions_thrown",
    "fumbles_lost",
    "turnover_margin",
    "turnover_margin_per_game",
    "total_special_teams_snaps",
    "special_teams_snap_pct",
    "total_snaps"
  ],
  "teams": {
    "away": {
      "team_id": "populated_from_game_header",
      "team_abv": "DET",
      "games_in_window": 1,
      "latest_included_game_id": "populated_from_windowed_metrics",
      "latest_source_date": "2026-09-14",
      "metrics": {
        "each_metric_returned_by_get_team_rankings_for_game": {
          "metric": "runtime_metric_name",
          "value": null,
          "label": "populated_from_ranking_row",
          "definition": "populated_from_ranking_row",
          "category": "populated_from_ranking_row",
          "core_area": "populated_from_ranking_row",
          "comparison_direction": "populated_from_ranking_row",
          "higher_is_better": null,
          "format": "populated_from_ranking_row",
          "decimals": null,
          "ranking_usage": "populated_from_ranking_row",
          "signal_strength": "populated_from_ranking_row",
          "edge_language_allowed": null,
          "include_in_core_area_advantage": null,
          "confidence_eligible": null,
          "data_quality_status": "populated_from_ranking_row",
          "lens_tags": [],
          "league_rank": null,
          "league_percentile": null,
          "tier": "populated_from_ranking_row",
          "tier_label": "populated_from_ranking_row",
          "teams_ranked": null,
          "ranking_kind": "populated_from_ranking_row",
          "rank_direction": "populated_from_ranking_row",
          "rank_interpretation": "populated_from_ranking_row",
          "source_data_date": "YYYY-MM-DD",
          "data_lag_days": null
        }
      }
    },
    "home": {
      "team_id": "populated_from_game_header",
      "team_abv": "BUF",
      "games_in_window": 1,
      "latest_included_game_id": "populated_from_windowed_metrics",
      "latest_source_date": "2026-09-14",
      "metrics": {
        "each_metric_returned_by_get_team_rankings_for_game": {
          "metric": "runtime_metric_name",
          "value": null,
          "label": "populated_from_ranking_row",
          "definition": "populated_from_ranking_row",
          "category": "populated_from_ranking_row",
          "core_area": "populated_from_ranking_row",
          "comparison_direction": "populated_from_ranking_row",
          "higher_is_better": null,
          "format": "populated_from_ranking_row",
          "decimals": null,
          "ranking_usage": "populated_from_ranking_row",
          "signal_strength": "populated_from_ranking_row",
          "edge_language_allowed": null,
          "include_in_core_area_advantage": null,
          "confidence_eligible": null,
          "data_quality_status": "populated_from_ranking_row",
          "lens_tags": [],
          "league_rank": null,
          "league_percentile": null,
          "tier": "populated_from_ranking_row",
          "tier_label": "populated_from_ranking_row",
          "teams_ranked": null,
          "ranking_kind": "populated_from_ranking_row",
          "rank_direction": "populated_from_ranking_row",
          "rank_interpretation": "populated_from_ranking_row",
          "source_data_date": "YYYY-MM-DD",
          "data_lag_days": null
        }
      }
    }
  },
  "coverage": {
    "away_metric_count": null,
    "home_metric_count": null,
    "shared_metric_count": null,
    "missing_away_metrics": [],
    "missing_home_metrics": [],
    "lens_readiness": {
      "explosiveness": "complete_or_unavailable",
      "drive_control": "complete_or_unavailable",
      "scoring_finish": "complete_or_unavailable",
      "defensive_resistance": "complete_or_unavailable",
      "disruption_and_protection": "complete_or_unavailable",
      "turnover_balance": "complete_or_unavailable"
    },
    "warnings": []
  },
  "method": {
    "selection": "Latest phase-appropriate ranking snapshot strictly before the scheduled game date.",
    "frontend_role": "Existing Matchup Lens formulas transform this evidence into lens scores and comparison language.",
    "forecast": false
  }
}
```

The placeholder strings and nulls above describe values that must be supplied at runtime. They are not proposed production values and must not be returned literally by the implemented endpoint.

## 9. Metric inventory gate: 59 in code versus 63 in live rankings

The checked-in `METRIC_REGISTRY` used for this plan contains the 59 names listed in `metric_catalog`.

The verified September 15 BigQuery result reported 63 distinct regular-season ranking metrics for `as_of_date = 2026-09-14`.

This four-metric difference must be reconciled with a read-only inventory before implementation. No metric should be guessed, silently omitted, or hardcoded based solely on this document.

Required read-only verification:

```sql
SELECT
  metric,
  COUNT(DISTINCT team_id) AS teams_ranked,
  MIN(as_of_date) AS first_as_of_date,
  MAX(as_of_date) AS latest_as_of_date
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026`
WHERE window_type = 'regular_season_to_date'
  AND as_of_date = DATE '2026-09-14'
GROUP BY metric
ORDER BY metric;
```

The endpoint itself should remain dynamic: serialize every metric returned by `get_team_rankings_for_game()` and report coverage. The documented catalog is for review and contract visibility, not an allowlist.

## 10. Lovable frontend integration plan (Mary)

The existing Slate link remains:

```text
/matchup-lens?a=DET&b=BUF&view=overview&game=20260917_DET@BUF
```

Matchup Lens should:

1. Read the existing `game` URL parameter.
2. Treat the endpoint's game header and team IDs as authoritative.
3. Request `/game/<encoded-game-id>/lens-context` through the existing authenticated API client.
4. Use a pure adapter to transform `matchup_lens_v1` into the current `LensSnapshot` interface.
5. Feed that adapted snapshot to the existing scoring and rendering code.
6. Cache by game ID, for example `['matchup-lens-context', gameId]`.
7. Never show the preseason snapshot while a live request is loading.
8. Never silently substitute preseason data after a live request fails.

The following frontend behavior remains unchanged:

- six-lens definitions and formulas;
- metric weights and exclusions;
- constellation rendering;
- Biggest Edge;
- observations and comparison language;
- cards, tabs, layout, and URL `view` handling.

### Honest frontend states

- Loading: page shell or skeleton, without preseason values.
- Invalid/not-found game: controlled game-unavailable state.
- Authentication failure: existing sign-in/access behavior.
- Timeout/server failure: inline retry.
- `available: false`: show current matchup evidence as unavailable and surface the safe reason/freshness context.
- Incomplete lens: show that lens as incomplete/unavailable rather than computing missing inputs as zero.

The frozen preseason snapshot may remain only for an explicitly labeled development/manual baseline where no live game ID is requested.

## 11. Lovable communication ownership

All Lovable communication will be performed by Christian.

Codex, GitHub agents, and implementation agents must not send prompts, messages, questions, or responses to Lovable. They may draft a bounded prompt or question list for Christian, but Christian decides whether and when to send it and returns any response manually.

Before frontend implementation, Christian should ask Lovable to confirm:

1. The exact current TypeScript `LensSnapshot` and team-row interfaces.
2. Whether percentiles are expected on a 0–100 scale. The backend currently uses 0–100 and must not silently rescale.
3. The six lens definitions, required tag inclusion rules, exclusions, and weighting modifiers currently active in `matchup-lens.ts`.
4. How the current code handles a missing metric, null percentile, excluded metric, or data-quality warning.
5. The smallest adapter location that can replace `staticLensSnapshotSource` without altering rendering code.
6. The established authenticated API helper and base URL used elsewhere in the Lovable application.
7. Whether the page already shares a cached game-details query that should be reused or whether this dedicated endpoint should use its own game-ID cache key.

No Lovable implementation should begin until the endpoint response contract and metric/tag coverage are confirmed.

## 12. Paul’s backend/data safeguards

- Do not create a BigQuery view or table function for the first implementation.
- Reuse `get_team_rankings_for_game()` rather than duplicating pregame-selection SQL.
- Add only the missing `games_in_window` lookup.
- Preserve raw ranking metadata and frontend-required lens tags.
- Compare the Lovable tag vocabulary against production `lens_tags` before claiming all six lenses are complete.
- Mark insufficient lenses unavailable instead of assigning zero.
- Keep all data access read-only.
- Do not change `/game`, model outputs, or orchestration.

## 13. Verification and acceptance gates

### Backend contract

- Valid DET/BUF ID returns the canonical DET and BUF teams.
- Unknown game ID does not infer teams and returns the documented unavailable/not-found response.
- `window_type = regular_season_to_date`.
- `as_of_date = 2026-09-14` and is strictly before `2026-09-17`.
- Both teams report `games_in_window = 1` from data, not a hardcoded value.
- Every returned percentile is null or between 0 and 100.
- No duplicate team/metric records exist.
- Lens tags are arrays.
- All returned source dates are pregame-safe.
- The endpoint includes every metric returned by the existing ranking function.
- The endpoint makes no BigQuery writes.

### Frontend contract

- Opening the calendar link makes one lens-context request for its game ID.
- The screen identifies regular-season-to-date and September 14, not August 23 preseason.
- DET and BUF each show one game in the evidence window.
- Existing Lens output is unchanged when the adapter receives evidence equivalent to the old static fixture.
- Switching games changes the query/cache key and evidence.
- Loading and failure never flash or silently substitute frozen preseason values.
- Missing metrics degrade only the affected lens and do not crash the page.

### Regression protection

- Existing `/games` output is unchanged.
- Existing `/game/<game_id>` output is unchanged.
- Existing scheduled pipeline behavior is unchanged.
- Existing learning/orchestrator behavior is unchanged.
- No production or development data is modified.

## 14. Implementation sequence

Each gate should stop for review before proceeding:

1. **Approve this plan.** Documentation only.
2. **Run the read-only 63-metric inventory.** Reconcile it against the 59 checked-in registry names.
3. **Obtain Lovable interface answers through Christian.** No direct agent-to-Lovable communication.
4. **Freeze `matchup_lens_v1`.** Confirm field names, scale, null handling, tag coverage, and error convention.
5. **Implement the isolated backend endpoint.** No pipeline or orchestrator changes.
6. **Verify locally/read-only.** DET/BUF contract, pregame safety, coverage, and unchanged existing endpoints.
7. **Deploy backend first.** Confirm the live endpoint before frontend switching.
8. **Have Christian direct Lovable integration.** Replace the live-game source through the adapter.
9. **Run end-to-end verification.** Confirm Week 1 evidence appears for the September 17 matchup.
10. **Review the branch diff.** Only intended additive files/lines may proceed.
11. **Merge quickly into `main` only after explicit approval.** Do not merge automatically.

## 15. Rollback

Backend rollback is removal or disabling of the isolated lens-context route. Because the endpoint is read-only and does not alter `/game`, pipelines, tables, or orchestrator state, rollback requires no data restoration.

Frontend rollback is switching the Matchup Lens source back to its explicitly labeled static development source. A failed live request must not trigger that rollback silently at runtime.

## 16. Definition of done

This effort is complete when:

- the endpoint accepts a canonical game ID;
- it returns complete, pregame-safe evidence for both scheduled teams;
- it reports the correct window, ranking date, source dates, and games in window;
- it dynamically includes every ranking metric returned by the existing query;
- all six Lens buckets are either supported or explicitly unavailable;
- Lovable consumes the endpoint through a small adapter;
- frozen preseason evidence is never represented as live matchup evidence;
- existing `/games`, `/game`, pipeline, learning, and orchestrator behavior remains unchanged;
- documentation reflects the final implemented contract;
- Christian explicitly approves the merge to `main`.
