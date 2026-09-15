# Chunk A — Evidence Inventory Handoff

**Status:** blocked  
**Branch:** `feature/matchup-lens-api`  
**Starting commit:** `0fc31dd1dab27def79c19c14d83c20bcbec52fe9`  
**Retrieval time (UTC):** 2026-09-15T22:18:13Z  
**Assigned model / effort:** `gpt-5.6-sol` / High  
**Scope:** Chunk A only — read-only evidence inventory

## Outcome

Chunk A is **blocked**, not failed. The repository-side inventory is complete, but the live acceptance gate cannot pass because this environment has no BigQuery connector, no `bq` or `gcloud` CLI, no configured Google application credentials, and no Google project environment setting. No live result was invented or promoted from the plan's reported evidence.

No application code, existing test, service, route, query, pipeline, schema, table, scheduled job, learning system, orchestrator state, Endpoint Plan, or Product Roadmap was changed. No deployment, merge, or BigQuery write was performed.

## Scope completed

- Read the Endpoint Plan and Product Roadmap completely, in the required order.
- Checked `documentation/New API/handoffs/`; it did not exist at the starting commit, so there were no earlier handoffs.
- Inspected the branch versions of:
  - `queries/game_queries.py`
  - `analytics/metric_registry.py`
  - `agg/build_windowed_metrics.py`
  - `agg/build_metric_rankings.py`
- Reconciled the plan's 59-item sample catalog against the checked-in registry.
- Identified the ranking builder's code-level eligibility filter, duplicate grain, percentile validation, tag normalization, and source-date carry-forward behavior.
- Defined the exact live read-only checks still required for the Chunk A acceptance gate.

## Evidence matrix

| Check | Result | Provenance | Gate |
|---|---|---|---|
| Plan sample catalog | 59 distinct metric names | Endpoint Plan §8 | Repository evidence |
| Current checked-in registry | 73 metrics; `EXPECTED_METRICS` and `METRIC_REGISTRY` agree exactly | `analytics/metric_registry.py` at starting commit | Pass |
| Registry vs plan catalog | All 59 plan names remain registered; 14 registered names are absent from the plan catalog | Exact set comparison of branch files | Pass |
| Code-eligible ranking metrics | 70 maximum before null/missing live values: 73 registered minus 3 explicit excludes | Registry plus `load_windowed_rows()` filter in ranking builder | Pass as code inventory only |
| Live rankings metric count | Not retrieved. The plan/roadmap reports 63 for 2026-09-14, but this run did not independently verify it | BigQuery access unavailable | **Blocked** |
| Actual 59-versus-live reconciliation | Not established. A 59-versus-63 claim would be stale/incomplete because the current registry contains 73 | Repository evidence plus blocked live query | **Blocked** |
| DET/BUF metric coverage | Not retrieved | BigQuery access unavailable | **Blocked** |
| Ranking raw duplicate grain | Builder validates `season + as_of_date + window_type + metric + team_id`; live selected snapshot not queried | `validate_rankings_df()` | **Blocked live check** |
| Windowed raw duplicate grain | Builder validates `season + team_id + data_date + window_type + metric`; live selected snapshot not queried | `validate_windowed_df()` | **Blocked live check** |
| Percentile null/range | Ranking schema requires `league_percentile`; builder validates 0–100. Live selected rows were not queried | Ranking builder | **Blocked live check** |
| Lens tags | Every registry metric has at least one configured tag; 99 distinct configured tag strings; builder normalizes tags to arrays | Registry and ranking builder | Pass as code inventory only |
| Exclusions | `pressure_rate`, `sacks_plus_sacks_taken`, and `sack_to_turnover_ratio` are explicit registry/ranking exclusions | Registry | Pass |
| Six-lens coverage | Cannot be claimed from repository tags alone because the current Lovable six-lens tag/input vocabulary is a Chunk B dependency; production row coverage also remains unqueried | Roadmap A/B boundary | **Blocked** |
| Source-date safety | Query helper chooses latest ranking `as_of_date < game_date`; ranking builder guarantees `source_data_date <= as_of_date` | `get_team_rankings_for_game()`, ranking builder | Pass as code logic only |
| DET/BUF source dates | Expected 2026-09-14 basis is reported by the plan, not independently verified here | BigQuery access unavailable | **Blocked** |
| `games_in_window` alignment | Windowed rows carry `games_in_window` and `latest_included_game_id` at `data_date`; counts must align to each ranking row's `source_data_date`, not merely the latest windowed date before kickoff | Windowed and ranking builders | Pass as design finding; **blocked live values** |

## Metric reconciliation

### Current repository truth

The checked-in registry contains **73** metrics. The Endpoint Plan's sample catalog contains **59**. The following 14 registered metrics are absent from that sample catalog:

- `passing_first_downs`
- `rushing_first_downs`
- `first_downs_from_penalties`
- `penalty_count`
- `penalty_yards`
- `two_point_conversions`
- `turnovers`
- `defensive_tds`
- `safeties`
- `defensive_two_point_returns`
- `defensive_or_special_teams_tds`
- `blocked_fg`
- `blocked_punt`
- `blocked_xp`

There are no plan-catalog metrics missing from the registry.

The ranking builder excludes metrics when `ranking_usage = 'exclude'` or `data_quality_status = 'exclude'`. The three explicit exclusions are:

- `pressure_rate`
- `sacks_plus_sacks_taken`
- `sack_to_turnover_ratio`

Therefore, the code-level maximum rankable inventory is **70**, subject to live non-null availability. The plan/roadmap's reported **63 live metrics** would imply seven fewer live metrics than that code-level maximum, but identifying those seven requires the blocked live query. The acceptance gate does not permit guessing them.

### Actual 59-versus-live result

**Blocked.** This run cannot truthfully provide the requested live set. The correct future comparison is now:

1. 59 plan-sample metrics;
2. 73 registered metrics;
3. 70 code-eligible ranking metrics;
4. the independently queried live ranking metric set.

## Duplicate-grain findings

The code defines and validates these grains:

- Ranking: `season + as_of_date + window_type + metric + team_id`.
- Windowed: `season + team_id + data_date + window_type + metric`.

The existing API helper converts ranking rows to dictionaries keyed by metric, so duplicate raw rows would be overwritten after retrieval. Therefore, runtime dictionary cardinality cannot prove raw uniqueness. The selected live source grain must be checked directly in BigQuery before implementation.

No live duplicate count was obtained.

## Source-date and games-in-window alignment

The ranking builder uses carry-forward evidence: for each ranking `as_of_date`, each team/metric can use its latest `source_data_date <= as_of_date`. Consequently:

- `as_of_date` is not necessarily the team's actual windowed snapshot date.
- `games_in_window` and `latest_included_game_id` must be joined to the canonical team, season, window type, metric, and the ranking row's `source_data_date`.
- Selecting only the latest windowed `data_date < game_date` can attach a count from a newer snapshot than the ranking evidence.
- If counts disagree across metrics/source dates, the contract must not silently collapse them into one team-level count.

The plan's expected DET/BUF values—ranking date 2026-09-14 and one game per team—remain acceptance targets, not verified results from this run.

## Lens-tag and coverage findings

- The registry configures tags for all 73 metrics.
- The registry contains 99 distinct tag strings.
- Ranking code normalizes repeated tag values to plain arrays and rejects non-array tag payloads during validation.
- Live tag vocabulary, live null/empty arrays, DET/BUF coverage, and six-lens readiness were not verified.
- Six-lens completeness also requires the current Lovable inclusion, exclusion, weight, modifier, and null-handling rules. Chunk A must not infer those rules.

## Read-only queries required to unblock Chunk A

All queries target project `nfl-stream-406420`, dataset `Analytics`, season 2026, window `regular_season_to_date`, selected ranking date 2026-09-14, and canonical DET/BUF IDs resolved from the schedule table. Retrieval time must be recorded when executed.

### 1. Live metric inventory and metadata coverage

```sql
SELECT
  metric,
  COUNT(DISTINCT team_id) AS teams_ranked,
  COUNT(*) AS row_count,
  COUNTIF(league_percentile IS NULL) AS null_percentile_rows,
  COUNTIF(league_percentile < 0 OR league_percentile > 100) AS invalid_percentile_rows,
  COUNTIF(lens_tags IS NULL) AS null_lens_tag_rows,
  COUNTIF(ARRAY_LENGTH(lens_tags) = 0) AS empty_lens_tag_rows,
  ANY_VALUE(ranking_usage) AS ranking_usage,
  ANY_VALUE(data_quality_status) AS data_quality_status,
  MIN(source_data_date) AS min_source_data_date,
  MAX(source_data_date) AS max_source_data_date
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026`
WHERE window_type = 'regular_season_to_date'
  AND as_of_date = DATE '2026-09-14'
GROUP BY metric
ORDER BY metric;
```

### 2. Resolve canonical DET/BUF team IDs and header

```sql
SELECT
  gameID,
  gameDate,
  season,
  gameWeek,
  seasonType,
  CAST(teamIDAway AS STRING) AS away_team_id,
  away AS away_team_abv,
  CAST(teamIDHome AS STRING) AS home_team_id,
  home AS home_team_abv
FROM `nfl-stream-406420.League.schedule`
WHERE gameID = '20260917_DET@BUF'
LIMIT 1;
```

If runtime configuration resolves a different schedule object, use that resolved object and record it.

### 3. DET/BUF selected ranking rows

```sql
SELECT *
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026`
WHERE window_type = 'regular_season_to_date'
  AND as_of_date = DATE '2026-09-14'
  AND CAST(team_id AS STRING) IN UNNEST(@canonical_team_ids)
ORDER BY team_id, metric;
```

### 4. Raw ranking duplicate grain

```sql
SELECT
  season,
  as_of_date,
  window_type,
  metric,
  CAST(team_id AS STRING) AS team_id,
  COUNT(*) AS row_count
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026`
WHERE window_type = 'regular_season_to_date'
  AND as_of_date = DATE '2026-09-14'
  AND CAST(team_id AS STRING) IN UNNEST(@canonical_team_ids)
GROUP BY season, as_of_date, window_type, metric, team_id
HAVING COUNT(*) > 1
ORDER BY team_id, metric;
```

### 5. Matching windowed rows at ranking source dates

```sql
WITH selected_rankings AS (
  SELECT DISTINCT
    CAST(team_id AS STRING) AS team_id,
    metric,
    source_data_date
  FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026`
  WHERE window_type = 'regular_season_to_date'
    AND as_of_date = DATE '2026-09-14'
    AND CAST(team_id AS STRING) IN UNNEST(@canonical_team_ids)
)
SELECT
  r.team_id,
  r.metric,
  r.source_data_date,
  w.games_in_window,
  w.latest_included_game_id,
  w.data_date,
  COUNT(*) OVER (
    PARTITION BY r.team_id, r.metric, r.source_data_date
  ) AS matching_windowed_rows
FROM selected_rankings r
LEFT JOIN `nfl-stream-406420.Analytics.team_metrics_windowed_2026` w
  ON CAST(w.team_id AS STRING) = r.team_id
 AND w.metric = r.metric
 AND w.window_type = 'regular_season_to_date'
 AND w.data_date = r.source_data_date
ORDER BY r.team_id, r.metric;
```

### 6. Tag vocabulary and metric membership

```sql
SELECT
  tag,
  COUNT(DISTINCT metric) AS metric_count,
  ARRAY_AGG(DISTINCT metric ORDER BY metric) AS metrics
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026`,
UNNEST(lens_tags) AS tag
WHERE window_type = 'regular_season_to_date'
  AND as_of_date = DATE '2026-09-14'
GROUP BY tag
ORDER BY tag;
```

## Commands and inspections used

- GitHub branch/ref read for `feature/matchup-lens-api`.
- Full GitHub file reads for the two controlling documents and relevant query/build/registry code.
- Set comparison of exact metric keys from the Endpoint Plan and branch registry.
- Read-only environment capability checks:
  - `command -v bq`
  - `command -v gcloud`
  - presence-only checks for `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_CLOUD_PROJECT`, `GCLOUD_PROJECT`, and `BQ_PROJECT`
  - available connector inventory for a BigQuery-capable tool

No credential values were printed.

## Files changed

- Added `documentation/New API/handoffs/Chunk_A_Evidence_Inventory_Handoff.md`

No other file was changed.

## Validation performed

- Confirmed the branch at start: `0fc31dd1dab27def79c19c14d83c20bcbec52fe9`.
- Confirmed the new handoff path did not previously exist.
- Confirmed 73 registry keys equal the 73 `EXPECTED_METRICS` keys.
- Confirmed all 59 plan-catalog keys exist in the registry.
- Confirmed the exact 14 registry additions relative to the sample catalog.
- Confirmed the exact three explicit ranking exclusions.
- Confirmed all registry metrics configure at least one lens tag.
- Confirmed no BigQuery read was available; recorded the gate as blocked.
- Confirmed only this handoff is intended for the commit.

## Unresolved blockers and risks

1. Live metric names and the actual live count remain unverified.
2. The live seven-metric difference between 70 code-eligible and the plan-reported 63 is unknown.
3. DET/BUF team IDs, row counts, shared coverage, missing metrics, percentiles, tags, source dates, duplicates, counts, and latest included game IDs remain unverified.
4. The plan's statement that the checked-in registry contains 59 metrics is inconsistent with the branch's current 73-metric registry. The Endpoint Plan must remain unedited in this chunk; Chunk C must resolve catalog semantics explicitly.
5. A naive team-level `games_in_window` lookup can mismatch carry-forward ranking evidence. Contract work must use source-date-aligned counts or explicitly define an honest unavailable state.
6. Six-lens readiness cannot be declared until both live evidence and Chunk B's frontend rules are available.

## Next recommended chunk

**Repeat/finish Chunk A with authorized read-only BigQuery access.** Do not proceed to Chunk B or contract freezing on the assumption that the reported 63 rows remain current. Once the live inventory is captured and reconciled, Chunk A can be marked complete and the roadmap can advance to Chunk B.

## Ready-to-copy prompt for the next chat

```text
[@GitHub](plugin://github@openai-curated-remote) Continue GameLens on branch feature/matchup-lens-api in csells10/meow.

This chat is a narrow continuation of Chunk A only. Do not create additional agents and do not begin Chunk B or implementation.

First read completely, in order:
1. documentation/New API/GameLens_Matchup_Lens_API_Endpoint_Plan.md
2. documentation/New API/GameLens_Matchup_Lens_API_Product_Roadmap.md
3. documentation/New API/handoffs/Chunk_A_Evidence_Inventory_Handoff.md

Chunk A is currently blocked only because the prior environment lacked live BigQuery access. Use authorized read-only BigQuery access and run the handoff's six inventory checks against nfl-stream-406420 for game 20260917_DET@BUF, regular_season_to_date, ranking as_of_date 2026-09-14.

Requirements:
- Resolve canonical DET/BUF team IDs from the configured schedule table.
- Capture project/dataset/table, selection date, query grain, and UTC retrieval time.
- Reconcile the 59 plan-sample metrics, 73 checked-in registry metrics, 70 code-eligible metrics, and the actual live metric set by exact name.
- Report DET/BUF metric counts, shared/missing sets, source dates, percentile null/range results, tag vocabulary, exclusions, raw duplicate grains, games_in_window, and latest_included_game_id.
- Align windowed evidence to each ranking row's source_data_date; do not attach a newer unrelated snapshot.
- Perform no BigQuery writes and make no code, test, service, route, query, pipeline, schema, table, scheduler, learning, orchestrator, Endpoint Plan, or Product Roadmap changes.
- If access is still unavailable, update only the handoff with the exact blocked check and remain blocked.
- If the live acceptance gate passes, update the existing handoff status to complete with actual results and provenance.
- Commit and push only the Chunk A handoff update to feature/matchup-lens-api.
- Report the commit SHA and local fast-forward commands.
- Stop after Chunk A.
```
