# Chunk A — Evidence Inventory Handoff

**Status:** complete  
**Branch:** `feature/matchup-lens-api`  
**Starting commit:** `0fc31dd1dab27def79c19c14d83c20bcbec52fe9`  
**Prior blocked-handoff commit:** `2416a0bdf913c0b0e36609884a93a0235b75d792`  
**Live verification completed (UTC):** 2026-09-15T22:32:55Z  
**Assigned model / effort:** `gpt-5.6-sol` / High  
**Scope:** Chunk A only — read-only evidence inventory

## Outcome

Chunk A is **complete**. Christian ran the approved read-only BigQuery queries from his authenticated local Google Cloud CLI and returned the full results. Repository and live evidence are now reconciled without changing data or application behavior.

No application code, existing test, service, route, shared query, pipeline, schema, table, scheduled job, learning system, orchestrator state, Endpoint Plan, or Product Roadmap was changed. No deployment, merge, or BigQuery write was performed.

## Scope completed

- Read the Endpoint Plan and Product Roadmap completely in the required order.
- Confirmed there were no handoffs before the first Chunk A handoff.
- Inspected the branch versions of:
  - `queries/game_queries.py`
  - `analytics/metric_registry.py`
  - `agg/build_windowed_metrics.py`
  - `agg/build_metric_rankings.py`
- Reconciled the plan's 59-item sample catalog, the 73-item checked-in registry, the 70 code-eligible metrics, and the 63 live ranking metrics by exact name.
- Verified canonical DET/BUF IDs and game context.
- Verified DET/BUF ranking coverage, missing metrics, percentiles, tag presence, raw duplicate grain, source dates, `games_in_window`, and `latest_included_game_id`.
- Captured the live DET/BUF lens-tag vocabulary.

## Provenance

| Item | Value |
|---|---|
| Google Cloud project | `nfl-stream-406420` |
| Dataset | `Analytics` |
| Rankings table | `team_metric_rankings_2026` |
| Windowed table | `team_metrics_windowed_2026` |
| Schedule table | `nfl-stream-406420.League.schedule` |
| Ranking selection | `window_type = regular_season_to_date`, `as_of_date = 2026-09-14` |
| Target game | `20260917_DET@BUF` |
| Scheduled game date | `2026-09-17` |
| Query execution | Christian's authenticated local BigQuery CLI, read-only |
| Evidence reported to this handoff | 2026-09-15 UTC |

## Evidence matrix

| Check | Verified result | Provenance |
|---|---|---|
| Plan sample catalog | 59 distinct names | Endpoint Plan §8 |
| Checked-in registry | 73 metrics; `EXPECTED_METRICS` and `METRIC_REGISTRY` agree | Branch `analytics/metric_registry.py` |
| Code-eligible metrics | 70: 73 registered minus 3 explicit exclusions | Registry plus ranking-builder filter |
| Live ranking catalog | 63 distinct metrics | BigQuery ranking snapshot |
| Metric health | No null percentiles, invalid percentiles, or missing tags in the live catalog | BigQuery inventory |
| League coverage | 61 metrics rank 32 teams; `red_zone_efficiency` ranks 31; `fourth_down_pct` ranks 20 | BigQuery inventory |
| Canonical away team | DET, team ID `11` | Schedule table |
| Canonical home team | BUF, team ID `4` | Schedule table |
| BUF coverage | 63 rows / 63 distinct metrics | BigQuery selected-team inventory |
| DET coverage | 62 rows / 62 distinct metrics | BigQuery selected-team inventory |
| Shared DET/BUF coverage | 62 metrics | Set comparison; BUF has all 63 and DET lacks one |
| DET missing metric | `fourth_down_pct` | BigQuery live-catalog anti-join |
| Selected-team percentiles | Zero null; zero outside 0–100 | BigQuery selected-team inventory |
| Selected-team tag presence | Zero null/empty tag arrays | BigQuery selected-team inventory |
| Raw ranking duplicates | Zero duplicate grain groups; zero rows in duplicate groups | BigQuery ranking-grain query |
| Ranking `as_of_date` | `2026-09-14` | Query selection |
| DET/BUF `source_data_date` | `2026-09-13` for every returned team metric | BigQuery selected-team inventory |
| BUF window alignment | 63/63 rows matched; `games_in_window = 1`; latest game `20260913_BUF@HOU` | Exact source-date join |
| DET window alignment | 62/62 rows matched; `games_in_window = 1`; latest game `20260913_NO@DET` | Exact source-date join |
| Unmatched window rows | Zero for both teams | Exact source-date join |
| Live DET/BUF tag vocabulary | 90 distinct tags | BigQuery `UNNEST(lens_tags)` query |

## Actual metric reconciliation

### Counts

| Inventory | Count |
|---|---:|
| Endpoint Plan sample catalog | 59 |
| Checked-in registry | 73 |
| Code-eligible after explicit exclusions | 70 |
| Live 2026-09-14 ranking catalog | 63 |

The original framing of “59 in code versus 63 live” is not current repository truth. The checked-in registry contains 73 metrics. The plan's 59-name JSON catalog is a sample/subset and must not become a runtime allowlist.

### Live metrics absent from the 59-item plan sample

All 14 registered additions are live:

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

### Plan-sample/registry metrics absent from live rankings

Ten registered names are absent from the live ranking catalog:

- Explicit ranking exclusions:
  - `pressure_rate`
  - `sacks_plus_sacks_taken`
  - `sack_to_turnover_ratio`
- Code-eligible metrics without live ranking rows:
  - `total_offensive_snaps`
  - `offensive_snap_load`
  - `total_defensive_snaps`
  - `defensive_snap_load`
  - `total_special_teams_snaps`
  - `special_teams_snap_pct`
  - `total_snaps`

Thus:

- 59 sample − 10 absent sample metrics + 14 live additions = 63 live metrics.
- 73 registry − 3 explicit exclusions − 7 other metrics without live ranking rows = 63 live metrics.

The endpoint must remain dynamic and return what the existing ranking helper supplies; it must not hardcode 59, 63, 70, or 73.

## Duplicate-grain findings

The ranking builder's protected grain is:

```text
season + as_of_date + window_type + metric + team_id
```

For DET and BUF at the selected 2026-09-14 snapshot:

- duplicate grain groups: `0`
- rows in duplicate groups: `0`

The existing helper still reshapes rows into dictionaries keyed by metric and would overwrite duplicates after retrieval. Therefore, this clean one-time inventory does not by itself create a runtime duplicate guarantee. Chunk C must decide whether the endpoint needs raw-boundary duplicate detection or whether this inventory plus existing builder validation is sufficient.

The exact source-date windowed join also returned 63 BUF and 62 DET rows—equal to their ranking counts—with zero unmatched rows. No duplicate multiplication was observed at the matching windowed grain.

## Source-date and games-in-window alignment

The important distinction is now proven:

- Ranking comparison date: `2026-09-14`
- Actual DET and BUF evidence date: `2026-09-13`
- Target game date: `2026-09-17`

The ranking builder carries forward each team's latest `source_data_date <= as_of_date`. The plan's expected September 14 ranking basis is correct, but September 14 is not the teams' source-data date.

Exact matching on canonical team, metric, window, and ranking `source_data_date` produced:

| Team | Ranking metrics | Matched window rows | Games | Latest included game |
|---|---:|---:|---:|---|
| BUF / 4 | 63 | 63 | 1 | `20260913_BUF@HOU` |
| DET / 11 | 62 | 62 | 1 | `20260913_NO@DET` |

This confirms both one-game counts from data. A lookup that selects only the latest windowed date before kickoff could attach evidence newer than the ranking source. Chunk C must freeze source-date-aligned count semantics.

## Coverage and missing-metric finding

BUF contains all 63 live catalog metrics. DET contains 62 and lacks only `fourth_down_pct`.

The absence is compatible with a zero-denominator condition (no fourth-down attempts), but this query set did not separately prove the causal numerator/denominator values. The safe product behavior is unchanged: preserve the missing metric as missing and never convert it to zero.

Transport coverage for DET/BUF is therefore:

- away metric count: 62
- home metric count: 63
- shared metric count: 62
- missing away metrics: [`fourth_down_pct`]
- missing home metrics: []

Whether that missing metric makes a particular frontend lens incomplete remains a Chunk B/C contract question.

## Lens-tag findings

Every returned DET/BUF metric has a non-empty tag array. The selected matchup exposes 90 distinct live tags:

```text
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
```

This proves backend tag availability, not six-lens readiness. The exact Lovable tag inclusion, exclusion, weight, modifier, and null-handling rules remain the explicit Chunk B dependency.

## Queries and commands used

All BigQuery commands used `--project_id=nfl-stream-406420 --use_legacy_sql=false` and were read-only.

1. Live metric inventory grouped by metric, including team count, row count, percentile checks, tag checks, usage, quality, and source-date range.
2. Schedule lookup for `20260917_DET@BUF`.
3. Selected DET/BUF ranking coverage grouped by canonical team.
4. Live-catalog anti-join to identify team-specific missing metrics.
5. Ranking duplicate-grain query for the selected teams/date/window.
6. Exact ranking-to-windowed join on canonical team, metric, window, and `source_data_date`.
7. `UNNEST(lens_tags)` vocabulary query for DET/BUF.

Repository inspections used GitHub branch reads for the two controlling documents, prior handoff, ranking/windowed query code, builders, and registry.

The local CLI required this session-only environment correction before use:

```bash
export CLOUDSDK_PYTHON='C:\Users\csell\OneDrive\Desktop\Projects\Meow\meow\nfl\Scripts\python.exe'
```

No credential values were printed or changed.

## Files changed

- Updated `documentation/New API/handoffs/Chunk_A_Evidence_Inventory_Handoff.md`

No other file was changed.

## Validation performed

- Verified canonical game, date, season phase, week, and team IDs.
- Verified the live catalog contains 63 exact metric names.
- Verified the 59/73/70/63 set reconciliation.
- Verified league team coverage and the two partial metrics.
- Verified DET/BUF row and distinct-metric counts.
- Verified DET's only missing metric.
- Verified zero null/out-of-range percentiles for DET/BUF.
- Verified zero null/empty tag arrays for DET/BUF.
- Verified zero selected ranking duplicate groups.
- Verified every DET/BUF ranking row matches the exact windowed source date.
- Verified both one-game counts and latest included game IDs.
- Verified all evidence dates precede the September 17 target game.
- Verified 90 distinct live DET/BUF tags.
- Verified only this handoff is changed by the completion commit.

## Unresolved risks for later chunks

1. Chunk B must supply the exact frontend six-lens rules before lens readiness can be claimed.
2. Chunk C must define whether the response's `metric_catalog` means the live league catalog, union of team metrics, registry catalog, or another explicit denominator.
3. Chunk C must freeze source-date-aligned `games_in_window` behavior.
4. Chunk C must decide the endpoint's runtime duplicate-protection policy because the helper's dictionary reshape cannot detect overwritten duplicates.
5. Chunk C must define partial-lens handling for DET's missing `fourth_down_pct`.
6. The seven code-eligible snap metrics have no live ranking rows at this snapshot; they must not be fabricated or silently counted as live coverage.

## Next recommended chunk

**Chunk B — Obtain the exact frontend contract.** Chunk A's evidence gate is satisfied. Chunk B remains documentation/interface discovery only and must not begin implementation or contact Lovable directly; Christian owns all Lovable communication.

## Ready-to-copy prompt for the next chat

```text
[@GitHub](plugin://github@openai-curated-remote) Continue GameLens on branch feature/matchup-lens-api in csells10/meow.

This is a controlled execution of Chunk B only: obtain the exact frontend contract.

Use the model and effort assigned to Chunk B in documentation/New API/GameLens_Matchup_Lens_API_Product_Roadmap.md. Keep the task narrow. Do not create additional agents. Do not proceed to Chunk C or any later chunk.

First read completely, in order:
1. documentation/New API/GameLens_Matchup_Lens_API_Endpoint_Plan.md
2. documentation/New API/GameLens_Matchup_Lens_API_Product_Roadmap.md
3. documentation/New API/handoffs/Chunk_A_Evidence_Inventory_Handoff.md

Chunk A is complete. Treat its live 59/73/70/63 reconciliation, DET/BUF coverage, September 13 source dates, one-game counts, latest included game IDs, duplicate findings, and 90-tag vocabulary as the accepted evidence basis.

For Chunk B:
- Use Endpoint Plan §§10–11 and the roadmap's Chunk B brief.
- Prepare one consolidated question packet for Christian to send to Lovable.
- Request the exact current LensSnapshot and team-row TypeScript interfaces; six-lens definitions; required tags and inputs; weights/modifiers/exclusions; missing/null behavior; 0–100 percentile expectation; authenticated API helper/base URL; cache behavior; and smallest adapter insertion point.
- Produce a normalized field-mapping handoff and unresolved-question list.
- Do not contact Lovable yourself.
- Do not infer missing formulas or eligibility rules.
- Do not change application code, tests, services, routes, queries, pipelines, schemas, tables, jobs, learning systems, orchestrator state, the Endpoint Plan, or Product Roadmap.
- Do not deploy or merge.
- Create documentation/New API/handoffs/Chunk_B_Frontend_Contract_Handoff.md.
- The handoff must include status, branch/starting commit, scope, accepted Chunk A inputs, the ready-to-copy Lovable question packet, returned answers if Christian provides them in the chat, normalized field mapping, unresolved blockers, files changed, validation, next recommended chunk, and a complete prompt for the following chat.
- If Lovable's answers are not returned in the same chat, mark Chunk B blocked and stop.
- Commit and push only the new or updated Chunk B handoff to feature/matchup-lens-api.
- Report the commit SHA and local fast-forward commands.
- Stop after Chunk B.
```
