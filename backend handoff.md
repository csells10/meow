---

# Backend Status Addendum — Metric Registry, Cleaned Fact Tables, Windowed Metrics, and Crossover Plan

_Last updated: May 1, 2026_

## 1. Current Status

The backend foundation now has three completed additive layers:

```text
Analytics.game_metrics_flat
+ League.schedule
+ analytics/metric_registry.py
→ Analytics.game_team_metric_facts_{season}
→ Analytics.team_metrics_windowed_{season}
```

Built cleaned fact tables:

```text
Analytics.game_team_metric_facts_2023
Analytics.game_team_metric_facts_2024
Analytics.game_team_metric_facts_2025
```

Built windowed metric tables:

```text
Analytics.team_metrics_windowed_2023
Analytics.team_metrics_windowed_2024
Analytics.team_metrics_windowed_2025
```

Current live app behavior remains unchanged:

```text
✅ no app.py change yet
✅ no config.py change yet
✅ no /game query change yet
✅ no external NFL API re-fetch
✅ existing /game path still uses the old current source
```

This work is still foundation-only and additive.

---

## 2. Metric Registry Decision

The project now uses:

```text
analytics/metric_registry.py
```

as the canonical descriptive metadata source for metrics.

The registry answers:

```text
What does this metric mean?
```

It provides:

```text
metric
label
category
core_area
comparison_direction
higher_is_better
raw_or_derived
aggregation_method
numerator / denominator where needed
formatting hints
notes
```

The registry does **not** decide:

```text
model inputs
ranking eligibility
API exposure rules
frontend visibility
```

Those decisions should remain in the model/query/API layer after a separate audit.

Plain English rule:

```text
metric_registry.py = “What does this metric mean?”
model logic = “Which metrics should influence the lean?”
API/UI logic = “Which metrics should be shown?”
```

### Superseded registry idea

Older notes in this handoff may mention registry fields like:

```text
emit_to_fact
emit_to_windowed
emit_to_api
use_in_model
use_in_rankings
is_raw_ingredient
```

Those fields are now considered **outdated for the registry design**.

The current decision is:

```text
The metric registry is descriptive only.
It should not control model inputs or API exposure.
```

---

## 3. Legacy Metadata Decision

These source columns are now considered legacy/non-authoritative:

```text
Analytics.game_metrics_flat.category
Analytics.game_metrics_flat.core_area
```

Reason:

`parse_game_stats()` currently emits `category` and `core_area` into `game_metrics_flat`, but those values include older/generic labels such as:

```text
Offense
Defense
Snap Load
Raw
```

The cleaned fact table does **not** inherit those values.

Instead, the cleaned fact builder attaches metadata from:

```text
analytics/metric_registry.py
```

The cleaned fact table always uses registry-provided:

```text
label
category
core_area
comparison_direction
higher_is_better
raw_or_derived
aggregation_method
```

Plain English rule:

```text
game_metrics_flat = legacy/raw-ish source table
game_team_metric_facts_{season} = cleaned canonical fact table
team_metrics_windowed_{season} = model-ready aggregate table
```

---

## 4. Cleaned Fact Builder

Script:

```text
agg/build_metric_facts.py
```

Purpose:

```text
Read Analytics.game_metrics_flat
Join League.schedule
Ignore source category/core_area
Attach authoritative metadata from analytics/metric_registry.py
Normalize game phase fields
Write Analytics.game_team_metric_facts_{season}
```

Run command:

```bash
python -m agg.build_metric_facts --season 2025
```

Historical seasons:

```bash
python -m agg.build_metric_facts --season 2024
python -m agg.build_metric_facts --season 2023
```

Dry run:

```bash
python -m agg.build_metric_facts --season 2025 --dry-run
```

This script is additive only.

It does **not**:

```text
change /game
change app.py
change config.py
change api_call_nfl_stats.py
change queries/game_queries.py
change agg/aggregate_nfl_metrics_2025.py
re-call the external NFL API
drop or replace Analytics.team_metrics_season_2025
```

---

## 5. Cleaned Fact Table Behavior

The fact builder filters to the approved 58 metrics in `metric_registry.py`.

Unregistered legacy parser metrics are excluded from the cleaned fact table.

Currently excluded legacy metrics:

```text
blocked_fg
blocked_punt
blocked_xp
defensive_tds
penalty_count
penalty_yards
safeties
turnovers
```

This is expected.

These metrics may still exist in `game_metrics_flat`, but they are not part of the approved cleaned metric set.

---

## 6. Cleaned Fact Table Results

| Season | Table | Rows | Metrics | Games | Teams | Status |
|---|---|---:|---:|---:|---:|---|
| 2023 | `Analytics.game_team_metric_facts_2023` | 33,060 | 58 | 285 | 32 | ✅ validated |
| 2024 | `Analytics.game_team_metric_facts_2024` | 33,060 | 58 | 285 | 32 | ✅ validated |
| 2025 | `Analytics.game_team_metric_facts_2025` | 36,532 | 58 | 332 | 32 | ✅ validated |

### 2023 validation

Date range:

```text
2023-09-07 through 2024-02-11
```

Phase split:

```text
regular_season: 272 games / 31,552 rows
postseason: 13 games / 1,508 rows
```

Validation completed:

```text
✅ no duplicate fact grain rows
✅ no missing metadata
✅ no overlap with 2024 game IDs
```

### 2024 validation

Date range:

```text
2024-09-05 through 2025-02-09
```

Phase split:

```text
regular_season: 272 games / 31,552 rows
postseason: 13 games / 1,508 rows
```

Validation completed:

```text
✅ no duplicate fact grain rows
✅ no missing metadata
✅ no overlap with 2023 game IDs
```

### 2025 validation

Current status:

```text
✅ table built
✅ 58 metrics
✅ 32 teams
✅ 332 games
✅ registry metadata attached
✅ unregistered legacy parser metrics excluded
✅ script-level validation passed
```

---

## 7. Windowed Metrics Builder

Script:

```text
agg/build_windowed_metrics.py
```

Purpose:

```text
Read Analytics.game_team_metric_facts_{season}
Build phase-aware and rolling aggregate windows
Recalculate derived rates from summed ingredients
Write Analytics.team_metrics_windowed_{season}
```

Run command:

```bash
python -m agg.build_windowed_metrics --season 2025
```

Historical seasons:

```bash
python -m agg.build_windowed_metrics --season 2024
python -m agg.build_windowed_metrics --season 2023
```

Dry run:

```bash
python -m agg.build_windowed_metrics --season 2025 --dry-run
```

This script is additive only.

It does **not**:

```text
change /game
change app.py
change config.py
change api_call_nfl_stats.py
change queries/game_queries.py
re-call the external NFL API
drop or replace Analytics.team_metrics_season_2025
```

---

## 8. How `build_windowed_metrics.py` Is Implemented

`build_windowed_metrics.py` builds model-ready team aggregates from the cleaned fact table.

High-level flow:

```text
1. Load Analytics.game_team_metric_facts_{season}
2. Deduplicate source fact rows if needed
3. Pivot facts wide to one row per team/game
4. Sort each team’s games by global_week_order, game_date, and game_id
5. Build snapshots for each window_type
6. Recalculate derived metrics from summed raw ingredients
7. Attach metadata from metric_registry.py
8. Validate row grain, metadata, and rolling-window caps
9. Write Analytics.team_metrics_windowed_{season}
```

### Source table

Input:

```text
Analytics.game_team_metric_facts_{season}
```

The script intentionally builds from the cleaned fact table, not directly from:

```text
Analytics.game_metrics_flat
```

Reason:

```text
game_team_metric_facts_{season} already has cleaned schedule context,
normalized phase fields, and authoritative registry metadata.
```

### Pivot step

The script pivots from long format:

```text
season / game_id / team_id / metric / value
```

to wide format:

```text
one row per season / team_id / game_id
one column per metric
```

This makes it easier to sum ingredients and recalculate derived metrics.

### Window generation

The script currently builds these windows:

```text
regular_season_to_date
regular_plus_postseason_to_date
last_3_games
last_7_games
preseason_to_date
```

Window definitions:

| Window | Includes | Main use |
|---|---|---|
| `regular_season_to_date` | Completed regular-season games only | Main regular-season matchup logic |
| `regular_plus_postseason_to_date` | Completed regular-season + postseason games | Later playoff matchup logic |
| `last_3_games` | Last 3 completed meaningful games | Recent form support |
| `last_7_games` | Last 7 completed meaningful games | Medium recent form support |
| `preseason_to_date` | Completed preseason games only | Context/debug only |

Important rule:

```text
Preseason can exist for context/debugging,
but should not feed default Matchup Lean logic.
```

### Rate recalculation rule

The script does **not** average already-derived game-level percentages/rates.

Bad:

```text
average(points_per_play)
average(red_zone_efficiency)
average(yards_per_play)
```

Good:

```text
points_per_play = sum(actual_points) / sum(total_plays)
red_zone_efficiency = sum(red_zone_tds) / sum(red_zone_attempts)
yards_per_play = sum(total_yards) / sum(total_plays)
```

This was validated for `points_per_play` using Buffalo’s 2025 regular-season rows.

The windowed result matched the manual cumulative calculation with only tiny rounding differences.

### Denominator-zero rule

For `ratio_from_sums` metrics:

```text
if denominator is NULL or denominator = 0:
    value = NULL
else:
    value = numerator / denominator
```

The script does **not** replace zero denominators with `1`.

Reason:

```text
A fake fallback denominator creates misleading rates,
especially in early-season or tiny-sample windows.
```

### Output grain

Expected output grain:

```text
one row per season / team_id / data_date / window_type / metric
```

Where:

```text
data_date = date of the latest completed game included in that aggregate snapshot
```

This preserves the existing pregame-safety pattern:

```sql
data_date < @game_date
```

### Output table

Output:

```text
Analytics.team_metrics_windowed_{season}
```

Recommended current API migration target later:

```text
Analytics.team_metrics_windowed_2025
```

but `/game` should not switch yet.

---

## 9. Windowed Metrics Results

| Season | Table | Rows | Windows | Status |
|---|---|---:|---:|---|
| 2023 | `Analytics.team_metrics_windowed_2023` | 130,732 | 4 | ✅ built / validated |
| 2024 | `Analytics.team_metrics_windowed_2024` | 130,732 | 4 | ✅ built / validated |
| 2025 | `Analytics.team_metrics_windowed_2025` | 136,184 | 5 | ✅ built / validated |

### 2023 window summary

```text
last_3_games: 33,060 rows
last_7_games: 33,060 rows
regular_plus_postseason_to_date: 33,060 rows
regular_season_to_date: 31,552 rows
```

Date range:

```text
2023-09-07 through 2024-02-11
```

### 2024 window summary

```text
last_3_games: 33,060 rows
last_7_games: 33,060 rows
regular_plus_postseason_to_date: 33,060 rows
regular_season_to_date: 31,552 rows
```

Date range:

```text
2024-09-05 through 2025-02-09
```

### 2025 window summary

```text
last_3_games: 33,060 rows
last_7_games: 33,060 rows
preseason_to_date: 5,452 rows
regular_plus_postseason_to_date: 33,060 rows
regular_season_to_date: 31,552 rows
```

Date range:

```text
2025-08-07 through 2026-02-08
```

2025 has a `preseason_to_date` window because preseason rows exist in the 2025 cleaned fact table.

2023 and 2024 do not have preseason windows because their cleaned fact tables only contain regular season + postseason.

---

## 10. Windowed Metrics Validation Completed

### 2025 deep validation

Completed:

```text
✅ window summary / row shape passed
✅ duplicate grain check passed
✅ metadata completeness passed
✅ rolling-window caps passed
✅ phase isolation passed
✅ target-game exclusion passed
✅ derived-rate recalculation passed
✅ early-season sample-size behavior passed
✅ null value scan reviewed
✅ snap-load coverage reviewed
✅ metadata direction sanity passed
✅ playoff accumulation behavior passed
```

### Cross-season validation

Completed across 2023, 2024, and 2025:

```text
✅ combined duplicate grain check passed
✅ combined metadata completeness check passed
```

### 2024 validation

Completed:

```text
✅ window summary / row shape passed
✅ duplicate grain check passed
✅ metadata completeness passed
```

### 2023 validation

Completed from build validation and combined checks:

```text
✅ table built
✅ script validation passed
✅ duplicate grain check passed through combined validation
✅ metadata completeness passed through combined validation
```

Recommended optional remaining check:

```text
Run combined rolling-window cap SQL across 2023, 2024, and 2025.
```

The script-level validation already checks that:

```text
last_3_games <= 3
last_7_games <= 7
```

but the SQL check is still useful as an external BigQuery confirmation.

---

## 11. Old vs New Comparison Finding

A comparison for:

```text
20250914_SF@NO
```

showed that the old source table:

```text
Analytics.team_metrics_season_2025
```

included preseason + Week 1 values in the pregame snapshot dated:

```text
2025-09-07
```

Example old values:

```text
NO actual_points old = 62
SF actual_points old = 78
NO total_plays old = 252
SF total_plays old = 253
```

Those values exactly matched all cleaned fact rows through `2025-09-07`, including preseason.

The new source table:

```text
Analytics.team_metrics_windowed_2025
```

using:

```text
regular_season_to_date
```

correctly matched only regular-season facts through `2025-09-07`.

Example new values:

```text
NO actual_points new = 13
SF actual_points new = 17
NO total_plays new = 69
SF total_plays new = 72
games_in_window = 1
value_difference = 0
```

Conclusion:

```text
The windowed table fixes a real early-season data-lineage issue where preseason data leaked into the old season-to-date source.
```

This is a strong reason to continue toward the `/game` migration carefully.

---

## 12. Known Data Quality Note — Snap Load Metrics

These percentage/load metrics have low source coverage in 2025:

```text
offensive_snap_load
defensive_snap_load
special_teams_snap_pct
```

Game-level observed 2025 fact coverage:

```text
offensive_snap_load: 4 rows
defensive_snap_load: 4 rows
special_teams_snap_pct: 4 rows
```

Windowed 2025 non-null coverage remains low:

```text
regular_season_to_date: ~9.74% non-null
regular_plus_postseason_to_date: ~9.47% non-null
last_3_games: ~2.11% non-null
last_7_games: ~4.91% non-null
preseason_to_date: 0% non-null
```

Interpretation:

```text
Snap-load percentage metrics are retained in the registry,
but should be treated as low-coverage / not trusted for matchup logic
unless source coverage improves.
```

Do not remove them yet.

They are useful concepts, but the current source coverage is weak.

---

## 13. Current Checked-Off Phases

```text
✅ Phase 1 — Metric registry created
✅ Phase 2 — Cleaned fact table builder created
✅ Phase 2 — Cleaned fact tables built for 2023, 2024, and 2025
✅ Phase 3 — Schedule/game phase normalization added to cleaned fact table
✅ Phase 4 — Windowed metrics builder created
✅ Phase 5 — Windowed snapshots generated
✅ Phase 6 — Windowed tables written for 2023, 2024, and 2025
✅ Legacy parser metadata decision finalized
✅ Historical migration performed without external API re-fetch
✅ Old source preseason leakage identified and explained
```

---

# Crossover Plan — Safely Triggering New Builders Inside the App Framework

## 14. Crossover Purpose

The new builders exist, but they should not be wired into the live app immediately.

Correct migration posture:

```text
Foundation first.
Manual confidence second.
Environment-gated framework trigger third.
Feature-flagged API switch later.
Old path removal last.
```

Current safe flow remains:

```text
Cloud Scheduler / test route
→ API_CALLS
→ fetch games
→ fetch stats
→ fetch scores
→ old aggregate job
→ existing /game behavior
```

Manual builders currently available:

```bash
python -m agg.build_metric_facts --season 2025
python -m agg.build_windowed_metrics --season 2025
```

---

## 15. Phase C1 — Manual-Only Mode

This is the current recommended commit state.

Run manually after stats ingestion or historical backfills:

```bash
python -m agg.build_metric_facts --season 2025
python -m agg.build_windowed_metrics --season 2025
```

Historical seasons:

```bash
python -m agg.build_metric_facts --season 2024
python -m agg.build_windowed_metrics --season 2024

python -m agg.build_metric_facts --season 2023
python -m agg.build_windowed_metrics --season 2023
```

Status:

```text
✅ Current recommended commit state
```

---

## 16. Phase C2 — Environment-Gated Framework Trigger

Only after manual validation feels reliable, add optional post-stats jobs inside `app.py`.

These should run after the current old aggregate job.

Important:

```text
The old aggregate job should remain first because it supports the current live API path.
```

When enabled:

```text
stats inserted
→ run old aggregate job
→ optionally run cleaned fact builder
→ optionally run windowed metrics builder
→ do not change /game yet
```

When disabled:

```text
stats inserted
→ run old aggregate job
→ skip new builders
```

Recommended gating flags:

```text
ENABLE_METRIC_FACTS_BUILD=false
ENABLE_WINDOWED_METRICS_BUILD=false
```

---

## 17. Phase C2 Implementation Code

Add these constants near the top of `app.py`, after imports:

```python
NFL_SEASON = os.getenv("NFL_SEASON", "2025")

ENABLE_METRIC_FACTS_BUILD = (
    os.getenv("ENABLE_METRIC_FACTS_BUILD", "false").lower() == "true"
)

ENABLE_WINDOWED_METRICS_BUILD = (
    os.getenv("ENABLE_WINDOWED_METRICS_BUILD", "false").lower() == "true"
)
```

Add this helper function above `run_api_calls()`:

```python
def run_post_stats_jobs(stats_inserted: int):
    """
    Run downstream jobs after NFL stats are inserted.

    Safety design:
    1. Run the existing aggregate job first because it supports the current live API.
    2. Optionally run the cleaned metric fact builder only when enabled.
    3. Optionally run the windowed metrics builder only when enabled.
    4. Do not switch /game to the new tables here.

    The new builders are environment-gated so they can be tested inside the
    real framework without affecting production behavior by default.
    """
    season = NFL_SEASON

    # ------------------------------------------------------------
    # 1. Existing live aggregate job
    # ------------------------------------------------------------
    try:
        log_event(
            "info",
            "running_aggregate_job",
            reason="stats_inserted",
            count=stats_inserted,
            season=season,
        )

        from agg.aggregate_nfl_metrics_2025 import run_aggregate_for_season

        run_aggregate_for_season(season)

        log_event(
            "info",
            "aggregate_job_complete",
            season=season,
        )

    except Exception as e:
        log_event(
            "error",
            "aggregate_job_failed",
            season=season,
            error=str(e)[:500],
        )

        # The current aggregate job supports the live API path.
        # If this fails, surface the failure instead of silently continuing.
        raise

    # ------------------------------------------------------------
    # 2. Optional cleaned metric fact builder
    # ------------------------------------------------------------
    if ENABLE_METRIC_FACTS_BUILD:
        try:
            log_event(
                "info",
                "running_metric_facts_job",
                reason="stats_inserted",
                count=stats_inserted,
                season=season,
            )

            from agg.build_metric_facts import run_build_game_team_metric_facts

            fact_df = run_build_game_team_metric_facts(
                season=season,
                if_exists="replace",
                write=True,
            )

            log_event(
                "info",
                "metric_facts_job_complete",
                season=season,
                rows=len(fact_df),
            )

        except Exception as e:
            log_event(
                "error",
                "metric_facts_job_failed",
                season=season,
                error=str(e)[:500],
            )

            # During crossover, this should not break the live API path.
            # The old aggregate has already succeeded.
            # Keep this non-fatal until /game depends on the new tables.
            return

    else:
        log_event(
            "info",
            "metric_facts_job_skipped",
            reason="disabled_by_env",
            season=season,
        )

    # ------------------------------------------------------------
    # 3. Optional windowed metrics builder
    # ------------------------------------------------------------
    if ENABLE_WINDOWED_METRICS_BUILD:
        try:
            log_event(
                "info",
                "running_windowed_metrics_job",
                reason="stats_inserted",
                count=stats_inserted,
                season=season,
            )

            from agg.build_windowed_metrics import run_build_windowed_metrics

            windowed_df = run_build_windowed_metrics(
                season=season,
                if_exists="replace",
                write=True,
            )

            log_event(
                "info",
                "windowed_metrics_job_complete",
                season=season,
                rows=len(windowed_df),
            )

        except Exception as e:
            log_event(
                "error",
                "windowed_metrics_job_failed",
                season=season,
                error=str(e)[:500],
            )

            # During crossover, this should not break the live API path.
            return

    else:
        log_event(
            "info",
            "windowed_metrics_job_skipped",
            reason="disabled_by_env",
            season=season,
        )
```

Then replace the current conditional aggregation block inside `run_api_calls()`:

```python
# ------------------------------------------------------------
# Conditional aggregation
# ------------------------------------------------------------
# Only run the aggregation job if stats were actually inserted.
if stats_inserted > 0:
    log_event("info", "running_aggregate_job", reason="stats_inserted", count=stats_inserted)
    from agg.aggregate_nfl_metrics_2025 import run_aggregate_for_season
    run_aggregate_for_season("2025")
else:
    log_event("info", "aggregate_skipped", reason="no_stats_inserted")
```

with:

```python
# ------------------------------------------------------------
# Conditional post-stats jobs
# ------------------------------------------------------------
# Only run downstream jobs if stats were actually inserted.
if stats_inserted > 0:
    run_post_stats_jobs(stats_inserted)
else:
    log_event("info", "post_stats_jobs_skipped", reason="no_stats_inserted")
```

---

## 18. Phase C2 Environment Settings

Default production-safe values:

```text
ENABLE_METRIC_FACTS_BUILD=false
ENABLE_WINDOWED_METRICS_BUILD=false
```

Enable only when ready to test framework-triggered builds:

```text
ENABLE_METRIC_FACTS_BUILD=true
ENABLE_WINDOWED_METRICS_BUILD=true
```

Current season setting:

```text
NFL_SEASON=2025
```

Future season rollover:

```text
NFL_SEASON=2026
```

This prevents hardcoding the year in `app.py`.

---

## 19. Why the New Builders Should Be Non-Fatal at First

During crossover, the cleaned fact and windowed tables are not yet powering `/game`.

Therefore:

```text
old aggregate failure = serious live-path issue
new builder failure = log error, but do not break current live app
```

Once `/game` depends on the new windowed tables, this policy can change.

For now, the new builders should be treated as shadow/foundation jobs.

---

## 20. Phase C3 — API Switch Later

Do not update `/game/<game_id>` yet.

Only switch `/game` after all are true:

```text
✅ cleaned fact table validation passes
✅ windowed table validation passes
✅ old vs new outputs compared for known games
✅ early-season behavior reviewed
✅ playoff window behavior reviewed
✅ snap-load low-coverage issue documented
```

When ready, update:

```text
queries/game_queries.py::get_team_metrics()
```

from:

```text
Analytics.team_metrics_season_{season}
```

to:

```text
Analytics.team_metrics_windowed_{season}
```

using:

```sql
WHERE season = @season
  AND window_type = @window_type
  AND data_date < @game_date
```

Initial default windows:

```text
Regular season → regular_season_to_date
Wild Card → regular_season_to_date
Divisional Round+ → regular_plus_postseason_to_date
```

Do not make `last_3_games` or `last_7_games` the primary Matchup Lean input yet.

---

## 21. Phase C4 — Remove Old Path Only After Confidence

Do not remove the old aggregate path immediately after the first API switch.

Recommended transition:

```text
1. New windowed source powers /game behind a feature flag.
2. Compare known games.
3. Monitor logs.
4. Keep old aggregate available as fallback.
5. Remove old aggregate only after the new path is boring and stable.
```

Possible future flag:

```text
USE_WINDOWED_METRICS_FOR_GAME=false
```

Then later:

```text
USE_WINDOWED_METRICS_FOR_GAME=true
```

---

## 22. Commit Guidance

For the current commit, include:

```text
✅ agg/build_windowed_metrics.py
✅ backend handoff updates
```

Do not include:

```text
❌ app.py changes
❌ config.py changes
❌ /game query changes
```

Recommended commit message:

```bash
git commit -m "Add phase-aware windowed metrics builder"
```

---

## 23. Crossover Summary

Safe migration path:

```text
Manual build first.
Gated app trigger second.
Feature-flagged /game switch third.
Old path removal last.
```

This avoids a cliff jump and keeps the current API protected while the new backend foundation earns trust.

---
---

# May 2026 Update — Feature-Flagged `/game` Windowed Metrics Source

## What Was Accomplished

A small, safe `/game` crossover step was completed in:

```text
queries/game_queries.py

The API now supports a feature-flagged metric source switch:

USE_WINDOWED_METRICS_FOR_GAME=false

Default behavior remains unchanged.

When the flag is false:

/game uses Analytics.team_metrics_season_{season}

When the flag is true:

/game uses Analytics.team_metrics_windowed_{season}

The new windowed path uses the required pregame-safe filters:

WHERE CAST(season AS STRING) = @season
  AND window_type = @window_type
  AND data_date < @game_date

When `USE_WINDOWED_METRICS_FOR_GAME=true`, `/game` selects one `window_type`
from `Analytics.team_metrics_windowed_{season}` based on the target game’s phase.

Current default mapping:

- Preseason → `preseason_to_date`
- Regular season → `regular_season_to_date`
- Wild Card → `regular_season_to_date`
- Divisional Round → `regular_plus_postseason_to_date`
- Conference Championship → `regular_plus_postseason_to_date`
- Super Bowl → `regular_plus_postseason_to_date`

The selected window is then filtered with:

```sql
data_date < @game_date

Local Validation Completed

Test game:

20250914_SF@NO

Old path test:

USE_WINDOWED_METRICS_FOR_GAME=false
away metrics = 58
home metrics = 58

New windowed path test:

USE_WINDOWED_METRICS_FOR_GAME=true
window_type = regular_season_to_date
away metrics = 58
home metrics = 58

Important metric comparison:

OLD SF points_per_play = 0.308
OLD NO points_per_play = 0.246

NEW SF points_per_play = 0.236111
NEW NO points_per_play = 0.188406

Interpretation:

The API response shape stayed stable, but the new path returned cleaner regular-season-to-date values.

Manual sanity math:

SF: 17 points / 72 plays = 0.236111
NO: 13 points / 69 plays = 0.188406

This confirms the new /game source path is correctly using the phase-aware windowed table and avoiding the old preseason leakage issue for this test case.

Current Safety Status

Current state:

✅ queries/game_queries.py now supports a feature-flagged windowed source
✅ old source remains the default
✅ new source is available only when USE_WINDOWED_METRICS_FOR_GAME=true
✅ API metric payload shape remains compatible
✅ no app.py changes
✅ no config.py changes
✅ no ingestion changes
✅ no external NFL API re-fetch
✅ no source-table rebuild triggered by this change

Important:

The old aggregate path remains available as fallback.

This is not a full cutover yet.

It is a safe source-selection bridge.

Decision — Hold Off On app.py Plumbing For Now

The previous crossover plan proposed adding environment-gated post-stats builder orchestration inside app.py.

That work is now intentionally deferred.

Reason:

We do not want weekend/local testing of app.py orchestration to accidentally interact with ingestion routes,
API_CALLS, or raw source-table inserts before the idempotency behavior is fully reviewed.

Specific concern:

Analytics.game_metrics_flat is the raw-ish source table.
The stats ingestion process inserts parsed game rows and depends on games_to_process / boxscore status logic to avoid duplicate source data.

Current decision:

Do not touch app.py yet.
Do not touch config.py yet.
Do not add builders to API_CALLS.
Do not locally test via /test or scheduler routes for this crossover work.

The new cleaned/windowed builders remain manual for now.

Future app.py plumbing can still be added later, likely before or during preseason, but only after the ingestion/idempotency path is reviewed.

Updated PM Recommended Work Order
1. Feature-Flagged /game Windowed Source ✅ Completed

Status:

✅ Done

Completed work:

queries/game_queries.py now supports USE_WINDOWED_METRICS_FOR_GAME.
Old source remains default.
New windowed source can be enabled safely.
Local old/new path tests passed for 20250914_SF@NO.
2. Commit Current Feature-Flagged Source Switch

Status:

Next immediate step

Recommended commit:

git add queries/game_queries.py
git commit -m "Add feature-flagged windowed metrics source for game API"
3. Historical /game QA With Flag On

Status:

Next practical validation step

Test known games with:

USE_WINDOWED_METRICS_FOR_GAME=true

Validate:

/game returns expected response shape
team_comparison populates
core_area_comparison populates
game_profile populates
matchup_lean still reads correctly
model_trust still builds
frontend does not break

Recommended first regression game:

20250914_SF@NO

Then test a small batch across:

early season
midseason
late season
postseason
known low-confidence games
known high-confidence games
known misses
4. Pregame / Postgame Data Separation Audit

Status:

High priority

Confirm the new windowed source preserves the rule:

Pregame matchup logic must use only data available before the target game.

Required cutoff:

data_date < @game_date

Future improvement:

Consider moving from date-only cutoff to game_datetime cutoff if same-day ordering ever becomes important.
5. Metric Taxonomy / Category Consistency Audit

Status:

High priority foundation item

Confirm that metric names, categories, core areas, and higher/lower/context direction stay consistent across:

analytics/metric_registry.py
Analytics.game_team_metric_facts_{season}
Analytics.team_metrics_windowed_{season}
queries/game_queries.py
services/core_area_analysis.py
services/game_service.py
frontend labels

Goal:

Metric categories should be defined once and reused everywhere.
6. ETL Observability / app.py Post-Stats Plumbing

Status:

Deferred intentionally

This remains valuable, but should wait until after raw ingestion/idempotency behavior is reviewed.

Future desired work:

Add post-stats decision logging.
Add ENABLE_METRIC_FACTS_BUILD.
Add ENABLE_WINDOWED_METRICS_BUILD.
Keep builders disabled by default.
Run old aggregate first.
Run new builders only when explicitly enabled.

Reason for deferral:

Avoid accidental duplicate source-data concerns during local/weekend testing.
7. August / Preseason Builder Plumbing Test

Status:

Future

When real preseason data starts flowing, use preseason as a plumbing test.

Goal:

Validate builder orchestration, logging, and table writes.

Important:

Preseason can test the pipeline,
but preseason_to_date should not power default regular-season Matchup Lean logic.
8. Outcome Quality Labels

Status:

Future high-value model trust work

Do after the data source crossover is stable.

9. Postgame Swing Factors

Status:

Future high-value postgame explanation work

Do after outcome quality labels or alongside them.

10. Pregame Profile Weights

Status:

Future model-depth work

Should wait until the new source path and taxonomy audit are stable.

11. Recent Form / Rolling Window Sanity Check

Status:

Future

Use last_3_games and last_7_games as supporting checks later.

Do not make them the primary Matchup Lean input yet.

12. Game Context Flags + Early-Season Confidence Cap

Status:

Future

Important, especially for Weeks 1–4, but best handled after the new source path is stable.

Updated Crossover Summary

Current safe migration path is now:

1. Build cleaned/windowed tables manually. ✅
2. Add feature-flagged /game source switch. ✅
3. Keep old source as default. ✅
4. QA historical games with windowed source enabled.
5. Audit pregame/postgame cutoff behavior.
6. Audit metric taxonomy consistency.
7. Defer app.py builder automation until ingestion/idempotency concerns are reviewed.
8. Later add app.py post-stats plumbing with robust logging.
9. Enable builder plumbing during preseason testing.
10. Remove old path only after the new path is boring and stable.

Current posture:

Safe bridge built.
Valves closed by default.
Old path protected.
New path locally validated.
No ingestion risk introduced.