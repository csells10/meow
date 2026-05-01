---

# Backend Status Addendum — Metric Registry, Cleaned Fact Tables, and Crossover Plan

_Last updated: May 1, 2026_

## 1. Current Status

The first backend foundation step is complete:

```text
Analytics.game_metrics_flat
+ League.schedule
+ analytics/metric_registry.py
→ Analytics.game_team_metric_facts_{season}
```

Built and validated tables:

```text
Analytics.game_team_metric_facts_2023
Analytics.game_team_metric_facts_2024
Analytics.game_team_metric_facts_2025
```

Current commit should remain **foundation-only**:

```text
✅ analytics/metric_registry.py
✅ agg/build_metric_facts.py
✅ backend handoff documentation updates
❌ no app.py change yet
❌ no config.py change yet
❌ no /game query change yet
```

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

The cleaned fact table should always use registry-provided:

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
```

---

## 4. New Script Added

New script:

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

## 6. Built Table Results

| Season | Table | Rows | Metrics | Games | Teams | Status |
|---|---|---:|---:|---:|---:|---|
| 2023 | `Analytics.game_team_metric_facts_2023` | 33,060 | 58 | 285 | 32 | ✅ validated |
| 2024 | `Analytics.game_team_metric_facts_2024` | 33,060 | 58 | 285 | 32 | ✅ validated |
| 2025 | `Analytics.game_team_metric_facts_2025` | 36,532 | 58 | 332 | 32 | ✅ built / first-pass validated |

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

Recommended remaining 2025 spot-checks before windowed aggregation:

```text
phase split
duplicate fact grain SQL
metadata completeness SQL
snap-load coverage note preserved
```

---

## 7. Known Data Quality Note — Snap Load Metrics

These percentage/load metrics have low source coverage in 2025:

```text
offensive_snap_load
defensive_snap_load
special_teams_snap_pct
```

Observed 2025 coverage:

```text
offensive_snap_load: 4 rows
defensive_snap_load: 4 rows
special_teams_snap_pct: 4 rows
```

Raw snap-count fields are present more broadly:

```text
total_offensive_snaps
total_defensive_snaps
total_special_teams_snaps
total_snaps
```

Interpretation:

```text
Snap-load percentage metrics are retained in the registry,
but should be treated as low-coverage / not trusted for 2025 analysis
unless source coverage improves.
```

Do not remove them yet.

They are useful concepts, but the current source coverage is weak.

---

## 8. Current Checked-Off Phases

```text
✅ Phase 1 — Metric registry created
✅ Phase 2 — Cleaned fact table builder created
✅ Phase 2 — Cleaned fact tables built for 2023, 2024, and 2025
✅ Legacy parser metadata decision finalized
✅ Historical migration performed without external API re-fetch
```

---

## 9. Next Backend Phase

Next task:

```text
Build windowed aggregate tables from cleaned fact tables
```

Target script:

```text
agg/build_windowed_metrics.py
```

Target outputs:

```text
Analytics.team_metrics_windowed_2023
Analytics.team_metrics_windowed_2024
Analytics.team_metrics_windowed_2025
```

These should be built from:

```text
Analytics.game_team_metric_facts_{season}
```

not directly from:

```text
Analytics.game_metrics_flat
```

Initial `window_type` values:

```text
regular_season_to_date
regular_plus_postseason_to_date
last_3_games
last_7_games
preseason_to_date
```

Important rule:

```text
Preseason can exist for context/debugging,
but should not feed default Matchup Lean logic.
```

---

# Crossover Plan — Safely Triggering `build_metric_facts.py` Inside the App Framework

## 10. Crossover Purpose

The new fact table builder exists, but it should not be wired into the live app immediately.

Correct migration posture:

```text
Foundation first.
Manual confidence second.
Environment-gated framework trigger third.
Windowed tables fourth.
API switch later.
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

The new fact table builder remains manual for now:

```bash
python -m agg.build_metric_facts --season 2025
```

---

## 11. Phase C1 — Manual-Only Mode

This is the current recommended commit state.

Run manually after stats ingestion or historical backfills:

```bash
python -m agg.build_metric_facts --season 2025
python -m agg.build_metric_facts --season 2024
python -m agg.build_metric_facts --season 2023
```

Use this while validating:

```text
fact table grain
metadata completeness
phase split
row counts
snap-load coverage
old vs new consistency later
```

Status:

```text
✅ Current recommended commit state
```

---

## 12. Phase C2 — Environment-Gated Framework Trigger

Only after manual validation feels reliable, add an optional post-stats job inside `app.py`.

This should run after the current old aggregate job.

Important:

```text
The old aggregate job should remain first because it supports the current live API path.
```

When enabled:

```text
stats inserted
→ run old aggregate job
→ optionally run cleaned fact builder
→ do not change /game yet
```

When disabled:

```text
stats inserted
→ run old aggregate job
→ skip cleaned fact builder
```

The gating flag should default to off:

```text
ENABLE_METRIC_FACTS_BUILD=false
```

---

## 13. Phase C2 Implementation Code

Add these constants near the top of `app.py`, after imports:

```python
NFL_SEASON = os.getenv("NFL_SEASON", "2025")

ENABLE_METRIC_FACTS_BUILD = (
    os.getenv("ENABLE_METRIC_FACTS_BUILD", "false").lower() == "true"
)
```

Add this helper function above `run_api_calls()`:

```python
def run_post_stats_jobs(stats_inserted: int):
    """
    Run downstream jobs after NFL stats are inserted.

    Current safety design:
    1. Run the existing aggregate job first because it supports the current live API.
    2. Optionally run the new cleaned metric fact builder only when enabled.
    3. Do not switch /game to the new table here.

    This function is intentionally environment-gated so the new fact builder can
    be tested in the real framework without affecting production behavior by default.
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
    if not ENABLE_METRIC_FACTS_BUILD:
        log_event(
            "info",
            "metric_facts_job_skipped",
            reason="disabled_by_env",
            season=season,
        )
        return

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
        # Keep this non-fatal until /game depends on the new table.
        return
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

## 14. Phase C2 Environment Settings

Default production-safe value:

```text
ENABLE_METRIC_FACTS_BUILD=false
```

Enable only when ready to test framework-triggered fact builds:

```text
ENABLE_METRIC_FACTS_BUILD=true
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

## 15. Why the Fact Builder Should Be Non-Fatal at First

During crossover, the cleaned fact table is not yet powering `/game`.

Therefore:

```text
old aggregate failure = serious live-path issue
metric facts failure = log error, but do not break current live app
```

Once `/game` depends on the new windowed tables, this policy can change.

For now, the fact builder should be treated as a shadow/foundation job.

---

## 16. Phase C3 — Add Windowed Metrics Builder Later

After the fact builder is stable in manual or gated mode, add the future windowed builder:

```text
agg/build_windowed_metrics.py
```

Target outputs:

```text
Analytics.team_metrics_windowed_2023
Analytics.team_metrics_windowed_2024
Analytics.team_metrics_windowed_2025
```

Future post-stats flow:

```text
stats inserted
→ old aggregate job
→ cleaned fact builder
→ windowed metrics builder
→ /game still unchanged until validation passes
```

The windowed builder should also be environment-gated at first:

```text
ENABLE_WINDOWED_METRICS_BUILD=false
```

---

## 17. Phase C4 — API Switch Later

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

## 18. Phase C5 — Remove Old Path Only After Confidence

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

## 19. Commit Guidance

For the current commit, do **not** include Phase C2 `app.py` changes unless intentionally testing the gated framework trigger.

Recommended current commit contents:

```text
✅ analytics/metric_registry.py
✅ agg/build_metric_facts.py
✅ backend handoff updates
❌ app.py changes
❌ config.py changes
❌ /game query changes
```

Recommended commit message:

```bash
git commit -m "Add metric registry and cleaned fact table builder"
```

Later crossover commit message:

```bash
git commit -m "Add gated metric facts refresh after stats ingestion"
```

---

## 20. Crossover Summary

Safe migration path:

```text
Manual build first.
Gated app trigger second.
Windowed tables third.
Feature-flagged /game switch fourth.
Old path removal last.
```

This avoids a cliff jump and keeps the current API protected while the new backend foundation earns trust.

---

## Windowed Metrics Status

`agg/build_windowed_metrics.py` has been added and successfully built windowed metric tables for:

- `Analytics.team_metrics_windowed_2023`
- `Analytics.team_metrics_windowed_2024`
- `Analytics.team_metrics_windowed_2025`

The script builds from:

```text
Analytics.game_team_metric_facts_{season}

and writes:

Analytics.team_metrics_windowed_{season}

Validated so far:

2025 window summary passed
2025 duplicate grain check passed
2025 metadata completeness passed
2025 rolling-window caps passed
2025 phase isolation passed
2025 target-game exclusion passed
2025 derived-rate recalculation passed
2025 early-season sample-size behavior passed
2025 playoff accumulation behavior passed
Combined 2023/2024/2025 duplicate grain check passed
Combined 2023/2024/2025 metadata completeness check passed

Known caution:

Snap-load percentage metrics remain low coverage and should not be trusted for matchup logic yet.