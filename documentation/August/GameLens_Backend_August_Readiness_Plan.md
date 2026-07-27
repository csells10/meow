# GameLens Backend August Readiness Plan

## Current Status

Created: 2026-05-29  
Deadline: August preseason  
Status: Planning / not implemented

Primary goal:
Turn GameLens from a manually guided QA/modeling workflow into a reliable daily backend pipeline.

**Deadline target:** August preseason window  
**Purpose:** Turn GameLens from a manually-tested model/dashboard into a dependable daily pipeline that can ingest games, rebuild model-ready tables, run claim/game validation layers, and publish admin health summaries with enough observability to trust it.

## Not the Goal

This document is not about improving the model directly.

The goal is to make sure the backend can:
- ingest data
- rebuild derived tables
- refresh rankings
- validate claims
- summarize admin health
- report failures clearly

---

## 1. Why this work matters

Right now, GameLens has strong model-improvement momentum, but the production risk is not only model quality. The bigger August risk is that the app still depends on manual runs, manual sequencing, and manual interpretation.

By preseason, the backend should be able to answer:

1. Did new game/schedule/score/stat data arrive?
2. Which downstream tables rebuilt successfully?
3. Is `/game` reading from the correct pregame-safe source?
4. Did Level 1 → Level 4 claim/game validation run in the correct order?
5. What changed in yesterday’s admin/model-health stats?
6. If something failed, where did it fail and can it safely be replayed?

The goal is not to overfit. The goal is to create a reliable feedback loop.

---

## 2. Current known pipeline pieces

### Current ingestion layer

Existing Cloud Scheduler entry path triggers the Flask app, which runs configured API calls in sequence:

```text
Cloud Scheduler / POST / 
  -> setup_schedules()
  -> run_api_calls()
  -> configured API calls
```

Current API calls include:

```text
NFL Game Schedule API Call
NFL Stats API Call
NFL Scores API Call
```

Current behavior already has one important gate: aggregation only runs when stats rows were inserted.

Current limitation: the current aggregation hook still points to an older aggregate job and does not yet orchestrate the newer GameLens table builders.

---

## 3. Desired daily production chain

Recommended production order:

```text
1. Schedule ETL
   - refresh yesterday
   - refresh today
   - refresh next 2 days

2. Scores ETL
   - load completed/final scores
   - mark scores loaded

3. Boxscore/stats ETL
   - load completed/final boxscore stats
   - mark boxscore loaded

4. GameLens metric table builds
   - build game_team_metric_facts_{season}
   - build team_metrics_windowed_{season}
   - build team_metric_rankings_{season}

5. GameLens runtime/API readiness checks
   - verify /game can resolve header
   - verify pregame-safe windowed metrics exist
   - verify ranking_context exists where expected

6. Claim/game learning pipeline
   - Level 1: build claim training examples
   - Level 2: update claim validation
   - Level 3: update engineered claim features
   - Level 4: build claim-language calibration summaries

7. Admin health refresh
   - claim validation summary
   - game validation summary
   - confidence/core-area matrix
   - calibrated vs current confidence comparison
   - daily trend snapshots

8. Pipeline run summary
   - write success/failure status
   - write row counts
   - write warnings
   - notify/log if failure or material drift
```

---

## 4. Trigger design

### Trigger 1 — ingestion trigger

This is the existing scheduler entry point. It should remain small and boring.

Responsibilities:

- call schedule/scores/stats jobs
- record whether data was inserted
- exit cleanly if nothing changed
- write a pipeline run record

### Trigger 2 — metric build trigger

This should run only when postgame stat data exists.

Gate examples:

```sql
SELECT COUNT(*) AS stats_rows
FROM `nfl-stream-406420.Analytics.game_metrics_flat`
WHERE gameID IN (
  SELECT gameID
  FROM `nfl-stream-406420.League.schedule`
  WHERE gameStatus IN ('Final', 'Final/OT')
);
```

Recommended builder order:

```bash
python -m agg.build_metric_facts --season 2025 --write-bigquery
python -m agg.build_windowed_metrics --season 2025 --write-bigquery
python -m agg.build_metric_rankings --season 2025 --write-bigquery
```

Exact module names may need to match the repo, but the sequencing should stay stable.

### Trigger 3 — learning/validation trigger

This should run after metric facts/windowed/rankings are healthy.

Recommended order:

```bash
python -m agg.build_claim_training_examples --payload-run <payload_run> --run-id <run_id> --write-bigquery --replace-run
python -m agg.update_claim_training_validation --run-id <run_id> --write-bigquery
python -m agg.gamelens_training.update_claim_training_features --run-id <run_id> --write-bigquery
python -m agg.gamelens_training.build_claim_language_calibration --run-id <run_id> --write-bigquery --replace-run
```

Important: Level 1 currently depends on saved `/game` payload JSON files. Before full automation, decide whether production Level 1 should:

1. continue reading saved payload runs, or
2. generate payloads directly from the API for completed games, save them, then build claims.

The second option is better for daily automation.

---

## 5. Recommended new orchestration object

Create a small orchestration module rather than stuffing everything into `app.py`.

Suggested file:

```text
services/gamelens_pipeline_orchestrator.py
```

Suggested responsibilities:

```python
def run_daily_pipeline(load_date=None, season=None, mode="daily"):
    run = start_pipeline_run(load_date=load_date, season=season, mode=mode)

    ingestion_result = run_ingestion(load_date=load_date)

    if not ingestion_result.stats_inserted:
        mark_stage_skipped("metric_builds", reason="no_stats_inserted")
        return finish_pipeline_run(run)

    metric_result = run_metric_builds(season=season)
    assert_metric_builds_healthy(metric_result)

    payload_result = collect_or_refresh_gamelens_payloads(load_date=load_date)
    learning_result = run_claim_learning_pipeline(run_id=payload_result.run_id, season=season)

    admin_result = refresh_admin_health(run_id=payload_result.run_id, season=season)

    return finish_pipeline_run(run)
```

---

## 6. Add a pipeline run ledger

This is one of the most important missing pieces.

Suggested BigQuery table:

```text
Analytics.gamelens_pipeline_runs
```

Suggested columns:

```sql
run_id STRING,
run_date DATE,
load_date DATE,
season STRING,
mode STRING, -- daily, manual_backfill, preseason_test, replay
status STRING, -- running, success, partial_success, failed
started_at TIMESTAMP,
finished_at TIMESTAMP,

schedule_inserted_rows INT64,
score_inserted_rows INT64,
stats_inserted_rows INT64,

facts_rows INT64,
windowed_rows INT64,
ranking_rows INT64,
claim_rows INT64,
validated_claim_rows INT64,
level3_feature_rows INT64,
level4_calibration_rows INT64,

warning_count INT64,
error_count INT64,
notes STRING
```

This table becomes the scoreboard for whether the machine actually ran.

---

## 7. `/game` simplification idea

The idea is good, but be careful with the word “view.”

A normal BigQuery view does not really “accept a game_id” like a function. Better options:

### Option A — BigQuery table-valued function

Use this when you want SQL to accept a `game_id` parameter.

Example shape:

```sql
CREATE OR REPLACE TABLE FUNCTION `nfl-stream-406420.Analytics.fn_game_context`(input_game_id STRING)
AS (
  SELECT ...
  FROM ...
  WHERE game_id = input_game_id
);
```

### Option B — curated flattened view + Python service formatter

This is probably best for GameLens.

Use BigQuery to simplify the raw data joins, but keep JSON construction in Python:

```text
BigQuery/table function = clean source rows
Python game_service.py = product response shape, wording, guardrails
Frontend = display only
```

Suggested future source:

```text
Analytics.gamelens_game_context_v1
```

or

```text
Analytics.fn_gamelens_game_context(game_id)
```

Do not try to force the full nested `/game` JSON into SQL unless it actually reduces complexity.

---

## 8. Admin daily summary

Create a daily materialized admin summary separate from the existing live aggregate endpoint.

Suggested table:

```text
Analytics.gamelens_admin_daily_summary
```

Suggested grain:

```text
summary_date + season + run_id + summary_type + bucket fields
```

Examples:

```text
summary_type = claim_validation_baseline
summary_type = game_level_calibration
summary_type = calibrated_game_level_calibration
summary_type = core_area_alignment_matrix
summary_type = feature_scorecard
summary_type = category_matrix
```

This lets the admin page answer:

- What did yesterday look like?
- Is claim validation improving?
- Did Medium/High confidence drift?
- Did the calibrated matrix behave better than the current matrix?
- Are new feature buckets helping or just adding noise?

---

## 9. Keep same Claim example table or create a new one?

Recommendation: **keep the same claim example table as the canonical row-level table**, but use stronger run/version discipline.

Keep:

```text
Analytics.gamelens_claim_training_examples
```

Why:

- It already represents the right grain: one row per pregame claim.
- It already supports `run_id`.
- Level 1 → Level 3 are designed around updating/enriching this table.
- Creating a new table too early adds confusion unless the grain changes.

Add or enforce:

```text
run_id
model_version
feature_formula_version
pipeline_run_id
created_at
updated_at
source_payload_run
source_payload_path
```

Create new tables only for different grains:

```text
Analytics.gamelens_claim_language_calibration      -- summary/calibration grain
Analytics.gamelens_admin_daily_summary             -- admin daily snapshot grain
Analytics.gamelens_game_model_outcomes             -- game-level outcome grain
Analytics.gamelens_pipeline_runs                   -- pipeline/run-control grain
```

Rule of thumb:

> Same grain, same table. New grain, new table.

---

## 10. Did we miss anything?

Likely missing or worth explicitly adding:

### 1. Idempotency

Every job should be safe to rerun for the same date/run.

Need patterns like:

```text
--replace-run
DELETE WHERE run_id = @run_id before write
MERGE by stable keys
insert status rows only once or use run_id
```

### 2. Backfill mode vs daily mode

Daily mode and historical replay should be separate modes.

```text
mode=daily
mode=manual_backfill
mode=replay
mode=preseason_test
```

### 3. Data completeness checks

Before downstream builds:

- schedule exists
- score exists for final games
- boxscore exists for final games
- both teams have metrics
- windowed rows exist before game date
- ranking rows exist before game date

### 4. Failure handling

If Level 3 fails, Level 4 should not run.

If rankings fail, `/game` should still work but expose ranking unavailable reason.

### 5. Alerts / visible failure state

Minimum:

- log event
- write failed stage to `gamelens_pipeline_runs`
- expose last run status in admin page

Later:

- email/slack notification
- Cloud Logging alert

### 6. Preseason caveat

Preseason is perfect for trigger practice, but not perfect for model truth.

Use preseason to test:

- scheduling
- data arrival
- table rebuilds
- admin page refresh
- replay/backfill
- auth and route protection

Do not over-learn football conclusions from preseason unless clearly labeled.

### 7. Admin route registration check

Make sure `admin_claim_health_routes` is registered in the deployed Flask app. If it works in production, the deployed app may already include it, but the repo should make this obvious.

Expected pattern:

```python
from routes.admin_claim_health_routes import admin_claim_health_routes
app.register_blueprint(admin_claim_health_routes)
```

### 8. Payload collection automation

The Level 1 claim builder currently works from saved payload JSON runs. For daily automation, add a job that collects `/game` payloads for newly final games and saves them before Level 1.

### 9. Game-level validation pipeline

Claim validation is not the same as game validation. Keep both:

- Claim validation: did the claim language match postgame evidence?
- Game validation: did the matchup lean/prediction align with final result?

### 10. Documentation ownership

Create this doc in the repo, then update it whenever a pipeline stage is automated.

Suggested repo path:

```text
docs/operations/GameLens_Backend_August_Readiness_Plan.md
```

---

## 11. Suggested August milestones

### Milestone 1 — Documentation and inventory

- Write this doc into `docs/operations/`
- Confirm all scripts and table names
- Document current manual command sequence
- Add known gaps checklist

### Milestone 2 — Metric build automation

- After stats insert, run:
  - facts
  - windowed metrics
  - rankings
- Add row-count checks
- Add pipeline run ledger

### Milestone 3 — Claim/game learning automation

- Automate payload collection for final games
- Run Level 1 → Level 4 from one orchestrator
- Keep dry-run/manual mode available

### Milestone 4 — Admin daily snapshot

- Store daily admin health summary
- Add last-run status to admin page
- Add yesterday-vs-prior comparison

### Milestone 5 — Preseason practice mode

- Run daily in preseason
- Validate that jobs trigger safely
- Label preseason as practice data
- Avoid overfitting to preseason outcomes

---

## 12. Simple priority call

Highest priority before August:

1. Pipeline run ledger
2. Metric build trigger after stats insert
3. Payload collection automation
4. Level 1 → Level 4 orchestrator
5. Admin daily summary table
6. `/game` query simplification/table-function exploration
7. Model feature improvements

Model improvements are fun and valuable, but production reliability is the gatekeeper.

When you start next week, I’d begin with one boring but important question:

“How do I know the daily pipeline ran correctly?”

That points us straight to the first real build item:

Pipeline Run Ledger

Not model improvement. Not new features. Just:

daily_etl_started
daily_etl_finished
stats_inserted
facts_built
windowed_built
rankings_built
level_1_done
level_2_done
level_3_done
level_4_done
admin_summary_done
errors
warnings

