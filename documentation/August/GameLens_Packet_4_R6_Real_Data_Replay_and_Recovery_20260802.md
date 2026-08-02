# GameLens Packet 4 — R6 Real-Data Replay and Recovery

**Recorded:** 2026-08-02  
**Repository/branch:** `csells10/meow` / `dev`  
**Environment:** isolated controlled replay  
**Replay ID:** R6  
**Target game:** `20250918_MIA@BUF`  
**Replay date:** `2025-09-18`  
**Result:** Real API ingestion and the complete metric chain were proven in dev; production resource configuration remains a merge/deployment prerequisite.

---

## 1. What R6 proved

R6 was a real end-to-end execution, not a mocked unit test:

```text
Live Stats and Scores APIs
→ isolated dev source tables
→ Facts
→ Windowed Metrics
→ Rankings
→ completion/status records
→ empty Stats and Scores backlogs
```

Observed final outputs:

| Stage | Result |
|---|---:|
| Stats rows for the replay game | 132 |
| Score rows for the replay game | 2 |
| Facts rows | 36,538 |
| Windowed Metric rows | 138,532 |
| Ranking rows | 426,965 |
| Stats backlog rows for the game | 0 |
| Scores backlog rows for the game | 0 |

Production datasets were not targeted or changed.

This is strong evidence that the Packet 1 source safeguards, Packet 2 conductor, and Packet 4 `app.py` activation seam work with real external responses and real BigQuery writes.

---

## 2. Isolation configuration used

The successful dev recovery revision used:

```text
Cloud Run service: nfl-games-app-dev
Recovery revision: nfl-games-app-dev-00067-w8r
Service account: gamelens-dev-replay@nfl-stream-406420.iam.gserviceaccount.com
GAMELENS_ENVIRONMENT=dev
GAMELENS_RUN_MODE=controlled_replay
GAMELENS_ACTIVE_SEASON=2025
GAMELENS_PROJECT_ID=nfl-stream-406420
GAMELENS_LEAGUE_DATASET=League_dev
GAMELENS_SCORES_DATASET=Scores_dev
GAMELENS_ANALYTICS_DATASET=Analytics_dev
GCS_BUCKET_NAME=nfl-stream-406420-gamelens-dev-raw
GAMELENS_REPLAY_DATE=2025-09-18
Web-service memory: 1Gi
Web-service CPU: 1000m
Web-service timeout: 300 seconds
```

All nine environment pins and the dedicated service account were verified after the service update. The health endpoint returned:

```json
{"status":"ok"}
```

Never run a historical replay unless the environment, service account, dataset destinations, season, replay date, image, and target game have first been printed and inspected.

---

## 3. Initial R6 execution and failure boundary

The targeted `POST /` reached revision `nfl-games-app-dev-00066-hw9` and returned:

```text
HTTP status: 503
Latency: 52.530830863 seconds
Trace: projects/nfl-stream-406420/traces/59296b55589b0fa5714e25001b20308d
```

The request was not a total failure. Before the container stopped:

1. Schedule replay correctly skipped.
2. Stats called the live API and wrote 132 accepted rows.
3. Scores called the live API and wrote 2 accepted rows.
4. Facts completed and wrote 36,538 rows.
5. Windowed Metrics built and committed 138,532 rows.
6. Rankings had not started.

The decisive Cloud Run log line was:

```text
Memory limit of 512 MiB exceeded with 519 MiB used.
```

The container was killed while beginning the Windowed-table write/transition. BigQuery completed the Windowed write independently before the process died.

Post-crash table metadata established the exact commit boundary:

| Table | Rows | Last modified UTC |
|---|---:|---|
| `game_team_metric_facts_2025` | 36,538 | 2026-08-02 00:03:10 |
| `team_metrics_windowed_2025` | 138,532 | 2026-08-02 00:03:34 |
| `team_metric_rankings_2025` | 426,086 | 2026-08-01 19:09:35 |

Because Facts and Windowed had committed, the correct response was to stop and recover only the missing Rankings stage. Reposting the whole request would have repeated already completed work and made the failure boundary harder to reason about.

---

## 4. Recovery procedure used

### Step 1 — Do not rerun the original request

After any `5xx`, timeout, disconnect, or container restart:

1. Do not assume nothing happened.
2. Do not immediately retry.
3. Read the exact request trace and application/system logs.
4. Inspect status tables, backlogs, destination row counts, and table modification timestamps.
5. Identify the first stage that did not commit.

A failed HTTP response can still contain successful external API calls and committed BigQuery writes.

### Step 2 — Give the dev web service safe headroom

The isolated dev service was increased from `512Mi` to `1Gi`:

```bash
gcloud run services update "nfl-games-app-dev" \
  --project="nfl-stream-406420" \
  --region="us-central1" \
  --memory="1Gi"
```

This created revision `nfl-games-app-dev-00067-w8r`. The update itself did not run the pipeline or change BigQuery data.

The service revision, traffic, service account, timeout, resources, all environment variables, and `/health` were reverified before recovery continued.

### Step 3 — Create a one-stage Rankings recovery job

The conductor always starts at Facts, so it was intentionally not used for recovery. Rankings already has a direct CLI entry point.

A one-time Cloud Run job was created with:

```text
Job: gamelens-r6-rankings-recovery
Image: us-central1-docker.pkg.dev/nfl-stream-406420/meow-main/dev:ce8dcfa6fa5769d1dc1b5b5932c4940905a37705
Command: python
Arguments: -m agg.build_metric_rankings --season 2025 --if-exists replace
Service account: gamelens-dev-replay@nfl-stream-406420.iam.gserviceaccount.com
CPU: 1
Memory: 4Gi
Tasks: 1
Retries: 0
Timeout: 30 minutes
Destinations: League_dev, Scores_dev, Analytics_dev
```

Creating the job did not execute it. Its complete configuration was printed and inspected before the one permitted execution.

### Step 4 — Execute exactly once

Execution:

```text
gamelens-r6-rankings-recovery-6nsns
```

Result:

```text
Succeeded tasks: 1
Failed tasks: 0
Start: 2026-08-02T00:26:16.806639Z
Completion: 2026-08-02T00:29:46.158123Z
Duration reported by Cloud Run: 3m29.35s
```

The job must not be executed again for R6.

### Step 5 — Validate the final state

Final table metadata:

| Table | Rows | Last modified UTC |
|---|---:|---|
| `game_team_metric_facts_2025` | 36,538 | 2026-08-02 00:03:10 |
| `team_metrics_windowed_2025` | 138,532 | 2026-08-02 00:03:34 |
| `team_metric_rankings_2025` | 426,965 | 2026-08-02 00:29:41 |

Rankings increased from 426,086 to 426,965 rows: **+879**.

---

## 5. Schedule flags versus authoritative completion state

The replay game's schedule row remained:

```text
boxscore_loaded=false
score_loaded=false
```

No manual repair was performed.

Current `dev` code inspection found no ownership/update of those two schedule columns in `app.py`, `api_call_nfl_stats.py`, `api_call_nfl_scores.py`, or `api_call_nfl_games.py`.

The backlog views instead use the status tables as authoritative completion records:

```text
League_dev.boxscore_status.boxscore_loaded = TRUE
→ removes the game from League_dev.games_to_process

Scores_dev.score_status.score_loaded = TRUE
→ removes the game from Scores_dev.scores_to_process
```

Both status records existed and both backlogs returned zero rows. Therefore the false schedule columns were stale/redundant bookkeeping, not unfinished ingestion. Changing them manually would have hidden the inconsistency without correcting its ownership.

Non-blocking technical debt:

- consolidate the duplicate completion signals, or explicitly synchronize schedule flags;
- make the Scores backlog's `score_loaded IS FALSE` condition null-safe if that schedule column remains part of eligibility.

---

## 6. Expected warnings that were not failures

The following observations did not cause R6 to fail:

- BigQuery Storage API warnings;
- eight excluded/unregistered metrics;
- 220 duplicate Facts rows removed with `keep="last"`.

The fatal condition was specifically the Cloud Run memory-limit event.

---

## 7. Production and merge implications

R6 supports high confidence in the data-flow logic, but it does not prove that the entire metric chain can finish inside one uninterrupted `1Gi` web request.

What is proven:

- `512Mi` is insufficient; the process reached 519 MiB and was killed.
- Live Stats and Scores ingestion works.
- Facts, Windowed Metrics, and Rankings each produce valid dev outputs.
- The direct Rankings recovery entry point works with `4Gi`.
- Retry/status tables prevent this replay game from re-entering the source backlogs.
- Dev isolation kept production untouched.

What remains before Packet 4 can be called fully production-ready:

1. Persist an explicit production memory choice in deployment configuration so a deployment cannot silently revert to `512Mi`.
2. Decide whether Rankings remains inside the web-request conductor or runs as a dedicated Cloud Run job.
3. If keeping the complete chain inside the web request, perform one uninterrupted controlled run at the chosen production-equivalent memory.
4. Align the Cloud Run request timeout and Scheduler attempt deadline as already required by Packet 4.
5. Preserve zero automatic retries for the first controlled production activation.
6. Verify the deployed revision, environment, service account, route health, and returned stage summary before relying on the daily schedule.

Recommended interpretation:

> The engine and data flow work. Packet 4 uncovered a real deployment-capacity requirement before merge to `main`.

Do not merge to `main` on the assumption that the manual dev `1Gi` change is durable. The checked-in deployment configuration must carry the final memory setting.

---

## 8. Reusable partial-success investigation checklist

Use this sequence for future pipeline failures:

```text
HTTP failure or lost response
→ stop; do not resend
→ capture request revision, timestamp, trace, status, and latency
→ read application and system logs for that exact trace/window
→ inspect source status records and processing backlogs
→ inspect destination row counts and modification timestamps
→ identify the last committed stage
→ choose the narrowest available stage-specific recovery
→ print and inspect recovery configuration
→ execute once with retries disabled
→ validate final row counts/timestamps/backlogs
→ document the incident and update the packet status
```

Do not infer transactionality across external APIs, BigQuery writes, and an HTTP response. Each stage may commit independently.

---

## 9. R6 closure record

```text
Packet: Packet 4 — Activate through app.py
Checkpoint: R6 real-data controlled replay
Status: PASS with production resource follow-up
Target: 20250918_MIA@BUF
Real APIs called: yes
Stats rows: 132
Score rows: 2
Facts rows: 36,538
Windowed rows: 138,532
Rankings rows: 426,965
Stats backlog: 0
Scores backlog: 0
Initial failure: 512Mi Cloud Run memory limit exceeded at 519Mi
Recovery: Rankings-only Cloud Run job, 4Gi, zero retries
Recovery execution: gamelens-r6-rankings-recovery-6nsns
Production touched: no
Do not rerun: original POST or R6 recovery job
Remaining Packet 4 gate: durable production memory/execution design and activation configuration
```
