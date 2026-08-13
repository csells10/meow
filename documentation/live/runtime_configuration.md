# GameLens Runtime Configuration Guide

**Created:** 2026-08-04  
**Repository:** csells10/meow  
**Branch:** dev  
**Scope:** Cloud Run runtime configuration for GameLens dev testing  
**Source of truth:** runtime_config.py plus the live Cloud Run service configuration
**Snapshot boundary:** Section 3 records the 2026-08-04 controlled-replay service. It is historical evidence, not the current Packet 2 local-shadow configuration and not permission to reuse those dated values. Re-read live configuration before every cloud action.

---

## 1. Plain-language definitions

### Container

The container is the packaged GameLens application running inside Cloud Run. A Cloud Run revision is a specific version of that container plus its memory, timeout, service account, and environment-variable configuration.

### Environment variable

An environment variable is a named setting supplied to the container when it starts. GameLens reads these settings into the Python RUNTIME_CONFIG object during application startup.

Examples:

~~~text
GAMELENS_ENVIRONMENT=dev
GAMELENS_RUN_MODE=controlled_replay
GAMELENS_ACTIVE_SEASON=2025
~~~

These values are configuration. They are not API keys or passwords.

### Runtime service account

The service account is the Google Cloud identity used by the running container. It determines what the application is allowed to access, including BigQuery datasets, Cloud Storage buckets, and Secret Manager.

Current dev identity:

~~~text
gamelens-dev-replay@nfl-stream-406420.iam.gserviceaccount.com
~~~

The service-account email is an identity, not a secret key.

### Secret

A secret is sensitive data stored separately in Google Secret Manager. The Tank01 RapidAPI key is retrieved by the application under the name Tank_Rapidapi.

The secret value must never be placed in Cloud Run environment variables, terminal output, logs, documentation, GitHub, or chat. Local unit tests mock secret retrieval; the deployed cloud service uses its service account to retrieve the real secret.

---

## 2. Why these settings matter

The runtime configuration decides:

- whether the application is operating as production or dev;
- whether it performs a normal daily run or a controlled replay;
- which NFL season the metric pipeline builds;
- which BigQuery datasets receive reads and writes;
- which Cloud Storage bucket receives raw API-response backups;
- which historical date a controlled replay may process.

These values form one safety system. A correct code revision can still affect the wrong data if its runtime targets are wrong. Always inspect the complete group before invoking a write-capable route such as POST /.

---

## 3. Current dev configuration snapshot

Observed after restoration on 2026-08-04 for ready revision nfl-games-app-dev-00078-ngp:

| Setting | Current value | Purpose |
|---|---|---|
| GAMELENS_ENVIRONMENT | dev | Enables strict dev validation and rejects production data targets. |
| GAMELENS_RUN_MODE | controlled_replay | Skips the Schedule stage and restricts Stats/Scores to the approved replay scope. |
| GAMELENS_ACTIVE_SEASON | 2025 | Supplies the season used by Facts, Windowed Metrics, and Rankings. |
| GAMELENS_PROJECT_ID | nfl-stream-406420 | Selects the Google Cloud project. |
| GAMELENS_LEAGUE_DATASET | League_dev | Dev Schedule, status, and Stats backlog objects. |
| GAMELENS_SCORES_DATASET | Scores_dev | Dev score rows, status, and Scores backlog objects. |
| GAMELENS_ANALYTICS_DATASET | Analytics_dev | Dev raw Stats and derived GameLens metrics. |
| GCS_BUCKET_NAME | nfl-stream-406420-gamelens-dev-raw | Dev-only raw API-response backups. |
| GAMELENS_REPLAY_DATE | 2025-09-18 | The only historical date allowed by the current controlled replay. |
| Runtime service account | gamelens-dev-replay@nfl-stream-406420.iam.gserviceaccount.com | Supplies the deployed app's Google Cloud permissions. |

This is an evidence snapshot, not a permanent assumption. Re-read the live service before every cloud test.

---

## 4. Settings that must be treated as groups

### Isolation group

Inspect these together before every dev request:

~~~text
GAMELENS_ENVIRONMENT
GAMELENS_PROJECT_ID
GAMELENS_LEAGUE_DATASET
GAMELENS_SCORES_DATASET
GAMELENS_ANALYTICS_DATASET
GCS_BUCKET_NAME
runtime service account
~~~

For the approved dev environment, all three datasets must use their _dev names and the bucket must be the dev raw-response bucket. Never change just one dataset and assume the request remains isolated.

### Execution-scope group

Inspect these together:

~~~text
GAMELENS_RUN_MODE
GAMELENS_ACTIVE_SEASON
GAMELENS_REPLAY_DATE
request load_date
~~~

The active season controls metric building. The replay date and request load_date control historical selection during a controlled replay. They must describe the same intended test.

---

## 5. Fail-closed protections in runtime_config.py

When GAMELENS_ENVIRONMENT=dev, application startup requires explicit values for:

- run mode;
- active season;
- all three datasets;
- raw-response bucket.

The application rejects startup when:

- a dev dataset equals a production dataset;
- a dev dataset differs from the approved League_dev, Scores_dev, or Analytics_dev target;
- the dev raw bucket is the production xtra_point bucket;
- the dev bucket name does not contain a recognizable dev segment;
- the season is not a four-digit value beginning with 20;
- the replay date is not a real YYYY-MM-DD date;
- controlled replay is requested in production;
- the run mode is anything other than daily or controlled_replay.

A failed startup is a safety result. Do not weaken these checks to make a revision become ready.

---

## 6. Run-mode behavior

| Environment | Run mode | Schedule behavior | Stats/Scores behavior | Intended use |
|---|---|---|---|---|
| dev | controlled_replay | Intentionally returns schedule_replay_skipped; it does not call the Schedule API. | Reads the dev backlog and restricts selection to the configured replay date/request date. | Reproduce one approved historical game through isolated dev targets. |
| dev | daily | Calls the real Schedule API for yesterday, today, and the next two days. | Checks existing eligible dev backlog; it may process older eligible games even if Schedule finds no games. | Dev-only cloud proof of normal daily behavior. Requires a deliberate configuration change and a separate approval before POST /. |
| production | daily | Normal production Schedule behavior. | Normal production backlog behavior. | Scheduled live operation. |
| production | controlled_replay | Rejected during startup. | Not allowed. | Never use. |

Important: the current controlled_replay configuration protects the historical replay, but it cannot test the new behavior for a real Schedule API response containing body: []. That proof requires a separately approved temporary dev daily revision, followed by restoration to the recorded replay configuration.

An empty Schedule response does not globally disable Stats or Scores. Those stages still inspect their isolated backlog views. If no eligible backlog exists, they exit as no-ops without making their NFL API calls. If an older eligible dev game exists, processing it can be correct and must be anticipated before the request.

---

## 7. Read-only inspection command

Run this before any dev cloud request:

~~~bash
export CLOUDSDK_PYTHON=python

PROJECT_ID="nfl-stream-406420"
REGION="us-central1"
DEV_SERVICE="nfl-games-app-dev"

gcloud run services describe "$DEV_SERVICE" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --format="yaml(
    status.latestCreatedRevisionName,
    status.latestReadyRevisionName,
    status.traffic,
    spec.template.spec.serviceAccountName,
    spec.template.spec.timeoutSeconds,
    spec.template.spec.containers[0].resources,
    spec.template.spec.containers[0].env
  )"
~~~

Required inspection:

1. Latest created revision equals latest ready revision.
2. Dev traffic points to the intended ready revision.
3. Service account is the dedicated dev replay identity.
4. Environment and run mode match the intended test.
5. Project, all datasets, and raw bucket are dev targets.
6. Season and replay/request dates agree.
7. Memory and timeout remain 4Gi and 900s.

This command reads configuration only. It does not build, deploy, or invoke the application.

---

## 8. Rules for changing runtime configuration

A runtime-configuration change creates a new Cloud Run revision. Treat it as a deployment even when the container image is unchanged.

Before changing anything:

1. Record the current ready revision and complete environment-variable set.
2. Record current dev backlog counts for Stats and Scores.
3. State exactly which behavior the new revision is meant to prove.
4. Confirm the targets remain isolated dev resources.
5. Obtain approval for the configuration mutation and the later write-capable request as separate steps.

During a change:

- update related settings together;
- prefer an explicit, reviewable gcloud run services update command;
- use --update-env-vars for intended changes instead of accidentally replacing the full set;
- remove a no-longer-applicable replay date rather than leaving ambiguous stale scope;
- never place secret values on a command line;
- never change the production service as part of a dev test.

After a change:

1. Wait for the new revision to become ready.
2. Re-run the complete read-only inspection.
3. Confirm dev traffic and the service account.
4. Confirm backlog state before calling POST /.
5. Make only the single approved request.
6. Inspect revision-specific HTTP and application logs.
7. Restore the recorded configuration when the temporary test is complete.
8. Verify the restored revision before ending the test.

Do not copy a mutation command from this document and execute it by assumption. Build the exact command from the live configuration and the current test plan, review it, and run it as its own bounded step.

---

## 9. Cloud Build relationship

cloudbuild-dev.yaml deploys a new image to nfl-games-app-dev with 4Gi memory and a 900s timeout. It does not define the GameLens environment variables listed above.

The variables live on the Cloud Run service template and are read by each new revision. Therefore, verify them after every dev deployment rather than assuming the build file defines or repairs them.

A documentation-only commit with [skip ci] is intended to avoid triggering the regional dev Cloud Build trigger. It changes GitHub documentation only; it does not change the live service configuration or create a Cloud Run revision.

---

## 10. Secrets and local tests

Local tests deliberately mock external boundaries such as:

- get_secret("Tank_Rapidapi");
- Tank01 HTTP responses;
- BigQuery clients and writes;
- raw-response uploads.

That keeps tests repeatable without exposing credentials. The real parsing, validation, orchestration, no-op, and failure-handling logic still executes.

The deployed dev cloud test is different:

- it starts with the live environment variables;
- it runs as the configured service account;
- it retrieves the real Tank01 secret from Secret Manager;
- it can read and write the configured dev datasets and bucket;
- POST / is therefore a real write-capable operation even when a no-op is expected.

Never interpret dev or test as cannot write.

---

## 11. Gate H dev remediation evidence — 2026-08-04

The Schedule no-game correction and combined branch were proven through the real dev cloud path before any transfer to `main`.

~~~text
Combined dev commit: 2d72dbd1c762b6ebae238ab40615a82142a8db47
No-game correction commit: aabdd8f
Focused regression tests: 35 passed
Temporary daily revision: nfl-games-app-dev-00077-c8j
Temporary run mode: daily
Temporary active season: 2026
Temporary replay date: unset
Dev datasets and raw bucket: isolated dev targets
~~~

The successful daily request produced:

~~~text
HTTP status: 200
Schedule status: success
Dates checked: 4
Dates with games: 1
Dates with no games: 3
Schedule failures: 0
Inserted game: 20260806_CAR@ARI
Stats: no_op — no eligible games
Scores: no_op — no eligible games
Metric pipeline: skipped — no accepted stats games
Overall application status: no_op
~~~

The overall `no_op` is expected: Schedule completed successfully, but the future game was not eligible for Stats or Scores.

An unusually close manual rerun temporarily returned HTTP 500 because the newly streamed August 6 Schedule row was still in BigQuery's streaming buffer. The row remained present exactly once with `boxscore_loaded=false` and `score_loaded=false`. After the buffer cleared, one approved rerun returned the successful HTTP 200 result above. No code or data repair was required.

Dev was then restored:

~~~text
Restored revision: nfl-games-app-dev-00078-ngp
Image commit: 2d72dbd1c762b6ebae238ab40615a82142a8db47
Traffic: 100%
Run mode: controlled_replay
Active season: 2025
Replay date: 2025-09-18
~~~

This completes the dev proof of the Gate H remediation. It does not complete Gate H in production.

The next authorization boundary is documentation review followed by a fresh Git and production-state inspection. Only then may the proven history move to `main`, create a new no-traffic production candidate, and proceed through candidate validation and deliberate promotion.
