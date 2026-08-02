# GameLens Controlled Production Cutover Plan

**Created:** 2026-08-02
**Repository:** `csells10/meow`
**Working branch:** `dev`
**Target service:** `nfl-games-app-main`
**Status:** Planned — no candidate deployment or traffic change has occurred
**Purpose:** Move Packet 4 from proven isolated-dev behavior to production through small, reversible gates while keeping the current frontend live until the candidate is explicitly approved.

---

## 1. Plain-language release model

This plan adds one controlled step between “merge to main” and “send the new version to everyone.”

```text
Current production revision keeps 100% of normal traffic
→ build a new production candidate with 0% normal traffic
→ test the candidate through its private-by-convention tagged URL
→ either promote it deliberately or leave the current revision untouched
```

The candidate is a real revision of the production Cloud Run service. It uses production identity, production configuration defaults, and production data access. The protection is **traffic isolation**, not data isolation.

Therefore:

- normal `gamelens.io` traffic continues reaching the current production revision;
- Cloud Scheduler continues reaching the current production revision until traffic is promoted;
- only deliberate requests to the tagged candidate URL reach the new revision;
- candidate tests must remain read-only at the application/data level;
- `POST /` and final-game `/game/{gameID}` requests are forbidden during candidate testing.

---

## 2. Why this release path exists

The July deployment incident showed that a healthy Cloud Run deployment is not enough proof that the browser-facing application is safe. The frontend can still fail if routes, authentication, CORS, or response contracts change unexpectedly.

The no-traffic candidate converts one large risk into separate release gates:

1. Can the container build and start?
2. Does the revision have the intended memory, timeout, identity, and production defaults?
3. Do health and CORS behave correctly?
4. Does Firebase authentication work?
5. Do `/games` and a non-final `/game/{gameID}` return the expected contracts?
6. Can an actual frontend preview render the candidate response?
7. Only after those answers are yes: should normal traffic move?

At every gate, stop if the evidence is unclear. A failed candidate does not require a production rollback because the current revision still owns normal traffic.

---

## 3. Proven evidence before this plan

### Packet 4 R6 controlled replay

R6 exercised the real ingestion and metric chain against isolated dev destinations for `20250918_MIA@BUF`:

```text
Live Stats API: 132 accepted rows
Live Scores API: 2 accepted rows
Facts: 36,538 rows
Windowed Metrics: 138,532 rows
Rankings: 426,965 rows
Stats backlog: 0
Scores backlog: 0
Production impact: none
```

R6 also proved that the old `512Mi` Cloud Run setting is insufficient. The initial request exceeded the limit after Facts and Windowed committed. Rankings was recovered once in a `4Gi`, zero-retry Cloud Run job. The original R6 POST and recovery execution must not be rerun.

### Durable runtime settings already completed on `dev`

Both deployment files now explicitly request:

```yaml
- '--memory'
- '4Gi'
- '--timeout'
- '900s'
```

Recorded commits:

```text
4913911 Persist Cloud Run resources for main
d77fcaf Persist Cloud Run resources for dev
```

The production Scheduler deadline has separately been changed from `180s` to `900s`. Its schedule, timezone, target, and enabled state were unchanged.

### `/game` browser-contract gate already completed locally

The current `dev` application proved:

```text
/game/<path:game_id> registered
/games registered
/me registered
/admin/gamelens/claim-health registered
/health registered

OPTIONS /game/contract-check: 200
OPTIONS /me: 200
Allowed origin: https://gamelens.io
Allowed authorization header: Authorization
Unauthenticated GET /game/contract-check: intentional 401 JSON
CORS header present on the 401 response
```

This directly checks the failure class that harmed the frontend in July. It does not replace deployed candidate testing.

### Current production baseline to preserve

At the time this plan was written:

```text
Serving revision: nfl-games-app-main-00136-vsx
Serving traffic: 100%
Serving memory: 512Mi
Serving timeout: 300s
Runtime service account: 362530996210-compute@developer.gserviceaccount.com

Retired revision: nfl-games-app-main-00143-twq
Retired revision traffic: 0%

Main trigger branch: ^main$
Main trigger build file: cloudbuild.yaml
Scheduler job: Get-NFL-Schedule
Scheduler deadline: 900s
Scheduler schedule: 8:00 a.m. America/New_York
```

The current serving revision is the runtime rollback anchor. Reconfirm its name immediately before deployment because this document is a snapshot, not a substitute for live inspection.

---

## 4. Non-negotiable safety rules

1. Work from a clean, synchronized `dev` branch.
2. Keep the dev Cloud Build trigger disabled.
3. Keep the production trigger restricted to `^main$`.
4. Keep Scheduler automatic retries disabled during the first production activation.
5. Do not change metric formulas, windows, rankings, `lens_tags`, `/game` logic, Firebase authorization, CORS policy, or Levels 1–4 as part of this release mechanism.
6. Do not use a 1% traffic split. Low traffic makes it unpredictable, and Scheduler could become the request sent to the candidate.
7. Do not call the candidate root `POST /`; it can run production ingestion and downstream builders.
8. Do not call a final game through candidate `/game/{gameID}`; the current service can write model outcome and trust rows for final games.
9. Use an upcoming/non-final game for `/game` validation.
10. Never paste a Firebase token into GitHub, documentation, chat, or a committed shell script.
11. Do not advance to the next gate because a command merely returned exit code zero. Check the expected evidence listed for that gate.
12. If production traffic or data changes unexpectedly, stop immediately and use the rollback section.

---

## 5. Release state ladder

| Gate | State | Production user traffic | Production data writes | Exit requirement |
|---|---|---:|---:|---|
| A | Documentation and design | Current revision 100% | None | Plan reviewed |
| B | No-traffic YAML prepared on `dev` | Current revision 100% | None | Diff and tests pass |
| C | Candidate deployed from `main` | Current revision 100% | None from deployment | Candidate healthy and 0% normal traffic |
| D | Candidate API tests | Current revision 100% | None allowed | Health, CORS, auth, `/games`, and non-final `/game` pass |
| E | Candidate UI preview | Current revision 100% | None allowed | Real game page renders without browser errors |
| F | Promotion window | Candidate 100% after explicit command | Avoid ingestion during switch | Live smoke tests pass |
| G | Controlled daily activation | Candidate 100% | Expected production ETL writes | First scheduled run and `/game` follow-up pass |
| H | Cleanup | Candidate remains 100% | Normal daily behavior | Temporary deployment flags removed or intentionally retained |

No gate is implied. Record the evidence and make an explicit go/no-go decision before moving forward.

---

## 6. Gate A — commit this plan only

### Change

Create:

```text
documentation/live/go_plan.md
```

### Expected effect

```text
Code behavior: unchanged
Cloud Build configuration: unchanged
Cloud Run revisions: unchanged
Cloud Scheduler: unchanged
Production data: unchanged
Frontend traffic: unchanged
```

### Suggested commit

```text
Document controlled production cutover plan [skip ci]
```

### Stop point

Stop after the documentation commit. Review this runbook before editing deployment configuration.

---

## 7. Gate B — prepare a no-traffic main deployment

This gate is a future code/configuration change. It is **not** performed by creating this plan.

### Exact file scope

Modify only:

```text
cloudbuild.yaml
```

Do not add the no-traffic/tag arguments to `cloudbuild-dev.yaml`. The tagged candidate mechanism is specifically for the main production service.

### Intended arguments

Add these arguments to the existing `gcloud run deploy nfl-games-app-main` step after the durable memory/timeout arguments:

```yaml
      - '--no-traffic'
      - '--tag'
      - 'packet4-candidate'
```

The resulting deployment must still include:

```yaml
      - '--memory'
      - '4Gi'
      - '--timeout'
      - '900s'
      - '--no-traffic'
      - '--tag'
      - 'packet4-candidate'
```

### Why both flags are needed

- `--no-traffic` prevents the new revision from automatically receiving the service's normal URL traffic.
- `--tag packet4-candidate` assigns a stable targeted URL so deliberate test requests can reach that revision.

### Local/read-only checks before committing

```bash
git switch dev
git fetch origin
git status --short --branch
git rev-list --left-right --count origin/dev...HEAD
git diff -- cloudbuild.yaml
git diff --check
```

Proceed only if the working tree is understood and the diff is limited to the candidate arguments.

### Regression checks before committing

Run the focused application and conductor tests already used for Packet 4:

```bash
python -m unittest tests.test_app -v
python -m unittest discover -s tests/services -p "test_gamelens_metric_pipeline_conductor.py" -v
```

Also rerun the local browser-contract check recorded in the August handoff if any application file has changed since its last pass.

### Pre-commit stop conditions

Stop if any of the following is true:

- `cloudbuild.yaml` targets a service other than `nfl-games-app-main`;
- the main trigger no longer matches only `^main$`;
- `4Gi` or `900s` is missing;
- an application file is unexpectedly included in the diff;
- unit tests fail;
- `git diff --check` reports errors.

### Suggested commit

```text
Deploy main as a no-traffic Packet 4 candidate
```

Committing this change on `dev` must not deploy anything. Confirm the external dev trigger remains disabled and the production trigger remains main-only before pushing.

---

## 8. Gate C — final pre-merge inspection

This gate is read-only.

### Git checks

```bash
git switch dev
git status --short --branch
git fetch origin

echo "=== MAIN VS DEV DIVERGENCE ==="
git rev-list --left-right --count origin/main...origin/dev

echo
echo "=== DIFF SAFETY CHECK ==="
git diff --check origin/main...origin/dev

echo
echo "=== DEPLOYMENT DIFF ==="
git diff origin/main...origin/dev -- cloudbuild.yaml cloudbuild-dev.yaml
```

Expected branch relationship before the controlled merge is that `dev` is not behind `main`. If main has moved, stop and reconcile deliberately rather than assuming a fast-forward remains safe.

### Live infrastructure checks

```bash
export CLOUDSDK_PYTHON=python

PROJECT_ID="nfl-stream-406420"
REGION="us-central1"
MAIN_SERVICE="nfl-games-app-main"
MAIN_TRIGGER="git-commit-nfl-games-app-us-central1-csells10-meow"
SCHEDULER_JOB="Get-NFL-Schedule"

gcloud run services describe "$MAIN_SERVICE" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --format="yaml(status.latestReadyRevisionName,status.traffic,spec.template.spec.serviceAccountName,spec.template.spec.timeoutSeconds,spec.template.spec.containers[0].resources.limits,spec.template.spec.containers[0].env)"

gcloud builds triggers list \
  --project="$PROJECT_ID" \
  --filter="name=$MAIN_TRIGGER" \
  --format="yaml(name,disabled,filename,github.push.branch,serviceAccount)"

gcloud scheduler jobs describe "$SCHEDULER_JOB" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --format="yaml(state,schedule,timeZone,attemptDeadline,retryConfig,httpTarget.uri,httpTarget.httpMethod)"
```

### Record before merging

```text
Current serving revision:
Current serving percentage:
Main runtime service account:
Main trigger branch expression:
Main trigger build file:
Scheduler state:
Scheduler deadline:
Scheduler retry setting:
Dev commit to be merged:
Local tests and result:
```

### Merge authorization boundary

Merging `dev` into `main` is the action that activates the enabled production build trigger. Do not merge as part of a read-only inspection. Treat the merge as a separate, explicit go decision.

---

## 9. Gate D — deploy the candidate with 0% normal traffic

### What the merge should cause

```text
main receives the approved dev commit
→ enabled main Cloud Build trigger starts
→ image builds and is pushed
→ Cloud Run creates a new nfl-games-app-main revision
→ new revision receives 4Gi memory and 900s timeout
→ packet4-candidate tag points to the new revision
→ normal service traffic remains on the previous serving revision
```

### Observe the build

Do not proceed while the build is still running. Confirm the build succeeds and note its commit SHA and build ID.

### Verify the new revision and traffic

```bash
export CLOUDSDK_PYTHON=python

PROJECT_ID="nfl-stream-406420"
REGION="us-central1"
MAIN_SERVICE="nfl-games-app-main"

gcloud run services describe "$MAIN_SERVICE" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --format="yaml(status.latestCreatedRevisionName,status.latestReadyRevisionName,status.traffic,status.url)"
```

Record:

```text
Candidate revision:
Candidate tagged URL:
Candidate normal traffic percentage: expected 0%
Previous serving revision:
Previous serving traffic percentage: expected 100%
```

Then inspect the candidate revision directly:

```bash
CANDIDATE_REVISION="REPLACE_WITH_VERIFIED_REVISION"

gcloud run revisions describe "$CANDIDATE_REVISION" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --format="yaml(metadata.name,metadata.creationTimestamp,spec.serviceAccountName,spec.timeoutSeconds,spec.containerConcurrency,spec.containers[0].image,spec.containers[0].resources.limits,spec.containers[0].env,status.conditions)"
```

### Required candidate configuration

```text
Ready condition: True
Memory: 4Gi
Timeout: 900 seconds
Runtime service account: 362530996210-compute@developer.gserviceaccount.com
Container image: built from the intended main commit
GAMELENS_ENVIRONMENT override: absent
GAMELENS_RUN_MODE override: absent
GAMELENS_*_DATASET dev overrides: absent
GCS_BUCKET_NAME dev override: absent
Normal traffic: 0%
Tagged URL: present
```

The absence of `GAMELENS_*` overrides on main is expected under the current design. `runtime_config.py` then chooses the built-in production defaults:

```text
environment: production
run mode: daily
active season: 2026
league dataset: League
scores dataset: Scores
analytics dataset: Analytics
raw bucket: xtra_point
```

### Immediate stop conditions

Stop without moving traffic if:

- the candidate is not Ready;
- the old revision no longer has 100% normal traffic;
- memory is not `4Gi`;
- timeout is not `900` seconds;
- the runtime identity changed unexpectedly;
- any dev dataset, dev bucket, `controlled_replay`, or replay-date variable appears;
- the image does not correspond to the intended main commit.

At this point, a failed candidate is not a live frontend outage. Leave the old revision serving, inspect logs, fix on `dev`, and create a new candidate.

---

## 10. Gate E — candidate API tests

Set the tagged URL only after copying it from the live Cloud Run service description:

```bash
CANDIDATE_URL="https://REPLACE_WITH_VERIFIED_TAGGED_URL"
```

### Test 1 — health

```bash
curl -i "$CANDIDATE_URL/health"
```

Expected:

- HTTP success;
- JSON health response;
- no startup/import exception in Cloud Run logs.

### Test 2 — `/game` CORS preflight

```bash
curl -i -X OPTIONS "$CANDIDATE_URL/game/contract-check" \
  -H "Origin: https://gamelens.io" \
  -H "Access-Control-Request-Method: GET" \
  -H "Access-Control-Request-Headers: Authorization"
```

Expected:

```text
HTTP 200
Access-Control-Allow-Origin: https://gamelens.io
Access-Control-Allow-Methods includes GET
Access-Control-Allow-Headers includes Authorization
```

### Test 3 — `/me` CORS preflight

```bash
curl -i -X OPTIONS "$CANDIDATE_URL/me" \
  -H "Origin: https://gamelens.io" \
  -H "Access-Control-Request-Method: GET" \
  -H "Access-Control-Request-Headers: Authorization"
```

Expected: the same CORS contract as `/game`.

### Test 4 — unauthenticated protected-route behavior

```bash
curl -i "$CANDIDATE_URL/game/contract-check" \
  -H "Origin: https://gamelens.io"
```

Expected:

```text
HTTP 401
JSON error body
Access-Control-Allow-Origin: https://gamelens.io
```

### Firebase token handling for authenticated tests

Use a fresh token from the existing logged-in frontend session. Do not save it in a file or paste it into chat. Enter it silently:

```bash
read -s FIREBASE_ID_TOKEN
export FIREBASE_ID_TOKEN
```

Clear it when testing is finished:

```bash
unset FIREBASE_ID_TOKEN
```

### Test 5 — authenticated `/me`

```bash
curl -i "$CANDIDATE_URL/me" \
  -H "Origin: https://gamelens.io" \
  -H "Authorization: Bearer $FIREBASE_ID_TOKEN"
```

Expected: HTTP 200 and the established user response contract.

### Test 6 — authenticated `/games`

Use a verified scheduled date. The known Packet 3 example is `2026-08-06`:

```bash
curl -i "$CANDIDATE_URL/games?date=2026-08-06" \
  -H "Origin: https://gamelens.io" \
  -H "Authorization: Bearer $FIREBASE_ID_TOKEN"
```

Expected:

- HTTP 200;
- response contains `date` and `games`;
- the expected scheduled game is present;
- no CORS or authorization error.

### Test 7 — authenticated non-final `/game`

Use only a game confirmed not final. The Packet 3 example was `20260806_CAR@ARI`, but recheck its live status before using it because this plan may be executed later.

```bash
SAFE_GAME_ID="REPLACE_WITH_VERIFIED_NON_FINAL_GAME_ID"

curl -i "$CANDIDATE_URL/game/$SAFE_GAME_ID" \
  -H "Origin: https://gamelens.io" \
  -H "Authorization: Bearer $FIREBASE_ID_TOKEN"
```

Required top-level response fields:

```text
header
final_score
game_profile
matchup_lean
model_outcome
model_trust
team_comparison
core_area_comparison
ranking_context
matchup_breakdown
```

Early-season empty metrics or `ranking_context.available: false` can be legitimate. A safe degraded response is not a frontend contract failure. The candidate fails this gate if it returns an exception, malformed JSON, missing established top-level fields, CORS failure, auth failure for an allowed user, or non-array `lens_tags` where tags are present.

### Forbidden candidate tests

Do not run:

```text
POST /
/test by any method
GET /game/{final_game_id}
controlled replay
manual Rankings recovery
```

### Log review

Review candidate-revision logs after the requests. Filter specifically to the candidate revision so errors from the old serving revision are not confused with candidate behavior.

Stop if any traceback, permission error, dataset-routing error, Firebase verification error, or unexpected write appears.

---

## 11. Gate F — frontend preview against the candidate

API `curl` checks are necessary but do not prove that the actual frontend renders correctly.

Create or use a temporary frontend preview whose API base URL points to the verified tagged candidate URL. Do not change the live `gamelens.io` API target yet.

### Browser test checklist

- Sign in through the normal Firebase flow.
- Confirm `/me` succeeds in the browser network panel.
- Open the game list for a known scheduled date.
- Open one verified non-final game.
- Confirm the page renders without a blank screen.
- Confirm no CORS errors appear in the console.
- Confirm no 401/403 occurs for the allowed user.
- Confirm no repeated request loop appears.
- Confirm degraded early-season sections render as unavailable/empty rather than crashing.
- Inspect the `/game` response and verify `lens_tags`, when present, are arrays.
- Confirm the live production frontend still reaches the old normal URL and remains usable throughout the preview.

If a temporary preview origin is not already allowed by CORS, do not casually broaden production CORS. Either test from the existing allowed `https://gamelens.io` origin using a deliberate browser-console request or create a separately reviewed narrow-origin change.

### Go/no-go record

```text
Candidate revision:
Candidate URL:
/health:
CORS /game:
CORS /me:
Unauthenticated 401 contract:
Authenticated /me:
Authenticated /games:
Authenticated non-final /game:
Frontend preview:
Candidate log review:
Unexpected writes:
Decision: GO / NO-GO
Reviewer/date:
```

Do not promote without an explicit `GO`.

---

## 12. Gate G — controlled traffic promotion

### Choose a quiet window

Promote soon after a normal scheduled run and with enough time before the next 8:00 a.m. Eastern Scheduler execution to complete live smoke testing. Do not promote while the current ETL request is running.

For maximum control, pause Scheduler immediately before promotion and resume it only after the live smoke test passes:

```bash
gcloud scheduler jobs pause Get-NFL-Schedule \
  --project="nfl-stream-406420" \
  --location="us-central1"
```

Verify `state: PAUSED`. Pausing changes scheduling only; it does not execute the job. Keep the pause short so a daily ingestion window is not accidentally skipped.

### Promote without rebuilding

```bash
gcloud run services update-traffic nfl-games-app-main \
  --project="nfl-stream-406420" \
  --region="us-central1" \
  --to-tags="packet4-candidate=100"
```

### Immediately verify traffic

```bash
gcloud run services describe nfl-games-app-main \
  --project="nfl-stream-406420" \
  --region="us-central1" \
  --format="yaml(status.latestReadyRevisionName,status.traffic,status.url)"
```

Expected: the candidate revision receives 100% of the normal service traffic.

### Live smoke test

Using the ordinary production URL and real frontend:

1. Check `/health`.
2. Sign in.
3. Confirm `/me`.
4. Load `/games` for a known date.
5. Open one verified non-final game.
6. Inspect browser console/network errors.
7. Confirm Cloud Run logs remain clean.

Do not invoke the ingestion root as a smoke test.

### Resume Scheduler after the live gate passes

```bash
gcloud scheduler jobs resume Get-NFL-Schedule \
  --project="nfl-stream-406420" \
  --location="us-central1"
```

Verify:

```text
state: ENABLED
attemptDeadline: 900s
schedule: 00 8 * * *
timeZone: America/New_York
automatic retries: still disabled
```

If the live smoke test fails, do not resume Scheduler on the candidate. Roll traffic back first.

---

## 13. Rollback plan

### Runtime/frontend rollback anchor

Immediately before promotion, save the actual 100%-serving revision name:

```bash
ROLLBACK_REVISION="REPLACE_WITH_PRE_PROMOTION_100_PERCENT_REVISION"
```

At the time this plan was created, the known baseline was:

```text
nfl-games-app-main-00136-vsx
```

Do not blindly use that snapshot later if the live baseline has changed.

### Roll back traffic

```bash
gcloud run services update-traffic nfl-games-app-main \
  --project="nfl-stream-406420" \
  --region="us-central1" \
  --to-revisions="$ROLLBACK_REVISION=100"
```

Then verify the service traffic map and rerun `/health`, `/me`, `/games`, and one non-final `/game` against the normal URL.

### What rollback does and does not undo

Traffic rollback restores the previous application revision. It does not reverse BigQuery or Cloud Storage writes already made by a production ingestion run.

This is why candidate tests prohibit ingestion and final-game `/game` calls, and why Scheduler should be paused during the short promotion smoke-test window. If a scheduled ingestion has already written data, preserve logs and table evidence before attempting any data repair. Do not delete or replace production tables reflexively.

### Rollback triggers

Rollback immediately for:

- frontend blank screen or widespread route failure;
- CORS regression;
- valid-user authentication failure;
- `/games` or `/game` 500 responses;
- malformed established response fields;
- wrong dataset/bucket routing;
- unexpected production writes during a supposedly read-only test;
- repeated OOM, timeout, or container crash;
- a critical error whose scope is unclear.

---

## 14. Gate H — first scheduled production run

After the candidate is promoted, live smoke-tested, and Scheduler is enabled, observe the next scheduled run as a controlled production activation.

### Do not assume success from HTTP status alone

Record the returned/logged stage summary:

```text
season
schedule status/count
stats accepted/rejected/skipped counts
scores accepted/rejected/skipped counts
facts status/count
windowed status/count
rankings status/count
failed stage
warnings
request duration
peak memory if available
```

### Acceptable outcomes

```text
No eligible/accepted Stats games
→ successful no-op for metric stages

Accepted Stats games
→ Facts → Windowed Metrics → Rankings complete in order

Partial or failed source/stage
→ failure remains visible and downstream stages do not falsely report success
```

### Post-run `/game` checks

- Confirm `/games` still returns the expected schedule.
- Confirm one upcoming/non-final `/game` remains usable.
- After the first completed 2026 games produce accepted Stats, verify populated 2026 metric rows and `lens_tags` arrays.
- Confirm early-season ranking absence is treated as legitimate only when supported by the underlying data state.
- Confirm both Stats and Scores backlog views reflect the expected completion state.
- Do not manually flip stale schedule booleans; current status tables are authoritative for the backlog views.

### Scheduler retry rule

Keep automatic retries disabled for the first controlled production runs. Revisit retries only after idempotency, partial-failure behavior, and runtime duration are proven in production.

---

## 15. Temporary candidate-flag cleanup

The `--no-traffic` and `--tag packet4-candidate` arguments are release-control settings, not normal automatic-deployment behavior.

If they remain in `cloudbuild.yaml`, every future main deployment will create a new zero-normal-traffic candidate and require an explicit promotion. That may be desirable, but it changes the permanent release process.

After Packet 4 stabilizes, make an explicit decision:

### Option A — keep controlled candidates permanently

Use when every main deployment should require manual candidate testing and traffic promotion.

Requirements:

- document the release owner;
- document required candidate tests;
- document who promotes traffic;
- prevent forgotten zero-traffic revisions from being mistaken for live releases.

### Option B — remove the temporary flags

Use when the no-traffic candidate was a one-time Packet 4 safety mechanism and future approved main deployments should resume normal traffic behavior.

Remove only:

```yaml
- '--no-traffic'
- '--tag'
- 'packet4-candidate'
```

Keep:

```yaml
- '--memory'
- '4Gi'
- '--timeout'
- '900s'
```

Do not make this cleanup until the promoted revision and first controlled scheduled run are proven.

---

## 16. Evidence log template

Append a dated entry here or in a linked handoff document after each completed gate:

```text
Date/time:
Operator:
Gate:
Dev commit:
Main commit:
Cloud Build ID:
Candidate revision:
Candidate tagged URL:
Previous serving revision:
Traffic before:
Traffic after:
Memory/timeout proof:
Environment/dataset proof:
Commands/tests run:
Results:
Cloud Run log findings:
Scheduler state/deadline/retries:
Production writes expected:
Production writes observed:
Decision: GO / NO-GO / ROLLED BACK
Next smallest action:
```

Never record secrets or Firebase tokens.

---

## 17. Final definition of done

Packet 4 production cutover is complete only when all of the following are true:

- `dev` changes were reviewed and intentionally merged to `main`;
- the new revision was first deployed at 0% normal traffic;
- the previous revision remained at 100% during candidate testing;
- candidate memory is `4Gi` and timeout is `900s`;
- candidate production defaults and runtime identity are correct;
- health, CORS, unauthenticated auth behavior, authenticated `/me`, `/games`, and a non-final `/game` passed;
- a frontend preview rendered successfully;
- candidate logs contained no blocking errors;
- traffic promotion was explicit and verified;
- the live frontend passed after promotion;
- Scheduler is enabled with a `900s` deadline and retries remain deliberately controlled;
- the first scheduled production run returned truthful stage evidence;
- the rollback revision and procedure were recorded;
- the permanent decision about no-traffic deployments was documented.

Until all criteria pass, describe the state precisely: **candidate deployed**, **candidate validated**, **promoted**, or **scheduled run proven**. Do not collapse those into a vague “production ready.”

---

## 18. Reference documentation

- [Cloud Run rollouts, rollbacks, traffic migration, and tagged revisions](https://docs.cloud.google.com/run/docs/rollouts-rollbacks-traffic-migration)
- [Cloud Run HTTPS and tagged URL formats](https://docs.cloud.google.com/run/docs/triggering/https-request)
- [Cloud Scheduler pause command](https://docs.cloud.google.com/sdk/gcloud/reference/scheduler/jobs/pause)
- [Cloud Scheduler resume command](https://docs.cloud.google.com/sdk/gcloud/reference/scheduler/jobs/resume)

These references support the release mechanism. The live GameLens service configuration and current `meow/dev` code remain the source of truth for the actual cutover.

---

## 19. Next action after this documentation commit

Stop. Review this plan.

When ready, perform only Gate B:

```text
inspect current dev and cloudbuild.yaml
→ add --no-traffic and packet4-candidate tag to main deployment only
→ review the exact diff
→ run focused tests
→ commit the isolated configuration change
→ stop before merging main
```
