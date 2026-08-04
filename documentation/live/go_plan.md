# GameLens Controlled Production Cutover Plan

**Created:** 2026-08-02
**Last execution update:** 2026-08-04
**Repository:** `csells10/meow`
**Documentation branch:** `dev`
**Backend release branch:** `main`
**Target service:** `nfl-games-app-main`
**Current status:** **GATE G COMPLETE — GATE H REMEDIATION PROVEN IN DEV — NEW PRODUCTION CANDIDATE PENDING**
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

| Gate | State | Status on 2026-08-04 | Production user traffic | Exit requirement |
|---|---|---|---:|---|
| A | Documentation and design | Complete | Current revision 100% | Plan reviewed |
| B | No-traffic YAML on `dev` | Complete | Current revision 100% | Diff and tests pass |
| C | Final pre-merge inspection | Complete | Current revision 100% | Live configuration recorded |
| D | Candidate deployed from `main` | Complete | Current revision 100% | Candidate healthy at 0% normal traffic |
| E | Candidate API tests | Complete | Current revision 100% | Health, CORS, auth, `/games`, non-final `/game`, and logs pass |
| F | Candidate UI preview | **Complete — GO** | Current revision 100% | Preview auth, `/me`, `/games`, non-final `/game`, logs, and live-site comparison passed |
| G | Promotion window | **Complete — GO** | Candidate `00146-meq` 100% | Rollback anchor rechecked, controlled promotion completed, live smoke tests passed, and Scheduler resumed |
| H | Remediation rollout and repaired scheduled run | **Remediation proven in dev — new candidate pending** | Current production revision remains live | Repaired production ETL proof recorded and release method decided |

No gate is implied. Record the evidence and make an explicit go/no-go decision before moving forward.

---

## 5A. Pre-Gate-F execution checkpoint — 2026-08-03

### Exact state

```text
State: candidate API validated
Current gate: Gate F — isolated Lovable frontend preview
Production promotion: not performed
Scheduler pause: not performed
Normal production traffic: unchanged
Production frontend: unchanged
```

This is a safe pause point. The backend candidate has passed the planned API contract checks, but the real browser application has not yet been pointed at it. Do not pause Scheduler or move traffic until Gate F is complete and explicitly recorded as `GO`.

### Git and deployment evidence completed

```text
Backend commit deployed: 26e85ca Deploy main as a no-traffic Packet 4 candidate
Candidate revision: nfl-games-app-main-00146-meq
Candidate tag: packet4-candidate
Candidate URL: https://packet4-candidate---nfl-games-app-main-ids7lwjjta-uc.a.run.app
Rollback/serving revision: nfl-games-app-main-00136-vsx
Normal traffic: 100% on nfl-games-app-main-00136-vsx
Candidate normal traffic: 0%; tag-only access
Candidate memory: 4Gi
Candidate timeout: 900s
Runtime identity: 362530996210-compute@developer.gserviceaccount.com
Dev/replay environment overrides: none
```

At the deployment checkpoint, local `main`, local `dev`, `origin/main`, and `origin/dev` all pointed to `26e85ca`. Reconfirm live branch and revision state before any later mutation; this entry is evidence, not a replacement for a fresh check.

### Candidate API evidence completed

Seven expected requests were observed on the candidate revision:

| Test | Result |
|---|---|
| `GET /health` | `200`, `{"status":"ok"}` |
| `OPTIONS /game/contract-check` from `https://gamelens.io` | `200`; origin, `Authorization`, and `GET` allowed |
| `OPTIONS /me` from `https://gamelens.io` | `200`; origin, `Authorization`, and `GET` allowed |
| Unauthenticated `GET /game/contract-check` | Intentional `401` JSON with readable CORS response |
| Authenticated `GET /me` | `200`; established active-admin contract |
| Authenticated `GET /games?date=2026-08-06` | `200`; returned scheduled `20260806_CAR@ARI` |
| Authenticated `GET /game/20260806_CAR@ARI` | `200`; full game-page contract with legitimate early-season empty metrics |

The non-final game response correctly reported `game_status: Scheduled`, `final_score: null`, `model_outcome: null`, and `ranking_context.reason: no_ranking_rows_found`. Empty comparison and metric arrays are valid before 2026 results exist; the response degraded cleanly rather than failing.

The revision-specific log scan showed:

```text
POST requests: none
5xx responses: none
Unexpected requests: none
Application error scan: empty
Unexpected writes: none observed
```

The Firebase token used for the authenticated checks was entered silently and then removed from the shell. It was not recorded in this document.

### Lovable pre-change inspection findings

Lovable completed analysis only and reported:

- the production API URL is hardcoded once in `src/lib/nfl-api.ts`, line 7;
- `src/lib/admin-api.ts` imports the same `API_BASE`, so it requires no separate edit;
- there are no frontend `.env`, `import.meta.env`, or `VITE_*` API-base mechanisms;
- the published production site remains on its last published build when a branch preview changes;
- the safest preview is a temporary Lovable branch named `packet4-candidate-preview`;
- the temporary edit is one line pointing `API_BASE` at the candidate tagged URL;
- the branch must not be merged or published;
- the exact branch-preview hostname is unknown until Lovable creates/assigns it;
- a fresh/incognito browser session should be used so the 24-hour React Query cache cannot mask which backend answered.

Potential preview-origin requirements were also identified: Firebase Authorized Domains, Google OAuth Authorized JavaScript origins, and the backend CORS allowlist may need the exact branch-preview hostname. Those are additive but shared configuration changes. Do not make them preemptively. First obtain the exact preview hostname, then inspect and approve each narrow change. In particular, do not assume the candidate can receive a service-only CORS change until the backend's actual CORS configuration mechanism is verified.

### Why this has not overcomplicated the normal path

The Lovable branch is a temporary test adapter, not the future production architecture:

```text
Temporary Lovable branch
  → points directly to tagged candidate URL for browser proof
  → never merges and never publishes

Production gamelens.io
  → stays on the stable nfl-games-app-main service URL
  → stays unchanged during candidate testing
  → automatically reaches the new revision only after backend traffic is promoted to 100%
```

After Gate F passes, backend traffic—not frontend source—is promoted. The normal `gamelens.io` API URL does not need to change. The temporary Lovable branch can then be reverted or deleted. If promotion fails, traffic returns to the saved rollback revision while the production frontend remains unchanged.

This process cannot guarantee that software will never fail. It does keep the unproven browser path away from production users, preserves a known serving revision, and makes both the temporary frontend change and the backend traffic move independently reversible.

### Next smallest action

Christian must create and switch to `packet4-candidate-preview` through Lovable's branch switcher. Then send the following prompt to Lovable:

```text
GameLens Packet 4 candidate frontend preview — branch-only API_BASE change

Prerequisite: I have created and switched to the Lovable branch packet4-candidate-preview from current main.

Before editing, verify that the active branch is exactly packet4-candidate-preview. If it is not, stop and report the active branch. Do not make any change.

On that branch only, change exactly one line in src/lib/nfl-api.ts:

FROM:
export const API_BASE = "https://nfl-games-app-main-362530996210.us-central1.run.app";

TO:
export const API_BASE = "https://packet4-candidate---nfl-games-app-main-ids7lwjjta-uc.a.run.app";

Do not edit, create, rename, or delete any other file. src/lib/admin-api.ts already imports API_BASE and must not be changed.

Do not merge the branch.
Do not click or trigger Publish/Update.
Do not modify Firebase, Google OAuth, Cloud Run, CORS, domains, or any project setting.
Do not sign in, open a game, call the backend, or interact with the branch preview yet.

After the one-line change, report only:
1. Active branch name.
2. Branch preview URL and exact hostname. If Lovable has not assigned one, report UNKNOWN; do not guess.
3. File changed.
4. Exact one-line diff.
5. Confirmation that no other file changed.
6. Confirmation that the branch was not merged.
7. Confirmation that Publish/Update was not triggered and gamelens.io is unchanged.

Then stop.
```

### Stop conditions for the next action

Stop immediately if:

- the active Lovable branch is `main` or anything other than `packet4-candidate-preview`;
- Lovable proposes more than the one-line `API_BASE` edit;
- Lovable asks to publish, update, merge, or test sign-in before reporting the preview hostname;
- the candidate URL differs from the verified tagged URL above;
- any production setting changes unexpectedly.

---

## 5B. Gate F completion and rollback checkpoint — 2026-08-03

### Outcome

```text
Gate F decision: GO
Candidate browser validation: complete
Production promotion: not performed
Scheduler pause: not performed
Normal production traffic: unchanged
Production frontend source: unchanged
Next gate: Gate G preparation
```

Gate F proved the complete read-only browser path through the isolated candidate without changing the published frontend or normal Cloud Run traffic.

### Temporary Lovable preview configuration

```text
Lovable branch: packet4-candidate-preview
Preview URL: https://preview--nfl-analytica-pro.lovable.app/login
Preview origin: https://preview--nfl-analytica-pro.lovable.app
Only changed file: src/lib/nfl-api.ts
Branch merged: no
Publish/Update triggered: no
Production gamelens.io changed: no
```

The only temporary frontend change was:

```diff
- export const API_BASE = "https://nfl-games-app-main-362530996210.us-central1.run.app";
+ export const API_BASE = "https://packet4-candidate---nfl-games-app-main-ids7lwjjta-uc.a.run.app";
```

This branch is a disposable test adapter. It must not be merged into frontend `main` and must not be published. After the production cutover is proven, revert this line or delete the temporary branch.

### Gate F evidence

| Check | Result |
|---|---|
| Candidate CORS preflight from exact Lovable preview origin to `/me` | `200`; exact origin, `Authorization`, `GET`, and `OPTIONS` allowed |
| Lovable sign-in | Passed |
| GameLens sign-in and authenticated `/me` | Passed; application moved beyond access check |
| Browser `/games?date=2026-08-06` | Passed; displayed one `CAR at ARI` game at 8:00 PM |
| Browser `/game/20260806_CAR@ARI` | Passed; future preseason matchup rendered without a blank screen |
| Early-season empty ranking state | Rendered safely as “No clear matchup edge”; expected because ranking rows do not yet exist |
| Candidate request log | Browser `OPTIONS` and `GET` calls for `/me`, `/games`, and `/game` returned `200` |
| Candidate application error scan | Empty |
| Candidate write safety | No `POST /`; no ingestion triggered |
| Production `gamelens.io` slate | Passed for 2026-08-06 |
| Production `gamelens.io` matchup detail | Passed for `20260806_CAR@ARI` |

Two earlier `GET /` requests returned `405`. This is expected because the ingestion root accepts `POST`, not `GET`. They did not execute ingestion and are not Gate F failures. One preflight experienced a cold start; later request latency returned to normal.

### Rollback anchor confirmed by the post-preview safety check

```text
Rollback revision: nfl-games-app-main-00136-vsx
Rollback revision normal traffic: 100%
Candidate revision: nfl-games-app-main-00146-meq
Candidate access: packet4-candidate tag
Candidate normal traffic: 0%
Candidate tagged URL: https://packet4-candidate---nfl-games-app-main-ids7lwjjta-uc.a.run.app
Unexpected candidate 5xx responses: none
Candidate application errors: none
```

This is the confirmed rollback anchor from the latest Gate F traffic inspection. Because Cloud Run state is mutable, re-run the read-only service, Scheduler, and in-flight-request checks immediately before Gate G. If `00136-vsx` is no longer the sole 100%-serving revision, stop and record the new live anchor rather than using this snapshot blindly.

### What “fully main” means after this checkpoint

The normal path remains simple:

```text
GitHub main
  → contains the approved backend and the completed cutover documentation

Published frontend main
  → keeps the stable nfl-games-app-main service URL
  → does not receive the temporary tagged candidate URL

Cloud Run nfl-games-app-main
  → moves from 00136-vsx to 00146-meq only through the explicit Gate G traffic command
```

Do not merge or publish `packet4-candidate-preview`. The ordinary frontend URL already follows whichever Cloud Run revision owns 100% of the service's normal traffic.

### Next smallest actions

1. Commit this Gate F record to `dev`.
2. Fast-forward the local `dev` branch to the documentation commit.
3. Deliberately merge/push the documentation record from `dev` into GitHub `main`; this is a Git operation and does not itself move Cloud Run traffic because the deployed backend commit is already the no-traffic candidate commit.
4. Before Gate G, re-confirm:
   - the rollback revision still owns 100% normal traffic;
   - `00146-meq` is still the ready candidate;
   - Scheduler is enabled, has a `900s` deadline, and no ingestion request is in flight;
   - no unexpected GitHub or Lovable source change is present.
5. Only then pause Scheduler, promote the candidate, run the live smoke tests, and either resume Scheduler or roll back.
6. After the promoted path is proven, revert/delete the temporary Lovable preview branch.

Stop before Step 3 until the documentation fast-forward is inspected. Merging `dev` to `main`, pausing Scheduler, and moving Cloud Run traffic are separate authorization boundaries.


---

## 5C. Gate G production promotion completion — 2026-08-03

### Outcome

```text
Gate G decision: GO
Candidate revision promoted: nfl-games-app-main-00146-meq
Normal production traffic: 100% on candidate
Stable production frontend: passed
Scheduler: resumed and ENABLED
Production ingestion manually triggered: no
Rollback performed: no
Next gate: Gate H — first scheduled production run
```

Gate G moved the already validated Packet 4 candidate to normal production traffic, proved the ordinary frontend path, and restored the daily Scheduler only after the smoke tests and error review passed.

This is the formal completion of the **traffic cutover**. It is not yet the final Packet 4 definition of done because the first scheduled production ETL run has not occurred under the promoted revision.

### Git and revision anchors

```text
GitHub main before promotion: 6a29757
GitHub dev before promotion: 6a29757
Promoted revision: nfl-games-app-main-00146-meq
Promoted revision memory: 4Gi
Promoted revision timeout: 900s
Rollback revision: nfl-games-app-main-00136-vsx
Candidate tag retained: packet4-candidate
Stable Cloud Run service: nfl-games-app-main
```

The rollback revision remains the known pre-promotion runtime anchor. Traffic can be returned to `00136-vsx` with the documented rollback command if Gate H exposes a blocking production failure. Reconfirm that revision still exists and is Ready before any later rollback; do not rely on this dated snapshot blindly.

### Controlled promotion sequence and evidence

| Step | Evidence | Result |
|---|---|---|
| Fresh pre-cutover service snapshot | Rollback `00136-vsx` at 100%; candidate `00146-meq` Ready | Passed |
| Candidate runtime check | `4Gi`, `900s`, healthy container | Passed |
| Candidate tagged health | `GET /health` returned `200 {"status":"ok"}` | Passed |
| Scheduler safety pause | `Get-NFL-Schedule` reported `PAUSED` before traffic movement | Passed |
| Explicit traffic promotion | `00146-meq` received 100% normal traffic | Passed |
| Stable production health | Scheduler's ordinary service URL returned `200` | Passed |
| Revision attribution | Stable-URL health request logged on `00146-meq` | Passed |
| Production Google authentication | Sign-in completed successfully | Passed |
| Production slate | 2026-08-06 displayed `CAR at ARI` | Passed |
| Production matchup detail | `20260806_CAR@ARI` rendered safely | Passed |
| Expected early-season state | “No clear matchup edge” rendered without failure | Passed |
| Final candidate error scan | No fresh application errors found | Passed |
| Scheduler resume | Job reported `ENABLED` after all smoke tests passed | Passed |
| Final traffic verification | `00146-meq` remained at 100% | Passed |

### Live frontend request proof

The post-promotion request log tied the ordinary production hostname directly to `00146-meq`:

```text
OPTIONS /me: 200
GET /me: 200
OPTIONS /games?date=2026-08-03: 200
GET /games?date=2026-08-03: 200
OPTIONS /games?date=2026-08-06: 200
GET /games?date=2026-08-06: 200
OPTIONS /game/20260806_CAR@ARI: 200
GET /game/20260806_CAR@ARI: 200
Fresh candidate application error scan: empty
```

The final non-final game request completed successfully. The observed multi-second `/game` latency was not accompanied by a timeout, `5xx`, traceback, or error record.

### Final Scheduler state after Gate G

```text
Job: Get-NFL-Schedule
State: ENABLED
Schedule: 00 8 * * *
Time zone: America/New_York
Attempt deadline: 900s
HTTP method: POST
Target: https://nfl-games-app-main-362530996210.us-central1.run.app/
Traffic behind target: 100% nfl-games-app-main-00146-meq
Manual force-run issued during Gate G: no
```

Resuming the job restored its schedule; it did not issue a manual run. Because the August 3 8:00 a.m. Eastern window had already passed, the next expected automatic execution is August 4, 2026 at 8:00 a.m. Eastern.

The retry configuration was not changed during Gate G. The pre-cutover snapshot recorded:

```text
maxBackoffDuration: 3600s
maxDoublings: 5
maxRetryDuration: 0s
minBackoffDuration: 5s
```

Preserve and inspect the live values during Gate H rather than paraphrasing them as disabled or enabled without the exact configuration.

### Production-write safety

```text
Candidate API tests during Gates E–G: read-only
Promotion command: traffic routing only
Browser smoke tests: read-only non-final game paths
Manual POST / ingestion during Gate G: none
Unexpected production writes observed: none
```

Gate G did not prove the full daily ETL against production destinations. That proof belongs to Gate H.

### Frontend cleanup remains deferred

The temporary Lovable branch `packet4-candidate-preview` remains a test artifact:

```text
Merged into frontend main: no
Published to gamelens.io: no
Production API_BASE changed: no
Production frontend still uses the stable service URL: yes
```

Do not merge or publish that branch. Revert its one-line tagged-URL change or delete the temporary branch only after the promoted revision and first scheduled run are proven.

### Gate H stop point

Do not repeat the candidate deployment, Gate E API tests, Lovable preview, Scheduler pause, traffic promotion, or Gate G browser smoke test.

The next bounded action is to observe the first automatic scheduled run on August 4, 2026 at 8:00 a.m. Eastern and capture truthful production evidence for:

- the Scheduler attempt and HTTP result;
- the Cloud Run request on `00146-meq`;
- schedule, Stats, Scores, Facts, Windowed, and Rankings stage outcomes;
- request duration and any timeout, OOM, permission, or dataset-routing failure;
- Stats and Scores backlog state;
- post-run `/games` and one safe non-final `/game`;
- whether traffic remains 100% on `00146-meq`;
- whether the rollback anchor remains available.

Do not manually force the Scheduler job merely to accelerate Gate H. If the automatic run fails or its result is ambiguous, preserve logs and evidence before retrying, rolling back, or repairing data.


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

API `curl` checks have passed. This gate proves that the actual Lovable frontend can authenticate and render the candidate response without changing the published production frontend.

### Phase F1 — branch-only API target swap

Prerequisite: create and switch to Lovable branch `packet4-candidate-preview` from current frontend main through Lovable's branch switcher.

On that branch only, change:

```ts
export const API_BASE = "https://nfl-games-app-main-362530996210.us-central1.run.app";
```

to:

```ts
export const API_BASE = "https://packet4-candidate---nfl-games-app-main-ids7lwjjta-uc.a.run.app";
```

No other frontend file needs to change. `src/lib/admin-api.ts` imports the shared constant.

During F1:

- do not merge the branch;
- do not Publish/Update;
- do not change live `gamelens.io`;
- do not sign in or call the backend yet;
- record the exact branch preview URL and hostname;
- stop if the active branch is not `packet4-candidate-preview`.

Rollback for F1 is the single-line revert to the stable production URL or deletion of the temporary branch.

### Phase F2 — exact-origin authorization review

After Lovable reports the branch hostname, inspect the current configuration before changing anything:

1. Is the exact preview hostname already a Firebase Authorized Domain?
2. Is `https://<preview-host>` already an authorized JavaScript origin for the existing Google web client?
3. Does the candidate response allow that exact origin for `OPTIONS /me` and `OPTIONS /game/contract-check`?
4. How does the backend currently build its CORS allowlist, and can the preview origin be limited to the candidate without changing the live serving revision?

Do not broaden CORS to `*` for authenticated routes. Do not add guessed hostnames. Any required change must be additive, exact-origin, documented, and independently reversible.

### Phase F3 — browser test

Use a fresh/incognito browser profile to avoid the frontend's 24-hour persisted React Query cache.

- Sign in through the normal Firebase flow.
- Confirm `/me` succeeds in the browser network panel.
- Open the game list for `2026-08-06`.
- Open only verified non-final game `20260806_CAR@ARI` while it remains non-final.
- Confirm the page renders without a blank screen.
- Confirm no CORS errors appear in the console.
- Confirm no 401/403 occurs for the allowed user.
- Confirm no repeated request loop appears.
- Confirm degraded early-season sections render as unavailable/empty rather than crashing.
- Confirm the live production frontend still reaches the stable production URL and remains usable.
- Review candidate-revision logs again for only the preview-session requests, no `POST`, no `5xx`, and no application errors.

Do not use `POST /`, `/test`, a historical final game, or any write-triggering route.

### Go/no-go record

```text
Lovable branch:
Preview URL:
Preview hostname:
Only src/lib/nfl-api.ts changed:
No merge:
No Publish/Update:
Firebase preview authorization:
Google OAuth preview origin:
Candidate CORS preview origin:
Browser /me:
Browser /games:
Browser non-final /game:
Console errors:
Candidate log review:
Unexpected writes:
Production gamelens.io unchanged:
Decision: GO / NO-GO
Reviewer/date:
```

Do not promote without an explicit `GO`. After a successful preview, keep the production frontend source on the stable service URL. Do not merge the candidate URL into the production frontend.

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

## 19. Gate H remediation checkpoint — 2026-08-04

The instruction to wait for the first automatic August 4 Scheduler execution is retired. That execution occurred and produced the evidence that led to the Schedule no-game remediation.

Gate H is not complete. The correction has been proven in dev but has not yet been transferred to a new production candidate.

### Dev remediation evidence

```text
Schedule no-game correction commit: aabdd8f
Combined dev commit: 2d72dbd1c762b6ebae238ab40615a82142a8db47
Focused regression tests: 35 passed
Temporary daily revision: nfl-games-app-dev-00077-c8j
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
```

The overall `no_op` is correct. Schedule succeeded, while the future game was not yet eligible for Stats, Scores, or metric processing.

An immediate manual repeat temporarily encountered BigQuery's streaming-buffer restriction while replacing the newly inserted Schedule row. The row remained present exactly once with both processing flags `false`. After the buffer cleared, the approved single retest passed without code or data repair. This is recorded as an immediate-retry limitation, not a blocker for the normal daily cadence.

Dev was restored after the proof:

```text
Restored revision: nfl-games-app-dev-00078-ngp
Image commit: 2d72dbd1c762b6ebae238ab40615a82142a8db47
Traffic: 100%
Run mode: controlled_replay
Active season: 2025
Replay date: 2025-09-18
```

### Next bounded release sequence

```text
review and commit the Gate H evidence
→ inspect fresh Git and live production state
→ fast-forward main to the proven dev history
→ verify creation of a new 0%-traffic production candidate
→ repeat the read-only candidate gates
→ authorize or reject deliberate traffic promotion
→ preserve the first automatic production execution of the repaired revision
→ complete Gate H only if its Scheduler, Cloud Run, ETL, backlog, and frontend evidence pass
```

Do not manually invoke production `POST /`. Do not assume the current Scheduler or production revision state from this document; inspect both immediately before transferring the proven history to `main`.

Until the new production candidate is deployed and validated, describe the release precisely as:

```text
Gate G complete
Gate H remediation proven in dev
dev restored to controlled_replay
production remediation candidate pending
Gate H incomplete
```

Do not remove the no-traffic/tag release flags. Resume with a fresh Git and production-state inspection, execute one bounded step at a time, and stop at every authorization boundary.
