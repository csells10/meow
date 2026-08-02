# How to Use the GameLens August Readiness Roadmap

- **Roadmap:** `documentation/August/GameLens_Backend_August_Readiness_Roadmap.md`
- **Repository:** `csells10/meow`
- **Code source of truth:** Current `meow/dev`

This file helps a new ChatGPT conversation resume backend work without relying on memory from an older chat.

Use the attached How-To and canonical August Readiness Roadmap to continue the GameLens backend work. The older August Readiness Plan is a superseded planning draft and must not override the roadmap. First inspect the current meow/dev branch and recent commits read-only. Reconcile the repository against the roadmap, confirm the current packet, and recommend only the next smallest locally testable step. Preserve lens_tags, Levels 1–4, historical runs, and existing behavior. Do not change code until we agree on the next step. When providing Python changes, give me complete functions rather than scattered line edits.

The goal is simple:

> Inspect current code and commits, determine the first unfinished roadmap packet, complete one locally testable behavior, and stop.

---

## Current verified handoff — 2026-08-02

This snapshot is a starting point, not a substitute for checking current `meow/dev`.

```text
Branch/source of truth: dev
Packet 4 application seam: implemented in app.py; current tests cover no-op, positive Stats, configured season, visible failures, and truthful HTTP status
Latest verified checkpoint: R6 real-data controlled replay — PASS with production resource follow-up
Replay target: 20250918_MIA@BUF in isolated 2025 dev datasets
Real source proof: live APIs wrote 132 Stats rows and 2 Score rows
Metric proof: Facts 36,538 → Windowed 138,532 → Rankings 426,965
Backlog proof: Stats 0; Scores 0
Production impact: none
Initial failure: dev Cloud Run 512Mi limit exceeded at 519Mi after Facts and Windowed committed
Recovery: dev service raised to 1Gi; missing Rankings stage executed once in a 4Gi, zero-retry Cloud Run job
Recovery execution: gamelens-r6-rankings-recovery-6nsns
Do not rerun: original R6 POST or Rankings recovery job
Schedule flags: false but non-authoritative; status tables cleared both backlog views
Current Packet 4 status: IN PROGRESS — dev real-data E2E proven; durable production resource configuration required
Merge gate: persist production memory and decide request-conductor versus dedicated Rankings job
Timeout gate: persist Cloud Run and Scheduler activation deadlines required by the roadmap; keep automatic retries disabled initially
Next action: make the resource/execution decision and validate production-equivalent configuration before merge to main
```

The full evidence and reusable partial-success recovery procedure are recorded in:

```text
documentation/August/GameLens_Packet_4_R6_Real_Data_Replay_and_Recovery_20260802.md
```

R6 proved the real source and metric data flow, but it also proved that the old `512Mi` service configuration is unsafe. Do not describe `1Gi` as a complete-chain proof: the web request stopped before Rankings, and Rankings was recovered separately at `4Gi`. The remaining Packet 4 decision must be carried into checked-in deployment configuration before merging to `main`.


The deployment preflight is also complete. Commit `2d95b4e` changed only `Dockerfile` from Python 3.9 to Python 3.11 and is present on `dev`; this matches `runtime.txt`. GitHub comparison confirmed `dev` is exactly one Dockerfile commit ahead of documentation checkpoint `d76dbea`. The external `dev` Cloud Build trigger is disabled, while the enabled production trigger matches `^main$`, so the push did not deploy production.

Cloud Run still has a 300-second request timeout. The enabled `Get-NFL-Schedule` job runs daily at 8:00 a.m. in `America/New_York`, has a 180-second attempt deadline, and has no automatic retries because `retryCount` is absent/default `0`. Packet 4 must persist a 900-second Cloud Run timeout in deployment configuration and align the Scheduler deadline to 900 seconds during controlled activation. Do not change production timeouts during read-only planning, and keep retries disabled until the local/container rehearsal and first controlled run succeed.

Packet 2's conductor remains implemented by `e054ee0` and clarified by `4c3e919`. It requires an explicit season, calls Facts → Windowed Metrics → Rankings, stops on invalid or failed stages, and remains inert until `app.py` calls it.

Packet 3 established the full 322-game 2026 schedule, created only the three missing empty 2026 metric shells, verified exact 2025 schema parity, and preserved `lens_tags` as `STRING/REPEATED`. The 2026 `write=False` conductor check correctly failed at Facts because there were zero completed-game source rows, skipped Windowed Metrics and Rankings, and left all three tables at zero rows.

The scheduled-game service-layer smoke test resolved `20260806_CAR@ARI` as season 2026, returned `final_score: null`, safely omitted metric sections, reported `no_ranking_rows_found` and `ranking_context_unavailable`, and emitted no malformed `lens_tags`. Because there were no metric rows, populated tag-array verification is deliberately deferred until accepted completed-game Stats exist.

If current `origin/dev` contains later commits, inspect and reconcile them. Do not reset or redo Packets 1–3 merely because this snapshot is older than the branch.
---

## 1. What is authoritative

Use this order when information disagrees:

1. Current code on `meow/dev`
2. Passing tests and recorded local/BigQuery evidence
3. Recent commits
4. The roadmap's decisions and done criteria
5. The roadmap progress snapshot
6. Prior chat summaries

The progress table is intentionally not authoritative. It saves time when current, but a new chat should correct it from code and commit evidence when necessary.

---

## 2. New-chat startup procedure

Before proposing code:

1. Read this guide and the full roadmap.
2. Open the current `csells10/meow` repository and inspect `dev`.
3. Record the current `dev` commit.
4. Review recent commits that could match roadmap packets.
5. Compare implemented code and tests with each packet's done criteria.
6. Reconcile the roadmap progress snapshot.
7. Name the first unfinished or uncertain packet.
8. Inspect the complete affected Python function or file.
9. State the smallest local test that would prove the next behavior.
10. Stop and explain the proposed packet before changing code.

If working from Christian's local checkout, first check for uncommitted changes. Do not blindly overwrite, reset, or pull over his work.

Useful orientation checks include:

```bash
git status --short
git branch --show-current
git rev-parse HEAD
git log --oneline --decorate -15
git fetch origin
git log --oneline --decorate HEAD..origin/dev
```

Fetching updates is safe for comparison. Merging, rebasing, switching branches, or pulling should happen only after the current working tree is understood.

---

## 3. Git and deployment safety

The working branch for August-readiness packets and roadmap checkpoints is `dev`. Do not apply these updates to `main`.

Before editing or committing:

```bash
git switch dev
git fetch origin
git status --short --branch
git rev-list --left-right --count origin/dev...HEAD
```

Proceed only from a clean, understood working tree. If local `dev` is merely behind `origin/dev`, update it with:

```bash
git pull --ff-only origin dev
```

A local commit does not deploy anything. A push can invoke an external Google Cloud Build trigger. The checked-in `cloudbuild.yaml` deploys the production service `nfl-games-app-main`, so neither the `dev` branch name nor documentation-only file scope is sufficient protection by itself.

Before a normal code push:

1. Confirm the external Cloud Build production trigger matches `main` only.
2. Do not push `dev` if that trigger can match `dev`.
3. After pushing, check build history and confirm no unintended production build started.

For a documentation-only checkpoint that must not invoke Cloud Build, include `[skip ci]` in the commit message:

```text
Document Packet 3 completion and advance to Packet 4 [skip ci]
```

This skip marker is an extra safeguard for the documentation checkpoint. It does not replace correcting and verifying the production trigger's branch filter.

### Verified deployment preflight — 2026-07-30

```text
Dockerfile runtime: Python 3.11
Runtime commit on dev: 2d95b4e
Dev Cloud Build trigger: disabled
Production Cloud Build trigger: enabled for ^main$ only
Cloud Run timeout: 300 seconds
Get-NFL-Schedule deadline: 180 seconds
Get-NFL-Schedule schedule: 8:00 a.m. America/New_York
Automatic Scheduler retries: none
```

The agreed Packet 4 activation target is 900 seconds for both Cloud Run and Scheduler. The limit itself does not bill 15 minutes; cost follows actual execution and BigQuery work. Persist the Cloud Run timeout in `cloudbuild.yaml`, change the Scheduler deadline only during controlled activation, and keep retries disabled initially.

---

## 4. How to determine the current packet

Do not mark a packet complete because a commit message sounds similar.

For each possible packet, verify:

- the expected file or function exists;
- the roadmap's required behavior is present;
- focused tests exist and pass;
- any required dry run or BigQuery check was actually performed;
- the related commit can be identified;
- no done criterion is merely assumed.

Use these statuses:

```text
NOT STARTED | NEXT | IN PROGRESS | BLOCKED | NEEDS RECHECK | COMPLETE | LATER
```

If evidence is incomplete, use `NEEDS RECHECK`. Do not send Christian backward or redo working code without first proving what is missing.

Christian does not need to remember to check off every packet. At a meaningful commit boundary, the active chat should offer the updated progress-table row or the complete updated roadmap file.

---

## 5. Working style for code changes

Christian makes and tests his own local changes. Guidance should therefore be easy to apply and easy to reverse.

- Complete replacement function when one function changes
- Complete file when several tightly coupled functions change
- Exact filename and function name
- Focused test with the behavior change
- One behavior per meaningful commit
- No unrelated cleanup
- No isolated "change these three lines" instructions inside a larger function
- No `app.py` activation before Packets 1–3 are proven

For each implementation step, provide:

1. What behavior becomes true
2. Files affected
3. Complete replacement function or file
4. Exact local test command
5. Expected result
6. Suggested commit message
7. One next packet only

### Unit tests versus a production API run

The Packet 1A/1B/1C test suite is a development regression suite, not another stage in the scheduled ETL cycle.

Current runtime behavior is:

```text
Scheduler or manual /test ingestion trigger
→ Schedule loader
→ Stats loader
   → Packet 1A validator runs inside Packet 1B loading/retry behavior
→ Scores loader
   → Packet 1C score validation and retry behavior
```

The safety behavior proven by the tests therefore runs automatically when its loader runs. The 31 `unittest` cases themselves do not run when the API job or the `/test` ingestion route is kicked off.

Run the focused source-ingestion suite explicitly with:

```bash
python -m unittest discover -s tests/api_calls -p "test_*.py" -v
```

Run Packet 2's focused conductor suite with:

```bash
python -m unittest discover -s tests/services -p "test_gamelens_metric_pipeline_conductor.py" -v
```

The credentialed historical read-only check is:

```bash
python -c "from services.gamelens_metric_pipeline_conductor import run_gamelens_metric_pipeline; print(run_gamelens_metric_pipeline(season='2025', write=False))"
```

This command reads and calculates against current stored 2025 inputs. Because `write=False` is forwarded to every builder, it does not create, replace, append to, recreate, or wipe BigQuery tables. Normal BigQuery query costs can still occur.

Packet 3 also ran the same conductor for season `2026` with `write=False`. With no completed-game Stats, the expected result is `failed_stage: facts` with Windowed Metrics and Rankings skipped. Treat that as a fail-closed early-season boundary, then verify the three 2026 metric tables remain unchanged. Do not reinterpret an empty Facts source as successful populated readiness.

The current `app.py` and `cloudbuild.yaml` do not invoke these commands. Automatic test execution would require a dedicated CI or Cloud Build test step. The route named `/test` is a manual ingestion endpoint; it is not the Python unit-test runner.

---

## 6. Lens-tag preservation rule

`lens_tags` is a protected forward-looking data contract.

Current intended flow:

```text
metric_registry.py
→ Windowed Metrics
→ Rankings
→ query/service layer
→ /game JSON
```

Rules:

- `metric_registry.py` owns the tag definitions.
- BigQuery stores `lens_tags` as `REPEATED STRING`.
- Python represents them as `list[str]`.
- `/game` exposes them as a JSON array.
- Do not flatten, stringify, rename, or silently discard tags.
- Normal builder and orchestration changes must not rewrite tag values.
- Packet 3 preserved the schema contract with `lens_tags = STRING/REPEATED`; because zero 2026 metric rows existed, populated tag-array and representative strong/supporting/watch samples remain part of the first completed-game follow-up.

This protection does not require a new service, table, or scheduler.

---

## 7. Ready-to-paste opening prompt

```text
We are continuing the GameLens backend August-readiness work.

Repository: csells10/meow
Branch/source of truth: current meow/dev
Current packet: Packet 4 — Activate through app.py
Current status: IN PROGRESS — dev real-data E2E proven; production resource configuration still required

First read these files from the repository:
- documentation/August/GameLens_August_Readiness_Roadmap_How_To.md
- documentation/August/GameLens_Backend_August_Readiness_Roadmap.md
- documentation/August/GameLens_Packet_4_R6_Real_Data_Replay_and_Recovery_20260802.md

R6 is closed and must not be rerun:
- Target game: 20250918_MIA@BUF
- Live APIs wrote 132 Stats rows and 2 Score rows to isolated dev tables.
- Facts wrote 36,538 rows.
- Windowed Metrics wrote 138,532 rows.
- Rankings recovery produced 426,965 rows.
- Both source backlogs are zero.
- Production was untouched.
- The original POST and recovery job must not be executed again.

Important R6 finding:
- The original dev request exceeded its 512Mi Cloud Run limit with 519Mi used.
- Facts and Windowed had already committed before the HTTP 503.
- Dev was raised to 1Gi and verified healthy.
- Only Rankings was recovered through a one-time 4Gi, zero-retry Cloud Run job.
- Therefore 512Mi is proven insufficient, but one uninterrupted complete-chain run at 1Gi is not proven.

Begin read-only. Inspect current dev, app.py, tests/test_app.py, services/gamelens_metric_pipeline_conductor.py, cloudbuild.yaml, cloudbuild-dev.yaml, and current Cloud Run/Scheduler configuration.

Tell me:
1. the exact current dev commit and whether the working tree is clean;
2. which Packet 4 code/tests and deployment criteria are complete;
3. the smallest durable production memory/execution design;
4. whether Rankings should remain in the request conductor or become a dedicated Cloud Run job;
5. the exact checked-in memory and timeout configuration changes;
6. the safest production-equivalent validation before merge to main;
7. the rollback plan and stop point.

Preserve every route, blueprint, auth check, targeted load_date path, /game behavior, metric formula, window definition, ranking rule, lens_tags contract, Level 1–4 path, historical run, and the status-table retry boundary.

Do not rerun R6. Do not manually flip the stale schedule flags. Do not touch production data during planning. Do not begin Packet 5 or productionize Levels 1–4. Propose only the remaining Packet 4 resource/deployment step first.
```

---

## 8. End-of-session handoff

At the end of a useful session, record only:

```text
Packet:
Status:
Commit hash and message:
Tests run and result:
BigQuery/dry-run evidence, if any:
Important decision:
Next packet:
```

If no commit was made, say so. A diagnosis or investigation can be useful without falsely advancing the roadmap.

---

## 9. What not to do

- Do not treat frontend game selection as claim-learning evidence.
- Do not make Levels 1–4 a prerequisite for `/game`.
- Do not reuse or replace historical QA `run_id` values.
- Do not add a workflow engine, run ledger, readiness subsystem, or snapshot platform unless a demonstrated problem requires it.
- Do not let an old attachment override current `meow/dev`.
- Do not mark a packet complete without test evidence.
- Do not treat Packet 3's expected empty-Facts failure as a regression.
- Do not claim populated 2026 `lens_tags` proof until real metric rows exist.
- Do not keep working through several packets in one oversized change.
