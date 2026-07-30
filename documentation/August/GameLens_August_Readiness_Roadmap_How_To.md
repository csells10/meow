# How to Use the GameLens August Readiness Roadmap

- **Roadmap:** `documentation/August/GameLens_Backend_August_Readiness_Roadmap.md`
- **Repository:** `csells10/meow`
- **Code source of truth:** Current `meow/dev`

This file helps a new ChatGPT conversation resume backend work without relying on memory from an older chat.

Use the attached How-To and canonical August Readiness Roadmap to continue the GameLens backend work. The older August Readiness Plan is a superseded planning draft and must not override the roadmap. First inspect the current meow/dev branch and recent commits read-only. Reconcile the repository against the roadmap, confirm the current packet, and recommend only the next smallest locally testable step. Preserve lens_tags, Levels 1–4, historical runs, and existing behavior. Do not change code until we agree on the next step. When providing Python changes, give me complete functions rather than scattered line edits.

The goal is simple:

> Inspect current code and commits, determine the first unfinished roadmap packet, complete one locally testable behavior, and stop.

---

## Current verified handoff — 2026-07-30

This snapshot is a starting point, not a substitute for checking current `meow/dev`.

```text
Branch/source of truth: dev
Last verified code commit on dev: 2d95b4e — Align Cloud Run image with Python 3.11
Latest completed packet: Packet 3 — 2026 operational checkpoint (preseason/zero-data readiness)
2026 schedule: 322 games loaded; repeat preview skipped all 322 with 0 inserts
2026 metric tables: Facts, Windowed Metrics, and Rankings shells created and verified empty
Schema proof: 39/33/39 columns match 2025; lens_tags is STRING/REPEATED
Conductor proof: write=False failed closed at empty Facts and skipped downstream stages
/game proof: scheduled 20260806_CAR@ARI resolved safely with empty metrics and explicit unavailable reasons
Runtime preflight: Dockerfile now uses Python 3.11, matching runtime.txt; commit changed only Dockerfile and is pushed to dev
Deployment trigger proof: dev trigger disabled; enabled production trigger matches ^main$ only
Current deadlines: Cloud Run 300s; production Scheduler 180s; Scheduler has no automatic retries
Activation decision: align Cloud Run and Scheduler to 900s during controlled Packet 4 activation; keep retries disabled initially
Production impact so far: none — Python 3.11 and documentation are on non-deploying dev; no timeout setting changed
Protected behavior: no application code, app.py scheduling, formulas, Levels 1–4, or historical tables changed
Next packet: Packet 4 — Activate through app.py
Deferred proof: populated 2026 Stats → Facts → Windowed → Rankings → /game → Levels 1–4
```

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
Last verified code commit on dev: 2d95b4e — Align Cloud Run image with Python 3.11
Latest verified completed packet: Packet 3 — 2026 operational checkpoint (preseason/zero-data readiness)
Expected next packet: Packet 4 — Activate through app.py

Packet 3 evidence:
- League.schedule contains all 322 2026 games; repeat preview skipped 322 and inserted 0.
- Analytics.game_team_metric_facts_2026, Analytics.team_metrics_windowed_2026, and Analytics.team_metric_rankings_2026 exist with zero rows.
- Their schemas match 2025 exactly at 39/33/39 columns.
- lens_tags remains STRING/REPEATED.
- run_gamelens_metric_pipeline(season="2026", write=False) failed closed at empty Facts and skipped downstream stages without changing the tables.
- Scheduled game 20260806_CAR@ARI resolved through the /game response builder with season 2026, no final score, safe empty metric sections, no_ranking_rows_found, and ranking_context_unavailable.
- No application code, source ingestion, formulas, Levels 1–4, historical run IDs, or historical tables changed.
- Populated-row, populated-lens_tags, and Levels 1–4 real-data proof is deferred until accepted completed-game 2026 Stats/Scores exist.

Deployment preflight evidence:
- Commit 2d95b4e changed only Dockerfile from Python 3.9 to Python 3.11 and is pushed to dev.
- Dockerfile now matches runtime.txt at Python 3.11.
- The dev Cloud Build trigger is disabled; the enabled production trigger matches ^main$ only.
- The push to dev did not deploy production.
- Cloud Run currently allows 300 seconds; Get-NFL-Schedule allows 180 seconds and has no automatic retries.
- During controlled Packet 4 activation, align both deadlines to 900 seconds and keep retries disabled initially.

First read these files from the repository:
- documentation/August/GameLens_August_Readiness_Roadmap_How_To.md
- documentation/August/GameLens_Backend_August_Readiness_Roadmap.md

Begin read-only. Inspect current dev and recent commits, then inspect the complete current app.py scheduler path, its configuration, Schedule/Stats/Scores return contracts, services/gamelens_metric_pipeline_conductor.py, and focused tests.

Tell me:
1. what current dev proves;
2. the exact legacy 2025 aggregate hook Packet 4 will replace;
3. the smallest complete app.py behavior change;
4. the focused tests for no-op, success, partial failure/failure, targeted load_date, route registration, and auth preservation;
5. the exact BigQuery effects and rollback path;
6. whether the external production deployment trigger is still restricted away from dev;
7. how the Python 3.11 Docker image will be built and boot-tested locally;
8. the exact cloudbuild.yaml change for a 900-second Cloud Run request timeout and the controlled Scheduler command/check for a matching 900-second deadline.

Preserve every route, blueprint, auth check, targeted load_date path, /game behavior, metric formula, window definition, ranking rule, lens_tags contract, Level 1–4 path, and historical run.

Do not change code yet. Do not redesign GameLens. Do not begin Packet 5 or productionize Levels 1–4. Do not change production Cloud Run or Scheduler settings during read-only planning. First propose Packet 4 only, including exact files/functions, tests, Python 3.11 container rehearsal, expected behavior, matching 900-second activation settings, deployment safety, rollback, and stopping point.
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
