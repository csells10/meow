# How to Use the GameLens August Readiness Roadmap

- **Roadmap:** `documentation/August/GameLens_Backend_August_Readiness_Roadmap.md`
- **Repository:** `csells10/meow`
- **Code source of truth:** Current `meow/dev`

This file helps a new ChatGPT conversation resume backend work without relying on memory from an older chat.

Use the attached How-To and canonical August Readiness Roadmap to continue the GameLens backend work. The older August Readiness Plan is a superseded planning draft and must not override the roadmap. First inspect the current meow/dev branch and recent commits read-only. Reconcile the repository against the roadmap, confirm the current packet, and recommend only the next smallest locally testable step. Preserve lens_tags, Levels 1–4, historical runs, and existing behavior. Do not change code until we agree on the next step. When providing Python changes, give me complete functions rather than scattered line edits.

The goal is simple:

> Inspect current code and commits, determine the first unfinished roadmap packet, complete one locally testable behavior, and stop.

---

## Current verified handoff — 2026-07-29

This snapshot is a starting point, not a substitute for checking current `meow/dev`.

```text
Branch/source of truth: dev
Last verified implementation commit: 4c3e919
Latest completed packet: Packet 2 — GameLens metric pipeline conductor
Packet 2 implementation: e054ee0; naming clarification: 4c3e919
Verification: 7 focused Packet 2 tests passed; credentialed 2025 BigQuery dry run succeeded with write=False
Dry-run rows: Facts 36,532; Windowed Metrics 138,532; Rankings 426,086
Protected behavior: no BigQuery tables changed; app.py and production scheduling remain unchanged
Next packet: Packet 3 — 2026 operational checkpoint
```

`e054ee0` added Packet 2's small conductor, and `4c3e919` clarified its scope through the final module name `services/gamelens_metric_pipeline_conductor.py`. The conductor requires an explicit season, calls the existing builders in Facts → Windowed Metrics → Rankings order, stops on invalid or failed stages, and returns truthful stage statuses and row counts. It is inert until `app.py` calls it.

The real 2025 `write=False` run completed successfully with 36,532 Facts rows, 138,532 Windowed Metrics rows, and 426,086 Rankings rows. The expected warnings—5,328 rows across eight unregistered metrics excluded and 220 duplicate Facts rows deduplicated with `keep="last"`—did not fail the run. No BigQuery table was created, replaced, appended to, recreated, or wiped.

If current `origin/dev` contains later commits, inspect and reconcile them. Do not reset or redo Packet 2 merely because this snapshot is older than the branch.

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
Document Packet 2 completion and advance to Packet 3 [skip ci]
```

This skip marker is an extra safeguard for the documentation checkpoint. It does not replace correcting and verifying the production trigger's branch filter.

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
- Packet 3 verifies representative strong, supporting/context, and watch/excluded metrics.

This protection does not require a new service, table, or scheduler.

---

## 7. Ready-to-paste opening prompt

```text
We are continuing the GameLens backend August-readiness work.

Repository: csells10/meow
Branch/source of truth: current meow/dev
Last verified implementation: Packet 2 at e054ee0, clarified at 4c3e919, on 2026-07-29
Latest verified completed packet: Packet 2 — GameLens metric pipeline conductor
Focused verification: 7 Packet 2 tests passed
Credentialed verification: 2025 write=False dry run succeeded (Facts 36,532; Windowed 138,532; Rankings 426,086)
Expected next packet: Packet 3 — 2026 operational checkpoint

First read these files from the repository:
- documentation/August/GameLens_Backend_August_Readiness_Roadmap.md
- documentation/August/GameLens_August_Readiness_Roadmap_How_To.md

Begin read-only. Inspect current dev, recent commits, existing tests, and the complete functions relevant to the roadmap. Reconcile the roadmap progress snapshot from code and test evidence; do not trust checkboxes or prior chat memory by themselves.

Tell me:
1. the latest confirmed completed packet and its evidence;
2. the first unfinished or uncertain packet;
3. whether that packet changes production behavior;
4. the smallest local test that will prove it;
5. the exact files/functions we would touch.

Preserve the roadmap's safety contract, including lens_tags as REPEATED STRING/list[str] from metric_registry.py through Windowed Metrics, Rankings, queries, and /game.

For Packet 3, inspect the existing metric-table setup script, current 2025 schemas, available 2026 source data, and the Packet 2 conductor before proposing any write. Separate read-only schema/source checks from deliberate 2026 table creation or replacement.

Do not change code yet. Do not redesign GameLens. Do not jump ahead to Packet 4, app.py, or Levels 1–4. First propose the smallest safe Packet 3 checkpoint, including the exact BigQuery reads, any deliberate writes, expected evidence, and stop conditions.
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
- Do not keep working through several packets in one oversized change.
