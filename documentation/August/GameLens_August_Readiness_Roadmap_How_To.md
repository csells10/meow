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
Last verified implementation commit: 67e2212
Latest completed packet: Packet 1C — Score validation and retry safety
Packet 1C implementation: 67e2212
Verification: 31 focused Packet 1A/1B/1C tests passed; focused loader files compiled
BigQuery contract: existing Scores.scores and Scores.score_status schemas preserved
Next packet: Packet 2 — Metric pipeline conductor
```

`67e2212` implements Packet 1C on top of the preserved Packet 1A/1B and current application baseline. It validates final score payloads, batches Tank01 requests by game date, reconciles only the target game's two score rows, confirms storage before marking success, and preserves the live BigQuery schema. `app.py`, `/game`, lens tags, Levels 1–4, and `main` were not changed.

If current `origin/dev` contains later commits, inspect and reconcile them. Do not reset or redo Packet 1B merely because this snapshot is older than the branch.

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
Document Packet 1B completion and advance to Packet 1C [skip ci]
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
Last verified implementation: Packet 1C at 67e2212 on 2026-07-29
Latest verified completed packet: Packet 1C
Focused verification: 31 Packet 1A/1B/1C tests passed
Expected next packet: Packet 2 — Metric pipeline conductor

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

For Packet 2, inspect the complete current callable entry points in build_metric_facts.py, build_windowed_metrics.py, and build_metric_rankings.py before proposing the conductor. Reuse those builders; do not copy their SQL or logic into the conductor.

Do not change code yet. Do not redesign GameLens. Do not jump ahead to Packet 3, app.py, or Levels 1–4. Once we agree on Packet 2, provide complete replacement functions/files and focused tests rather than scattered line edits.
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
