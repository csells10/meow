# How to Use the GameLens August Readiness Roadmap

- **Roadmap:** `documentation/August/GameLens_Backend_August_Readiness_Roadmap.md`
- **Repository:** `csells10/meow`
- **Code source of truth:** Current `meow/dev`

This file helps a new ChatGPT conversation resume backend work without relying on memory from an older chat.

Use the attached How-To and August Readiness Plan to continue the GameLens backend work. First inspect the current meow/dev branch and recent commits read-only. Reconcile the repository against the roadmap, confirm the current packet, and recommend only the next smallest locally testable step. Preserve lens_tags, Levels 1–4, historical runs, and existing behavior. Do not change code until we agree on the next step. When providing Python changes, give me complete functions rather than scattered line edits.

The goal is simple:

> Inspect current code and commits, determine the first unfinished roadmap packet, complete one locally testable behavior, and stop.

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

## 3. How to determine the current packet

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

## 4. Working style for code changes

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

## 5. Lens-tag preservation rule

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

## 6. Ready-to-paste opening prompt

```text
We are continuing the GameLens backend August-readiness work.

Repository: csells10/meow
Branch/source of truth: current meow/dev

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

Do not change code yet. Do not redesign GameLens. Do not jump ahead to app.py or Levels 1–4. Once we agree on the packet, provide complete replacement functions/files and focused tests rather than scattered line edits.
```

---

## 7. End-of-session handoff

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

## 8. What not to do

- Do not treat frontend game selection as claim-learning evidence.
- Do not make Levels 1–4 a prerequisite for `/game`.
- Do not reuse or replace historical QA `run_id` values.
- Do not add a workflow engine, run ledger, readiness subsystem, or snapshot platform unless a demonstrated problem requires it.
- Do not let an old attachment override current `meow/dev`.
- Do not mark a packet complete without test evidence.
- Do not keep working through several packets in one oversized change.
