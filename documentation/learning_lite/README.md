# GameLens Learning Lite Documentation Index

**Current status:** LL-1 planning baseline accepted; LL-2 is ready to start from the handoff prompt below; implementation has not started.
**Created:** 2026-08-21
**Repository:** `csells10/meow`
**Planning and implementation branch:** `learning-lite`
**Production authority:** `main` at `b93c41c210288b9b4d450b4145e2d596e566aa67`; Learning Lite documentation has not changed it
**Week 1 safety target:** be ready by the Wednesday, 2026-09-09 at 8:20 p.m. Eastern safety deadline, before the regular-season opener

Learning Lite is the current planning direction for GameLens learning. It replaces the proposed six-table operational platform as the next implementation path while preserving the completed packet work as evidence and a source of selectively reusable code.

The working principle is:

> Keep the football evidence and learning results. Keep operational plumbing lightweight until real use proves that more is necessary.

## Branch model

- `main` is the production authority.
- `learning-lite` is the only branch for current Learning Lite planning and future implementation.
- `dev` preserves the earlier operationalization prototype at commit `26287205f420f569d81ccfcb28a8e8e0656fc24b` and is a selective salvage source, never a wholesale merge candidate.
- No other long-lived branch is required for the current plan.

---

## Start here

Read these documents in order:

1. [Learning Lite Architecture](./GameLens_Learning_Lite_Architecture.md) — product boundaries, terminology, persistent data model, League Discovery readiness, calibration flexibility, and salvage decisions.
2. [Learning Lite Sprint](./GameLens_Learning_Lite_Sprint.md) — current checkpoint, proposed sequence, acceptance gates, Week 1 target, risks, and open decisions.
3. [Learning Lite Salvage Matrix](./GameLens_Learning_Lite_Salvage_Matrix.md) — the decision filter for all 92 archived development changes and their tests; it says what may be selectively adapted, deferred, preserved, or omitted and is not a build checklist.
4. [`documentation/GameLens_Product_Ideas.md`](../GameLens_Product_Ideas.md) — larger product ideas, including League Discovery and Postgame Signal Validation.
5. [Archived `documentation/live` packet suite](https://github.com/csells10/meow/tree/26287205f420f569d81ccfcb28a8e8e0656fc24b/documentation/live) — historical packet index and evidence from the earlier six-table direction.

The packet documents under `documentation/live` remain valuable implementation evidence. They are not the current authority for what should be built next.

---

## Current decision

Learning Lite starts from the production-safe capabilities already on `main` and selectively reuses the smallest valuable pieces from the archived `dev` work.

### Keep as the authority

- the existing Schedule, Stats, and Scores ingestion path;
- Facts, Windowed Metrics, and Rankings;
- `metric_registry.py` metadata and `lens_tags`;
- the current `/game` product response;
- the existing claim extractor, validator, feature worker, and language-calibration worker;
- the existing Claim Health and historical QA evidence;
- the current runtime claim-language support rules, including the backend support translated by the frontend as “Fits matchup.”

### Add or adapt in sequence

- **LL-2:** construct and validate a deterministic, hashable, pregame-safe `/game` payload with no final-score query, completed-game write, table, or persistence;
- **LL-3:** add one immutable pregame snapshot per eligible game, identical-retry safety, conflict protection, and a manual read-back check;
- **LL-4:** add snapshot lineage to claim-training rows and connect the existing Level 1/Claim Extraction owner through a thin adapter;
- at every checkpoint, port only the focused safety tests that travel with the approved behavior.

### Preserve for optional future use

- League Discovery based on existing rankings and `lens_tags`;
- Postgame Signal Validation based on snapshots, claims, outcomes, and final Facts;
- new pregame features that can later be evaluated by Language Calibration;
- a future algorithm that reads stable observations and versioned training examples without changing the evidence underneath it.

### Do not build initially

- a permanent multi-stage coordinator;
- `stage_runs`, `stage_game_results`, or postgame receipt tables;
- a duplicate outcome ledger;
- a new run-visibility backend or frontend;
- automatic Language Calibration promotion;
- a League Discovery interface;
- a winner-prediction product.

---

## Current checkpoint

### Checkpoint LL-0 — Direction and documentation

**Status:** Planning complete; implementation not started.

Agreed direction:

- Week 1 scope is the minimum trustworthy pregame evidence path.
- Week 1 may truthfully contain unavailable rankings and zero claim rows.
- The snapshot must preserve that absence rather than manufacture evidence.
- League Discovery does not need to be built for its source data to remain available.
- Language Calibration is the editor of historical claim language, not a catch-all for new ideas.
- New metrics, claims, validation rules, and pregame features stay with their owning code layers.
- New calibration findings remain versioned and advisory until deliberately promoted into runtime behavior.
- The historical `dev` work must remain preserved before any branch replacement or reset.

**Historical next checkpoint:** LL-1 — preserve and inventory.

### Checkpoint LL-1 — Preserve and inventory

**Date:** 2026-08-22
**Status:** Accepted as the current documentation baseline; implementation remains not started
**Branch / commit:** `learning-lite`, created from production `main` at `b93c41c210288b9b4d450b4145e2d596e566aa67`

**Objective**

Preserve the historical development work, establish an isolated Learning Lite branch from the production authority, and prepare a file-by-file salvage decision before implementation begins.

**Files changed**

- `documentation/learning_lite/README.md`
- `documentation/learning_lite/GameLens_Learning_Lite_Architecture.md`
- `documentation/learning_lite/GameLens_Learning_Lite_Sprint.md`
- `documentation/learning_lite/GameLens_Learning_Lite_Salvage_Matrix.md`

**Verification performed**

- verified GitHub `main` at `b93c41c210288b9b4d450b4145e2d596e566aa67`;
- verified historical `dev` at `26287205f420f569d81ccfcb28a8e8e0656fc24b`;
- verified `dev` itself retains the complete historical prototype at that exact commit, so a duplicate archive branch is not required;
- created `learning-lite` from the verified `main` commit and confirmed it was initially identical to `main`;
- kept the BigQuery REST-warning fix isolated from the Learning Lite documentation work.

**Observed evidence**

The historical development state remains recoverable from `dev` and its exact commit permalink. Learning Lite has a clean production-based branch without the 92-file development delta.

**Data and production effects**

No Learning Lite code ran. No BigQuery data was read or written for this checkpoint. No production traffic, Cloud Run configuration, or deployed service was changed by the branch and documentation work.

**Decisions**

- Product name: **Learning Lite**.
- Planning and implementation branch: `learning-lite`.
- Production authority: `main`.
- Historical development source: `dev` at `26287205f420f569d81ccfcb28a8e8e0656fc24b`.
- `dev` is a file-by-file salvage source, never a wholesale merge candidate.
- Checkpoint sequence: LL-2 builds and validates a side-effect-free pregame payload; LL-3 adds the single immutable snapshot store; LL-4 connects Claim Extraction to that snapshot.

**Known limitations / carry-forward**

The file-by-file salvage matrix and focused test mapping are accepted as the current planning baseline. They remain intentionally revisable. LL-2 authorizes no table, persistence, deployment, or production behavior change; implementation still requires a separate explicit start decision.

**Rollback or recovery point**

The clean starting point is `main` at `b93c41c210288b9b4d450b4145e2d596e566aa67`. Historical development work is preserved on `dev` at `26287205f420f569d81ccfcb28a8e8e0656fc24b`.

**Next checkpoint**

When implementation is explicitly authorized, begin LL-2 only: construct and validate the pregame-safe payload boundary without persistence, schema changes, deployment, or production wiring.

---

## Checkpoint documentation contract

Every Learning Lite checkpoint must update this README before the checkpoint is considered closed.

Record all of the following:

1. **Checkpoint identifier and date**
2. **Status** — planning, in progress, blocked, accepted, or superseded
3. **Branch and commit**
4. **Objective**
5. **Files changed**
6. **Tests and commands run**
7. **Observed results** — counts, game IDs, hashes, statuses, or relevant payload evidence
8. **Data effects** — tables read, tables written, rows inserted/updated, and whether production was touched
9. **Product behavior effects** — especially `/game`, Matchup Lean, confidence, language support, and frontend behavior
10. **Decisions made**
11. **Known limitations and honest gaps**
12. **Rollback or recovery point**
13. **Carry-forward work**
14. **Exact next checkpoint**

Use this template:

```markdown
### Checkpoint LL-# — Name

**Date:** YYYY-MM-DD
**Status:**
**Branch / commit:**

**Objective**

**Files changed**

**Verification performed**

**Observed evidence**

**Data and production effects**

**Decisions**

**Known limitations / carry-forward**

**Rollback or recovery point**

**Next checkpoint**
```

Do not mark a checkpoint complete merely because code exists. Completion requires observable evidence and an explicit next-state handoff.

---

## Terminology used in this folder

Use descriptive names in product and planning discussion while retaining the existing Level terminology where code compatibility requires it.

| Descriptive name | Existing term | Purpose |
|---|---|---|
| Pregame Snapshot | Capture boundary / Level 0 context | Preserve exactly what GameLens knew before kickoff |
| Claim Extraction | Level 1 | Convert the frozen product response into claim rows |
| Claim Grading | Level 2 | Compare claims with completed-game evidence |
| Feature Enrichment | Level 3 | Attach pregame-only explanatory features |
| Language Calibration | Level 4 | Evaluate historical reliability and recommend language treatment |

Language Calibration is not a general algorithm layer, feature factory, League Discovery engine, or automatic runtime controller.

---

## Maintenance rules

- This README owns checkpoint status and handoff breadcrumbs.
- The Sprint owns proposed sequence, scope, and acceptance gates.
- The Architecture document owns stable boundaries and terminology.
- Completed evidence must not be silently rewritten; append a dated correction or supersession note.
- Proposed work must never be described as implemented.
- Development evidence must never be described as production behavior.
- If scope changes materially, update all three Learning Lite documents in the same documentation checkpoint.
- Keep the older `documentation/live` packet suite intact as historical evidence.

---

## Start LL-2 on 2026-08-24 — ready-to-paste development prompt

Using this prompt is Christian's explicit authorization to implement **LL-2 only**. It does not authorize LL-3, persistence, deployment, or any later checkpoint.

```text
Start GameLens Learning Lite checkpoint LL-2 in csells10/meow on the learning-lite branch.

This prompt is explicit authorization to implement LL-2 only. Do not begin LL-3 or any later checkpoint.

Before editing:
1. Verify the current GitHub branch state and work only on learning-lite.
2. Treat main as the production authority and do not modify main or dev.
3. Read completely, in order:
   - documentation/learning_lite/README.md
   - documentation/learning_lite/GameLens_Learning_Lite_Architecture.md
   - documentation/learning_lite/GameLens_Learning_Lite_Sprint.md
   - documentation/learning_lite/GameLens_Learning_Lite_Salvage_Matrix.md
   - documentation/GameLens_Product_Ideas.md
4. Inspect current learning-lite/main code first. Use dev at
   26287205f420f569d81ccfcb28a8e8e0656fc24b only as a read-only salvage source.
   Do not merge or cherry-pick dev wholesale.
5. Reconcile the exact LL-2 file and test list against current code before changing anything.

LL-2 objective:
Implement the smallest side-effect-free pregame contract that can construct and identify the response GameLens would produce before kickoff.

Candidate behavior to reuse or adapt only when needed:
- a side-effect-free evidence container and loader;
- a shared game-response builder;
- a pregame-only entry point;
- deterministic capture identity;
- canonical payload SHA-256;
- detection and rejection of postgame-shaped payloads.

Required proof:
- the live and pregame builders produce equivalent pregame product sections from the same evidence;
- pregame mode cannot query final score;
- pregame mode cannot invoke completed-game outcome writes;
- postgame-shaped payloads fail closed;
- one eligible fixture produces a deterministic, hashable payload;
- existing /game behavior, Matchup Lean, confidence, claim-language support including “Fits matchup,” auth, CORS, routes, and frontend behavior remain unchanged;
- lens_tags remains BigQuery REPEATED STRING, Python list[str], and a JSON array.

Hard LL-2 boundaries:
- no table creation or schema change;
- no BigQuery or other persistence writes;
- no snapshot storage or retry reconciliation;
- no Claim Extraction writes;
- no coordinator, receipt ledger, Admin work, frontend work, backfill, or broad refactor;
- no deployment, merge to main, scheduling, trigger change, traffic change, or production invocation.

Working method:
- prefer existing main/learning-lite behavior over porting;
- adapt only the smallest useful seams from dev;
- keep one behavior per commit;
- run the smallest focused package-style tests from the repository root;
- stop if preserving the live route requires broader scope;
- do not continue merely because more dev code exists.

Before closing LL-2:
- update this README with the checkpoint date, branch/commit, exact files, tests and results, observed evidence, data and production effects, decisions, gaps, recovery point, and exact next checkpoint;
- update the Sprint only if scope, sequencing, status, or acceptance criteria changed;
- explicitly state that no table, persistence, deployment, or production behavior changed;
- stop at the LL-2 exit gate and request review before LL-3.
```
