# GameLens Learning Lite Documentation Index

**Current status:** Planning baseline finalized; implementation has not started.
**Created:** 2026-08-21
**Repository:** `csells10/meow`
**Planning and implementation branch:** `learning-lite`
**Production branch:** `main` remains unchanged
**Week 1 safety target:** preserve a valid pregame read before the first 2026 regular-season kickoff on 2026-09-09

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
3. [Learning Lite Salvage Matrix](./GameLens_Learning_Lite_Salvage_Matrix.md) — file-by-file disposition of all 92 archived development changes and their tests.
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

### Add or adapt first

- one immutable pregame snapshot per eligible game;
- deterministic snapshot identity and payload hashing;
- a pregame-safe `/game` builder with no final-score query or completed-game write;
- snapshot lineage on claim-training rows;
- a thin Level 1/Claim Extraction path that reuses the existing extractor;
- focused safety tests and a manual fallback command.

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

**Next checkpoint:** LL-2 — minimal pregame contract, with no persistence or schema work.

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

## Fresh-chat handoff prompt

```text
Continue GameLens from csells10/meow using the Learning Lite direction.

Do not begin by writing code. Read, in order:
1. documentation/learning_lite/README.md
2. documentation/learning_lite/GameLens_Learning_Lite_Architecture.md
3. documentation/learning_lite/GameLens_Learning_Lite_Sprint.md
4. documentation/learning_lite/GameLens_Learning_Lite_Salvage_Matrix.md
5. documentation/GameLens_Product_Ideas.md
6. the archived documentation/live packet suite at commit
   26287205f420f569d81ccfcb28a8e8e0656fc24b only for historical evidence

Treat main as the production authority. Treat the preserved dev archive and the
documentation/live packet suite as a salvage/archive source, not as approval to
continue Packet 6. Confirm the current Learning Lite checkpoint and the exact
bounded next action before making changes.
```
