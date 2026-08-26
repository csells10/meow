# GameLens Learning Lite Documentation Index

**Current status:** LL-2 is implemented locally on `learning-lite` and has passed its controlled-fixture exit proof. It is awaiting review; neither LL-3 nor Matchup Lens M1 is authorized.
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
4. [Matchup Lens Product, Data, Query, and Implementation Specification](./GameLens_Matchup_Lens_Product_Data_and_Implementation_Spec.md) — the product, data, calculation, query, API, navigation, infrastructure, state, and QA authority for the Matchup Lens experience.
5. [`documentation/GameLens_Product_Ideas.md`](../GameLens_Product_Ideas.md) — larger product ideas, including League Discovery and Postgame Signal Validation.
6. [Archived `documentation/live` packet suite](https://github.com/csells10/meow/tree/26287205f420f569d81ccfcb28a8e8e0656fc24b/documentation/live) — historical packet index and evidence from the earlier six-table direction.

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
- **Matchup Lens M1:** after LL-2 is accepted or the sequence is explicitly re-planned, authorize a separate bounded checkpoint to canonicalize the six-lens formula and registry, reconcile the discovery values, and add unit tests; it must not be folded into LL-2;
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

Begin LL-2 only: construct and validate the pregame-safe payload boundary without persistence, schema changes, deployment, production wiring, or Matchup Lens implementation. After LL-2 reaches its exit gate, review whether Matchup Lens M1 or LL-3 should be authorized next; neither is authorized by the LL-2 prompt.

---

### Checkpoint LL-2 — Minimal pregame contract

**Date:** 2026-08-26
**Status:** Implemented locally and proven against controlled fixtures; awaiting review
**Branch / commits:** `learning-lite`; `26a93f2fb4c49346da1e487de34539e7c9a66174`, `321f160f809c4cb6af45f500708222c04d8a3b43`, `24db9bb01bfb9ba50ccb8001f5dfd0e05c833511`, and `4102ab7e0a67856d7cb3d5a737db35d0905fcf1c`

**Objective**

Construct and identify the response GameLens would produce before kickoff through one shared product builder, while making final-score queries, completed-game writes, and postgame-shaped payloads unreachable from the pregame boundary.

**Files changed**

Implementation and focused proof:

- `services/game_service.py`
- `services/gamelens_pregame_contract.py`
- `tests/_gcp_stubs.py`
- `tests/services/test_game_service_pregame_capture.py`
- `tests/services/test_gamelens_pregame_contract.py`
- `tests/test_game_window_selection.py`

Checkpoint closeout:

- `documentation/learning_lite/README.md`
- `documentation/learning_lite/GameLens_Learning_Lite_Sprint.md`

No query, route, auth, CORS, application-registration, frontend, schema, setup, coordinator, Admin, or persistence file changed.

**Verification performed**

From the repository root:

```text
python -m unittest \
  tests.services.test_gamelens_pregame_contract \
  tests.services.test_game_service_pregame_capture \
  tests.test_game_window_selection \
  tests.test_game_service_confidence_calibration \
  tests.test_qa_calibrated_matchup_lean_parity \
  tests.test_metric_registry
```

Result: `Ran 23 tests — OK`.

```text
python -m py_compile \
  services/gamelens_pregame_contract.py \
  services/game_service.py \
  tests/_gcp_stubs.py \
  tests/services/test_gamelens_pregame_contract.py \
  tests/services/test_game_service_pregame_capture.py \
  tests/test_game_window_selection.py
```

Result: passed with no output.

```text
git diff --check
```

Result: passed with no output.

**Observed evidence**

- Controlled eligible fixture: `20260909_SEA@NE`, Scheduled, 2026 Regular Season Week 1; capture time `2026-09-09T12:00:00Z`, before scheduled kickoff `2026-09-10T00:20:00Z`.
- Deterministic identity: `capture_ca1f097100b6563570b23464`.
- Canonical payload SHA-256: `c4a5307a2366a5fee895a3ba085a24970283d221f1419ad35b3d1dfdfabcc851`.
- Repeating the same fixture, learning-run ID, kickoff, and response produced the same capture ID, hash, and payload.
- Live and pregame entry paths produced equivalent pregame product sections from the same evidence.
- Pregame loading did not call `get_final_score`; pregame construction did not call `save_model_results`.
- Supplied final-score evidence was ignored by pregame mode, and a postgame-shaped builder result raised `postgame_fields_populated` before it could be identified or hashed.
- The shared builder preserved Matchup Lean, confidence, Model Trust, ranking context, matchup breakdown, and claim-language annotations. The focused proof retained `language_boost_allowed`, the backend support rendered by the frontend as “Fits matchup.”
- `lens_tags` remains `REPEATED STRING` in the existing ranking schema, is still normalized/carried as a Python `list`, and serialized as a JSON array. No lens-tag producer or schema file changed.
- `routes/game_routes.py`, `routes/games.py`, `auth/firebase_auth.py`, `app.py`, and all frontend code were unchanged, preserving the existing `/game` route, Firebase authorization, CORS registration, route registration, and frontend contract.

**Data and production effects**

No BigQuery table was created, altered, verified, read, or written by LL-2 proof. The focused tests used controlled in-memory evidence and a local BigQuery import stub. No snapshot, claim, outcome, receipt, or other persistence write occurred. No deployment, scheduling, trigger, traffic, production invocation, merge to `main`, or change to production behavior occurred.

**Decisions**

- Reused the existing `game_service.py` product calculations and inserted only an evidence container, evidence loader, shared builder, and pregame entry point.
- Added a small pure `gamelens_pregame_contract.py` instead of porting the broader archived learning contract. It owns only deterministic capture identity, canonical JSON/SHA-256, postgame-field detection, and fail-closed validation.
- Did not port `queries/gamelens_snapshot_queries.py`, slate selection, canonical retry decisions, snapshot manifests, storage, or any LL-3 behavior. The controlled fixture was sufficient for the LL-2 exit gate.
- Kept the live `get_game_details(game_id)` signature and route unchanged; it now delegates through the shared builder with the existing live defaults.
- Kept bounded behavior commits separate: pure identity/validation, shared response boundary, self-contained window-selection proof, and explicit per-fixture pre-kickoff eligibility enforcement.

**Known limitations / carry-forward**

- LL-2 is proven against controlled fixtures only. It intentionally did not execute a real BigQuery read or invoke the production route.
- LL-2 validates one supplied fixture's game identity, Scheduled status, supported phase, timezone-aware capture time, and pre-kickoff boundary. Slate selection and schedule-to-kickoff assembly are later bounded work.
- LL-2 does not store the response, reconcile retries, select a slate, extract claims, or provide an operator command.
- The exact LL-3 table/dataset/schema and the exact Matchup Lens M1 formula remain unresolved and must not be inferred from this code.

**Rollback or recovery point**

- Pre-LL-2 Learning Lite head: `65f7584c5d8bed1415e4301de86b269a5a5950b9`.
- Production authority remains `main` at `b93c41c210288b9b4d450b4145e2d596e566aa67`.
- Historical salvage source remains `dev` at `26287205f420f569d81ccfcb28a8e8e0656fc24b`.
- LL-2 can be recovered by reverting the four bounded implementation commits above on `learning-lite`; no data or infrastructure recovery is required.

**Next checkpoint**

Stop at the LL-2 exit gate and review this evidence. After review, Christian must explicitly authorize either LL-3 or Matchup Lens M1 (or request a correction to LL-2). Neither later checkpoint is authorized by the completed LL-2 work.

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

## Next work — LL-2 review gate

LL-2 has reached its bounded exit gate. Review the checkpoint record above and the four implementation commits before authorizing more work.

The next decision is deliberately not embedded in code:

1. accept LL-2 as the pregame safety boundary;
2. request a bounded correction to LL-2; or
3. explicitly authorize either LL-3 or Matchup Lens M1 as a separate checkpoint.

No persistence, Matchup Lens implementation, deployment, or production invocation is authorized until that review decision is recorded.
