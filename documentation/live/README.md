# GameLens Live Documentation Index

**Current status:** Packets 1–3 are complete. Packet 3 received **Implementation GO** on 2026-08-16 after its code, dev table, seven-capture dry inventory, zero-claim receipt/retry, and post-ETL snapshot-immutability proofs passed. Its first genuine populated-capture write/retry remains a required but non-blocking operational validation before production promotion. The separate Calibrated Matchup Lean hotfix is released, present in both `main` and `dev`, and fully cleaned up. Both `dev` builds completed successfully. Packet 4's pure capture-aware grader, completed inventory, development grade ledger, and read-only dry-grade runner are implemented; twenty-three combined focused local tests pass. DAL–SEA reconciled to one capture, two final-score rows, 130 Facts rows, zero claims, and one stage receipt. The 13-field `GameLens_dev.game_model_outcomes` ledger is verified empty. No grade row or production behavior change has occurred.

**Updated:** 2026-08-17  
**Working branch:** `dev`  
**Production posture:** The existing 8:00 AM production load and learning wiring are unchanged. Calibrated Matchup Lean revision `nfl-games-app-main-00155-qaf` serves 100% of normal traffic, with `nfl-games-app-main-00153-jol` retained as rollback. No learning stage is wired into `app.py`, no learning tables have been created in production, and no learning-orchestration change is ready for `main`.

This file is the starting point for a new chat or a GitHub-assisted review. It separates current authority, completed evidence, historical context, and future work so older language is not mistaken for present status.

---

## Recommended reading order

1. [GameLens Learning Orchestration Product Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md) — current sprint status, packet order, gates, and next action.
2. [Calibrated Matchup Lean Hotfix](./GameLens_Calibrated_Matchup_Lean_Hotfix.md) — completed rule, QA evidence, production receipt, rollback, and branch handoff.
3. [Packet 4 — Postgame Outcome plus Levels 2–3](./GameLens_Packet_4_Postgame_Learning.md) — the active review plan; read before Packet 4 code.
4. [Packet 3 — Production-Safe Level 1 Plan](./GameLens_Packet_3_Production_Level_1.md) — completed implementation evidence and deferred real-data validation.
5. [Packet 2 — Shadow Pregame Snapshot Plan](./GameLens_Packet_2_Shadow_Pregame_Snapshot_Plan.md) — completed implementation and cloud proof that Packet 3 inherits.
6. [Packet 1 — Pregame Capture Contract](./GameLens_Packet_1_Pregame_Capture_Contract.md) — the locked contract and invariants.
7. [Product Data Collection and Learning Handoff](./GameLens_Product_Data_Collection_and_Learning_Handoff.md) — architecture, level boundaries, and failure-trace contract.
8. [Runtime Configuration](./runtime_configuration.md) — deployment and environment controls; use when a packet reaches cloud validation.
9. [Go Plan](./go_plan.md) — historical production-cutover evidence with learning-track and hotfix cross-references. It is not the current learning-packet plan.

**Packet-numbering note:** older files under `documentation/August/` whose titles also say “Packet 4” describe the completed controlled 2025 ETL replay from August 1–2. They are historical evidence, not the authority for the active Packet 4 postgame-learning work.

---

## Document authority

| File | Role | Current state | Do not infer |
|---|---|---|---|
| [Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md) | Current execution authority | Packets 1–3 complete; Packet 4 plan active | A future packet is implemented merely because it is described |
| [Calibrated Matchup Lean](./GameLens_Calibrated_Matchup_Lean_Hotfix.md) | Completed separate release receipt | Live in production and forward-merged to dev | Packet 4 or Admin should recreate the rule |
| [Packet 4](./GameLens_Packet_4_Postgame_Learning.md) | Active bounded implementation plan | Inventory complete; development grade ledger code implemented; setup and dry grade pending | Approval to wire postgame learning into production |
| [Packet 3](./GameLens_Packet_3_Production_Level_1.md) | Completed implementation evidence | Implementation GO; real populated dev validation carried forward | Approval to write production data or proof that genuine claim rows have been observed in cloud |
| [Packet 2](./GameLens_Packet_2_Shadow_Pregame_Snapshot_Plan.md) | Completed handoff evidence | GO, with exact parity and per-game observability proven in dev | The August 6 game has a recoverable pregame snapshot |
| [Packet 1](./GameLens_Packet_1_Pregame_Capture_Contract.md) | Locked behavioral contract | Complete | Its older point-in-time status overrides later Packet 2 evidence |
| [Architecture handoff](./GameLens_Product_Data_Collection_and_Learning_Handoff.md) | Stable system design and level boundaries | Active | Architecture prose is a live execution receipt |
| [Runtime configuration](./runtime_configuration.md) | Environment and deployment reference | Active, with dated inventory sections | Every listed inventory value is timeless |
| [Go plan](./go_plan.md) | Historical release/cutover record | Retained for breadcrumbs | It is the current packet checklist |

If two files appear to conflict, use this order: current Sprint status, current packet, completed packet evidence, Packet 1 contract, architecture handoff, then historical/reference documents.

---

## Stable truths as of 2026-08-16

- Packet 2 established canonical, immutable pregame snapshots with exact `/game` parity, honest coverage gaps, attempt receipts, and per-game results.
- Packet 3 reads only canonical `pregame_snapshots`, uses one shared Level 1 extraction path, and writes development-only claim examples through a game-scoped idempotent MERGE.
- Seven available preseason captures truthfully produced zero claims. The zero-claim write/retry, controlled populated-path tests, and DAL–SEA post-ETL immutability check passed.
- Packet 3 has **Implementation GO**. Its first genuine claim-bearing write/retry and bounded-slate reconciliation remain a required pre-production operational validation, not a reason to manufacture evidence.
- Calibrated Matchup Lean is a separate completed release. Revision `nfl-games-app-main-00155-qaf` serves 100% traffic and `nfl-games-app-main-00153-jol` remains the rollback anchor.
- The live representative comparison changed only the qualifying confidence label from High to Medium; the pick, profile, raw signal, Model Outcome, and Model Trust remained unchanged.
- Production and candidate health returned HTTP 200, and the bounded post-promotion error review returned no rows.
- Released `main` was forward-merged into `dev`. The forward-merge build from `0b4d4ad93b6b617b0b6ea2871764353375e0e2c7` and the documentation build from `43e2f824860f69a9daeb4cda05fc8fb310f378ce` both succeeded.
- The temporary hotfix branch was deleted locally and from GitHub.
- The production 8:00 a.m. workflow is unchanged. No learning stage is wired into `app.py`, and no production learning tables or writes were introduced.
- Packet 4 inspection confirmed that the existing outcome, Level 2, and Level 3 calculations can be reused, but their current database wrappers are not capture-bounded or development-safe enough to call unchanged.
- Packet 4's first code slice adds a dry-only capture-aware grader that revalidates the frozen payload and reuses the existing Model Outcome/Trust calculations.
- Packet 4's inventory found all eight required sources, one DAL–SEA capture, two score rows, 130 Facts rows, zero claims, and one game-stage receipt. The 13-field development grade ledger is now verified empty. The dry runner projects through the existing outcome/trust builders without writing; twenty-three combined focused local tests pass.

---

## Packets versus learning levels

A **packet** is a bounded unit of delivery and proof. A **learning level** is a stage in the data-learning architecture.

Packet 3 implements **Level 1 claim extraction** from already captured pregame snapshots. Later packets can implement postgame outcomes, validation, aggregation, promotion, orchestration, and production release without changing Packet 2’s historical evidence.

---

## Verified breadcrumbs

- Packet 2 final closure: commit `50d83ec`
- Live-document status reconciliation: commit `14653a7`
- Packet 3 planning handoff and live index: commit `4906a03`.
- Packet 3 snapshot adapter first pass and cleanup: commits `28b52df` and `6d1b977`.
- Canonical claim-schema reuse and dev storage contract: commits `3fe24d3` and `fb6e997`.
- Game-scoped MERGE, per-game receipts, and one-capture runner: commits `45b3863`, `c264002`, and `3e82c6f`.
- Dry-run current/projected row clarity and zero-claim retry semantics: commits `7e61540` and `dad1806`.
- Packet 3 zero-claim inventory and replay documentation: commits `18377e6` and `8634148`.
- Packet 3 post-ETL snapshot proof: commit `3c8d5ab`.
- Calibrated Matchup Lean main merge: `175e1d0b79ca5d7a61d606cfe08133dd836a95ee`.
- Production build and serving revision: `896a7a28-e968-4d1d-a224-db5f1ede8d34`; `nfl-games-app-main-00155-qaf` at 100%.
- Calibrated Matchup Lean main-to-dev forward merge: `0b4d4ad93b6b617b0b6ea2871764353375e0e2c7`.
- Release-documentation receipt: `43e2f824860f69a9daeb4cda05fc8fb310f378ce`; both associated `dev` builds succeeded.
- Temporary hotfix branch cleanup: complete locally and on GitHub.
- Packet 4 Slice 1 documentation checkpoint: `0cd970e`.
- Packet 4 pure capture-aware grader and focused tests: `7aa623e`.
- Packet 4 read-only handoff inventory runner and tests: `6354f72`.
- Packet 4 explicit read-only launch correction: `474ff41`.
- Packet 4 capture-aware development grade storage boundary: `5373cd0`.
- Packet 4 read-only DAL–SEA dry grade runner: `3980310`.

The full attempt IDs, capture IDs, hashes, row counts, and replay proofs remain in the completed Packet 2 document. They are intentionally not duplicated in every file.

---

## Current next action

Continue [Packet 4](./GameLens_Packet_4_Postgame_Learning.md) from `dev`. Slice 1 inspection is complete and Calibrated Matchup Lean remains a separate shared rule:

1. confirm the repository build for `3980310`;
2. run the read-only DAL–SEA dry grade plan;
3. confirm one capture, two score rows, zero existing and one projected grade;
4. review the reused Model Outcome/Trust result and deterministic hashes; and
5. keep the first grade insert disabled until that evidence passes.

Do not modify `app.py`, create production learning tables, or merge the learning flow to `main` as part of Packet 4.

---

## Fresh-chat handoff prompt

> Work from branch `dev`. Begin with `documentation/live/README.md`, then read the Sprint, Packet 4 plan, completed Packet 3 evidence, Packet 2 evidence, Packet 1 contract, and architecture handoff in that order. Packets 1–3 are complete; Packet 3 has Implementation GO with its first genuine populated-capture write/retry carried as a pre-production operational validation because all seven available preseason captures contained zero claims. Review Packet 4 before code. Preserve DRY worker reuse, per-game/per-stage failure traceability, development-only writes, the production 8:00 a.m. load, `/game`, and the frontend. Do not manufacture claims to close the deferred Packet 3 proof. Calibrated Matchup Lean is already released and forward-merged; use its shared helper and do not reimplement it inside Packet 4 or Admin.

---

## Maintenance rule

When a packet closes:

1. update the Sprint’s status and next action;
2. convert the packet document from plan language to verified evidence;
3. update this index’s current status and stable truths;
4. add the next packet companion plan before implementation begins;
5. preserve historical records, but add explicit scope notes wherever old future-tense language could mislead a new reader.
