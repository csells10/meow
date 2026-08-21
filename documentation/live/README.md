# GameLens Live Documentation Index

**Current status:** Packets 1–5 have their documented Implementation GO boundaries. Packet 3's first genuine claim-bearing validation remains a pre-production operational gate because the available preseason captures contained zero claims. Packet 4 proved frozen grading plus Levels 2–3, retries, and partial-failure isolation. Packet 5 has backend Implementation GO: its canonical six-table inventory, protected development-only read route, week/game hierarchy, historical-gap lifecycle, selected-game evidence, friendly run labels, and Lovable wireframe handoff are accepted. The static Lovable wireframe is not yet wired to the endpoint. Packet 6 end-to-end development rehearsal is the next learning packet. Production behavior remains unchanged.

**Updated:** 2026-08-20
**Working branch:** `dev`  
**Production posture:** The existing 8:00 AM production load and learning wiring are unchanged. Calibrated Matchup Lean revision `nfl-games-app-main-00155-qaf` serves 100% of normal traffic, with `nfl-games-app-main-00153-jol` retained as rollback. No learning stage is wired into `app.py`, no learning tables have been created in production, and no learning-orchestration change is ready for `main`.

This file is the starting point for a new chat or a GitHub-assisted review. It separates current authority, completed evidence, historical context, and future work so older language is not mistaken for present status.

---

## Recommended reading order

1. [GameLens Learning Orchestration Product Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md) — current sprint status, packet order, gates, and next action.
2. [Packet 6 — End-to-End Development Rehearsal](./GameLens_Packet_6_End_to_End_Development_Rehearsal.md) — next packet plan; review before code.
3. [Packet 5 — Admin and Run Visibility](./GameLens_Packet_5_Admin_and_Run_Visibility.md) — completed backend evidence and accepted Lovable handoff.
4. [Packet 4 — Postgame Outcome plus Levels 2–3](./GameLens_Packet_4_Postgame_Learning.md) — completed implementation evidence inherited by Packet 6.
5. [Packet 3 — Production-Safe Level 1 Plan](./GameLens_Packet_3_Production_Level_1.md) — completed implementation evidence and deferred genuine-claim validation.
6. [Packet 2 — Shadow Pregame Snapshot Plan](./GameLens_Packet_2_Shadow_Pregame_Snapshot_Plan.md) — completed capture implementation inherited by Packet 6.
7. [Packet 1 — Pregame Capture Contract](./GameLens_Packet_1_Pregame_Capture_Contract.md) — the locked contract and invariants.
8. [Product Data Collection and Learning Handoff](./GameLens_Product_Data_Collection_and_Learning_Handoff.md) — architecture, level boundaries, and failure-trace contract.
9. [Development Dataset Recreation Runbook](./GameLens_Development_Dataset_Recreation_Runbook.md) — six-table inventory, schema owners, clean rebuild order, verification, and Packet 8 production-migration requirement.
10. [Runtime Configuration](./runtime_configuration.md) — deployment, environment, six-table runtime posture, and Packet 6 rehearsal boundaries.
11. [Go Plan](./go_plan.md) — historical production-cutover evidence plus the current learning go-day gate list. It is not the current learning-packet checklist.
12. [Calibrated Matchup Lean Hotfix](./GameLens_Calibrated_Matchup_Lean_Hotfix.md) — completed separate rule release and rollback evidence.

**Packet-numbering note:** older files under `documentation/August/` whose titles also say “Packet 4” describe the completed controlled 2025 ETL replay from August 1–2. They are historical evidence, not the authority for the completed current Packet 4 postgame-learning work.

---

## Document authority

| File | Role | Current state | Do not infer |
|---|---|---|---|
| [Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md) | Current execution authority | Packets 1–5 complete; Packet 6 development rehearsal is next | A future packet is implemented merely because it is described |
| [Packet 6](./GameLens_Packet_6_End_to_End_Development_Rehearsal.md) | Next implementation plan | Planning checkpoint; review before code | Approval for Level 4, production scheduling, or a long-running job |
| [Packet 5](./GameLens_Packet_5_Admin_and_Run_Visibility.md) | Completed backend and wireframe handoff | Backend Implementation GO; Lovable wireframe accepted; endpoint wiring remains separate | Approval for production learning or a duplicate summary warehouse |
| [Calibrated Matchup Lean](./GameLens_Calibrated_Matchup_Lean_Hotfix.md) | Completed separate release receipt | Live in production and forward-merged to dev | Packet 4 or Admin should recreate the rule |
| [Packet 4](./GameLens_Packet_4_Postgame_Learning.md) | Completed implementation evidence | Implementation GO; all Slice 6 proofs passed | Approval to wire postgame learning into production |
| [Packet 3](./GameLens_Packet_3_Production_Level_1.md) | Completed implementation evidence | Implementation GO; real populated dev validation carried forward | Approval to write production data or proof that genuine claim rows have been observed in cloud |
| [Packet 2](./GameLens_Packet_2_Shadow_Pregame_Snapshot_Plan.md) | Completed handoff evidence | GO, with exact parity and per-game observability proven in dev | The August 6 game has a recoverable pregame snapshot |
| [Packet 1](./GameLens_Packet_1_Pregame_Capture_Contract.md) | Locked behavioral contract | Complete | Its older point-in-time status overrides later Packet 2 evidence |
| [Architecture handoff](./GameLens_Product_Data_Collection_and_Learning_Handoff.md) | Stable system design and level boundaries | Active | Architecture prose is a live execution receipt |
| [Dataset recreation runbook](./GameLens_Development_Dataset_Recreation_Runbook.md) | Code-owned schema inventory and release prerequisite | Six development tables documented; production migration belongs to Packet 8 | Current dev-only scripts are approved for production |
| [Runtime configuration](./runtime_configuration.md) | Environment and deployment reference | Active; Packet 6 remains manual, bounded, and development-only | Every listed inventory value is timeless |
| [Go plan](./go_plan.md) | Historical release/cutover record plus current learning go posture | Gate H historical; Packet 5 backend GO; Packets 6–8 remain | Packet 1–5 Implementation GO equals production GO |

If two files appear to conflict, use this order: current Sprint status, current packet, completed packet evidence, Packet 1 contract, architecture handoff, then historical/reference documents.

---

## Stable truths as of 2026-08-20

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
- Packet 4's dry proof reused the frozen capture and existing outcome/trust builders: DAL won 17–7, the frozen no-pick remained `No Pick`, and Model Trust remained neutral. The deliberate first write inserted one row; the identical retry inserted zero, reported one unchanged, and performed no write.
- Packet 4's bounded Level 2 adapter reuses the existing validation/scoring functions, enforces capture/game identity and Facts readiness, preserves unavailable results, and makes zero claims an explicit no-op. The corrected preview plus both dev boundary attempts passed; 130 total accepted Facts reconcile to 94 Level 2-eligible actual rows.
- Packet 4's Level 3 preview keeps the existing feature formulas but strips every postgame target before calculation, blocks after Level 2 failure, and allows ordinary unavailable validations. The real DAL–SEA preview passed with zero claims/features/rejections/postgame fields admitted and no write.
- Packet 4's Level 3 storage boundary updates existing capture-scoped development claims only, refuses inserts and non-development use, protects previously populated formula evidence, and reconciles read-back. The dev-only setup added exactly the 15 missing nullable fields to the existing claim table; both real boundary attempts then passed with zero learning writes.
- Packet 4's thin coordinator calls the existing grade, Level 2, and Level 3 boundaries in order, preserves successful sibling games, stops downstream stages after failure, emits the visual funnel, and writes immutable attempt/game/stage audit rows to one dedicated development receipt table. Eighty-four focused tests pass.
- The one-game coordinator cloud proof passed for `packet4_coordinator_dal_sea_20260818`: four receipts inserted on the first run, four unchanged and zero inserted on the retry, with stable lineage and zero learning writes.
- Packet 4's three-game read-only inventory proved the real admission split: DET–CIN and DAL–SEA have canonical captures; CAR–ARI has final scores and Facts but no capture, so its older production outcome cannot admit it to Packet 4.
- Packet 4 QA commands emit JSON only to standard output. Git, Docker, and local Cloud Build contexts exclude generated JSON; temporary `packet4_*.json` files are deleted locally after final documentation closure.
- The healthy two-game first run inserted DET–CIN's missing grade and seven receipts while preserving DAL–SEA. Its retry changed zero learning rows but exposed an expected-effect receipt conflict; `8de1cf2` narrows immutable comparison so first-write versus unchanged-retry effects do not conflict while stable evidence still does.
- The corrected healthy retry matched seven unchanged receipts. The natural partial-failure first/retry preserved DAL–SEA, rejected missing-capture CAR–ARI, skipped its downstream stages, and reconciled `7 inserted → 7 unchanged` with zero learning writes.
- Packet 4 has **Implementation GO** with 86 local tests passing. Genuine claim-bearing development validation remains a pre-production gate because no real preseason capture contained claims.
- `GameLens_dev` contains six packet-owned tables: `pregame_snapshots`, `stage_runs`, `stage_game_results`, `claim_training_examples`, `game_model_outcomes`, and `postgame_learning_stage_receipts`.
- Empty development structure is reproducible from five fail-closed setup entry points in dependency order. The claim table is created at the Packet 3 base shape and then expanded by the Packet 4 Level 3 migration (`117 → 132`). Setup does not restore evidence rows.
- Packet 5 has backend **Implementation GO**. The protected read-only route derives its hierarchy from the six existing `GameLens_dev` tables plus the established production data owners; it did not add a summary warehouse.
- The accepted Lovable wireframe follows `Overview > Week > Game > Clock > Stage evidence`. Wiring that frontend to the protected endpoint remains a separate integration checkpoint.
- Packet 6 will rehearse Levels 1–3 through a small, bounded, rerunnable development coordinator. It will evaluate current eligibility, call only released workers, and report `waiting` or `no_op` when a stage is not ready instead of depending on razor-thin execution timing.
- Production table names, migration automation, IAM, retention, and activation remain Packet 8 decisions. Current setup scripts refuse non-development runtimes.

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
- Packet 4 deliberate development grade write/retry runner: `49b7ff6`.
- Packet 4 bounded read-only Level 2 adapter and QA runner: `1814052`.
- Packet 4 total-versus-eligible Facts count clarification: `6eb4a9d`.
- Packet 4 Level 2 development update/retry boundary: `14a86bc`.
- Packet 4 bounded leakage-safe Level 3 preview: `861bd57`.
- Packet 4 capture-scoped Level 3 development update/retry boundary: `5cd25d7`.
- Packet 4 Level 3 dev-only additive schema setup: `11378ba`.
- Packet 4 bounded coordinator and durable stage receipts: `9b21f88`.
- Packet 4 JSON build-context hygiene: `a985670`.
- Packet 4 first-write versus retry receipt reconciliation fix: `8de1cf2`.
- Packet 5 canonical data-shape and hierarchical service checkpoints: `47fd256`, `48d38fb`, and `22c7a6b`.
- Packet 5 protected route registration and visual HTTP checkpoint: `97e5f83`, `0f0267d`, and `18d991c`.
- Packet 5 week navigation and tests: `15e6c97`, `7ca0b83`, and `a4baa43`.
- Packet 5 historical-gap lifecycle and friendly run labels: `e3a54e5`, `952e710`, and `d7fa8df`.

The full attempt IDs, capture IDs, hashes, row counts, and replay proofs remain in the completed Packet 2 document. They are intentionally not duplicated in every file.

---

## Current next action

Packet 5 is closed at its backend and wireframe boundary. Wire the accepted
Lovable page to `GET /admin/gamelens/run-visibility` as a separate protected
frontend integration checkpoint without changing the endpoint contract.

Before Packet 6 code, review the
[end-to-end development rehearsal plan](./GameLens_Packet_6_End_to_End_Development_Rehearsal.md).
Its first slice is a read-only eligibility preview over a bounded slate. The
small coordinator should then reuse the proven Packet 2–4 workers, allow one
invocation to cover one or many games, stop cleanly after each invocation, and
remain safe to rerun. It must not require one process to survive from pregame
through postgame.

Do not modify the production 8:00 a.m. load, production learning wiring,
`/game`, or the public frontend. Do not implement Level 4 in Packet 6. Packet 7
owns Level 4; Packet 8 owns production migration, IAM, scheduling, activation,
and rollback.

---

## Fresh-chat handoff prompt

> Work from branch `dev`. Begin with `documentation/live/README.md`, then read the Sprint, Packet 6 plan, completed Packet 5 handoff, completed Packet 4 evidence, completed Packet 3 evidence, Packet 2 evidence, Packet 1 contract, architecture handoff, development dataset recreation runbook, runtime guide, and current learning-go section of `go_plan.md`. Packets 1–5 have their documented Implementation GO boundaries. Packet 3's first genuine claim-bearing validation remains a pre-production operational gate because the available preseason captures contained zero claims. Review `GameLens_Packet_6_End_to_End_Development_Rehearsal.md` before code. Build the smallest bounded, rerunnable, development-only coordinator that evaluates current eligibility and reuses the Packet 2–4 workers; one invocation may cover one or many games and may finish with `waiting` or `no_op`. Do not make one process wait across pregame and postgame, add a new summary warehouse, implement Level 4, reconstruct missed captures, manufacture claims, wire production learning, treat dev-only setup scripts as production migrations, change the production 8:00 a.m. load, alter `/game`, or reimplement the released Calibrated Matchup Lean rule. Packet 7 owns Level 4 and Packet 8 owns production release decisions.

---

## Maintenance rule

When a packet closes:

1. update the Sprint’s status and next action;
2. convert the packet document from plan language to verified evidence;
3. update this index’s current status and stable truths;
4. add the next packet companion plan before implementation begins;
5. preserve historical records, but add explicit scope notes wherever old future-tense language could mislead a new reader.
6. update the dataset recreation runbook in the same commit whenever a table, field migration, partition, clustering rule, or schema owner changes.
