# GameLens Live Documentation Index

**Current status:** Packets 1 and 2 are complete. Packet 3 implementation is in progress: the snapshot adapter and development claim-table contract are complete; claim MERGE, per-game receipts, and cloud proof remain.

**Updated:** 2026-08-15  
**Working branch:** `dev`  
**Production posture:** The existing 8:00 AM production load is unchanged. No learning stage is wired into `app.py`, no learning tables have been created in production, and no learning-orchestration change is ready for `main`.

This file is the starting point for a new chat or a GitHub-assisted review. It separates current authority, completed evidence, historical context, and future work so older language is not mistaken for present status.

---

## Recommended reading order

1. [GameLens Learning Orchestration Product Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md) — current sprint status, packet order, gates, and next action.
2. [Packet 3 — Production-Safe Level 1 Plan](./GameLens_Packet_3_Production_Level_1.md) — the active packet and its current implementation checkpoint.
3. [Packet 2 — Shadow Pregame Snapshot Plan](./GameLens_Packet_2_Shadow_Pregame_Snapshot_Plan.md) — completed implementation and cloud proof that Packet 3 inherits.
4. [Packet 1 — Pregame Capture Contract](./GameLens_Packet_1_Pregame_Capture_Contract.md) — the locked contract and invariants.
5. [Product Data Collection and Learning Handoff](./GameLens_Product_Data_Collection_and_Learning_Handoff.md) — architecture and level boundaries.
6. [Runtime Configuration](./runtime_configuration.md) — deployment and environment controls; use when a packet reaches cloud validation.
7. [Go Plan](./go_plan.md) — historical production-cutover evidence. It is not the current learning-packet plan.

---

## Document authority

| File | Role | Current state | Do not infer |
|---|---|---|---|
| [Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md) | Current execution authority | Packets 1–2 complete; Packet 3 in progress | A future packet is implemented merely because it is described |
| [Packet 3](./GameLens_Packet_3_Production_Level_1.md) | Active bounded implementation plan | Adapter and dev table/storage contract complete; MERGE, receipts, and cloud proof pending | Approval to write production data or wire the daily load |
| [Packet 2](./GameLens_Packet_2_Shadow_Pregame_Snapshot_Plan.md) | Completed handoff evidence | GO, with exact parity and per-game observability proven in dev | The August 6 game has a recoverable pregame snapshot |
| [Packet 1](./GameLens_Packet_1_Pregame_Capture_Contract.md) | Locked behavioral contract | Complete | Its older point-in-time status overrides later Packet 2 evidence |
| [Architecture handoff](./GameLens_Product_Data_Collection_and_Learning_Handoff.md) | Stable system design and level boundaries | Active | Architecture prose is a live execution receipt |
| [Runtime configuration](./runtime_configuration.md) | Environment and deployment reference | Active, with dated inventory sections | Every listed inventory value is timeless |
| [Go plan](./go_plan.md) | Historical release/cutover record | Retained for breadcrumbs | It is the current packet checklist |

If two files appear to conflict, use this order: current Sprint status, current packet, completed packet evidence, Packet 1 contract, architecture handoff, then historical/reference documents.

---

## Stable truths as of 2026-08-13

- Packet 2 produced six canonical rows in `GameLens_dev.pregame_snapshots` for the August 13 slate.
- All six saved payloads matched live `/game` exactly: six HTTP 200 responses, six exact hashes, and zero field differences.
- `GameLens_dev.stage_runs` contains the four known capture attempts.
- `GameLens_dev.stage_game_results` contains nine historical per-game results across those attempts. The first backfill inserted nine rows; an identical retry inserted zero and found all nine existing.
- Coverage from August 6 through August 13 is explicit: six games captured and `20260806_CAR@ARI` marked `capture_missing` with reason `before_packet_2_capture_program`.
- The August 6 missing capture must not be reconstructed after kickoff and must not be used as if it were genuine pregame evidence.
- Packet 3 must read canonical payloads from `pregame_snapshots`. The receipt tables are for audit, coverage, and diagnosis—not product-payload input.
- The production 8:00 AM workflow is unchanged. Packet 3 remains dev-only until its own tests and cloud gates pass.
- Merge to `main` and production scheduling remain later release work, not a reward for completing Packet 2.

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

The full attempt IDs, capture IDs, hashes, row counts, and replay proofs remain in the completed Packet 2 document. They are intentionally not duplicated in every file.

---

## Current next action

Continue [Packet 3](./GameLens_Packet_3_Production_Level_1.md) in small, reviewable commits:

1. ~~snapshot-to-claim adapter and capture-aware identity~~ complete;
2. ~~dev table schema and storage boundary~~ complete;
3. implement the game-scoped MERGE and per-game receipts;
4. run the focused and relevant regression tests; and
5. complete the gated dev cloud proof and documentation closure.

Do not start Packet 4, modify `app.py`, create production learning tables, or merge the learning flow to `main` as part of Packet 3.

---

## Fresh-chat handoff prompt

> Work from branch `dev`. Begin with `documentation/live/README.md`, then read the Sprint, Packet 3 plan, completed Packet 2 plan, Packet 1 contract, and architecture handoff in that order. Treat Packet 2 evidence as complete and Packet 3 as in progress. The snapshot adapter and dev claim-table contract are complete; game-scoped MERGE, per-game receipts, and cloud proof remain. Inspect the current code before editing. Keep production and the 8:00 AM load unchanged. Implement only the next approved Packet 3 slice, verify it, and update the live breadcrumbs before closing the packet.

---

## Maintenance rule

When a packet closes:

1. update the Sprint’s status and next action;
2. convert the packet document from plan language to verified evidence;
3. update this index’s current status and stable truths;
4. add the next packet companion plan before implementation begins;
5. preserve historical records, but add explicit scope notes wherever old future-tense language could mislead a new reader.
