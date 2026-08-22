# GameLens Learning Lite Salvage Matrix

**Status:** LL-1 review draft; no implementation code has been ported
**Prepared:** 2026-08-22
**Production baseline:** `main` at `b93c41c210288b9b4d450b4145e2d596e566aa67`
**Historical source:** `archive/dev-pre-learning-lite-20260821` at `26287205f420f569d81ccfcb28a8e8e0656fc24b`
**Implementation branch:** `learning-lite`
**Compared range:** `main...dev`, merge base `175e1d0b79ca5d7a61d606cfe08133dd836a95ee`

---

## 1. Decision summary

The archived development delta contains 92 changed files and 23,765 additions. It must not be merged wholesale.

The bounded Learning Lite salvage is:

1. **LL-2, minimal pregame contract:** selectively adapt the shared `/game` evidence builder, pregame-only entry point, deterministic identifiers, payload validation and hashing, immutable snapshot storage, and their focused tests.
2. **LL-3, Claim Extraction:** selectively adapt the existing claim extractor adapter, capture lineage, claim reconciliation, a narrow runner, and focused tests.
3. **Later postgame review:** retain the pure frozen-capture grading logic as a candidate only after the Week 1 capture path is stable.
4. **Archive only:** preserve Packet 1–6 plans, QA evidence, development recreation instructions, and historical proof scripts for reference.
5. **Do not port:** omit the broad coordinator, stage receipts, duplicate outcome ledger, backfill machinery, Admin/run-visibility backend, and Packet 5 route.

No row below authorizes a code change. `Port/adapt` means the named behavior may be reimplemented or selectively copied in a future checkpoint after Christian approves the bounded worklist.

---

## 2. Six-table development inventory

This is the archived Packet 4 shape documented at `2628720`; it has not been re-created or promoted by Learning Lite.

| Archived `GameLens_dev` table | Archived purpose | Learning Lite decision |
|---|---|---|
| `pregame_snapshots` | One immutable pregame capture | **Keep the concept; adapt to one minimal snapshot store.** |
| `stage_runs` | Attempt-level stage receipts | **Do not port.** Use concise logs unless repeated operating need proves otherwise. |
| `stage_game_results` | Per-game stage receipts | **Do not port.** Do not backfill this operational ledger. |
| `claim_training_examples` | Claims linked to a capture | **Reuse/adapt the existing claim-training owner; add only required snapshot lineage.** |
| `game_model_outcomes` | Duplicate frozen-capture grade ledger | **Do not port initially.** Existing outcome/validation owners remain authoritative. |
| `postgame_learning_stage_receipts` | Packet 4 coordinator receipts | **Do not port.** The general coordinator is deferred. |

The archived field counts and recreation order remain evidence, not the target schema. Before any future schema change, inspect the current production schema and approve the exact additive fields separately.

---

## 3. File-by-file source matrix

### 3.1 Runtime, query, service, and root tooling files

| File in archived `dev` | Decision | Bounded rationale / paired verification |
|---|---|---|
| `.dockerignore` | Port/adapt only when LL scripts exist | Preserve the intent to exclude local QA artifacts, but review broad `*.json` exclusions against current runtime assets first. |
| `.gcloudignore` | Port/adapt only when LL scripts exist | Preserve artifact hygiene, but do not copy exclusions blindly; verify current Cloud Build inputs. |
| `agg/gamelens_training/create_claim_training_examples_table.py` | Port/adapt in LL-3 | Expose a reusable canonical schema only if the existing claim table needs lineage verification. Pair with `tests/test_claim_training_schema.py`. |
| `app.py` | Do not port | Its archived delta only registers the deferred Packet 5 Admin route. |
| `audit_gamelens_snapshot_coverage.py` | Archive only | Useful historical operator evidence; Learning Lite can begin with a bounded query/log check instead of a permanent audit runner. |
| `backfill_gamelens_stage_game_results.py` | Do not port | Exists to repair the stage receipt ledger that Learning Lite explicitly omits. |
| `queries/game_queries.py` | Port/adapt in LL-2 | Select only the pure `build_team_metrics_from_rows` and `build_team_rankings_from_rows` seams needed for side-effect-free evidence. Rebase manually onto current `main`; never copy the whole historical file. |
| `queries/gamelens_snapshot_queries.py` | Port/adapt in LL-2 | Reduce `BigQuerySlateEvidenceLoader` to the minimum bounded slate/evidence queries needed for capture. Pair with `tests/queries/test_gamelens_snapshot_queries.py`. |
| `routes/admin_run_visibility_routes.py` | Do not port | Packet 5 Admin/run visibility is deferred. |
| `services/game_service.py` | Port/adapt in LL-2 | Select `GameDetailsEvidence`, `load_game_details_evidence`, `build_game_details_from_evidence`, and `get_pregame_game_details`; prove the live route remains equivalent and pregame mode cannot query final score or write outcomes. |
| `services/gamelens_admin_run_visibility_service.py` | Do not port | Deferred Packet 5 read service with no Week 1 dependency. |
| `services/gamelens_claim_storage.py` | Port/adapt in LL-3 | Reconcile capture-linked claims with the current claim-training owner; do not create a parallel claim platform. Pair with its storage and schema tests. |
| `services/gamelens_learning_contract.py` | Port/adapt in LL-2 | Salvage the pure deterministic ID, candidate decision, postgame-field rejection, canonical JSON hash, manifest, and canonical-capture rules. Defer postgame readiness helpers until needed. |
| `services/gamelens_level1_service.py` | Port/adapt in LL-3 | Reuse the existing extractor through a thin frozen-snapshot adapter. Pair with Level 1 and runner tests. |
| `services/gamelens_level2_storage.py` | Archive for later review | Not required for the Week 1 pregame path; reconcile with current Claim Grading ownership before any future use. |
| `services/gamelens_level2_validation.py` | Archive for later review | Its bounded validation seam may inform postgame work, but it is not part of LL-2 or LL-3. |
| `services/gamelens_level3_features.py` | Archive for later review | Existing Feature Enrichment owns calculations; inspect only after capture and Claim Extraction are stable. |
| `services/gamelens_level3_storage.py` | Archive for later review | Do not carry the 15-field development migration into the minimal capture path. |
| `services/gamelens_packet4_coordinator.py` | Do not port | General multi-stage coordinator is explicitly deferred. |
| `services/gamelens_packet4_receipts.py` | Do not port | Receipt table and merge logic exist only for the omitted coordinator. |
| `services/gamelens_postgame_grading.py` | Port/adapt later | Pure frozen-capture grading is a useful later candidate. Review only after Week 1 snapshot evidence is stable. |
| `services/gamelens_postgame_storage.py` | Do not port initially | Avoid a duplicate outcome ledger; reconcile with existing production validation/outcome owners first. |
| `services/gamelens_snapshot_capture.py` | Port/adapt in LL-2 | Shrink to bounded capture orchestration, payload verification, lens tags, immutable retry handling, and honest no-op results. Remove stage receipt and general coordinator concerns. |
| `services/gamelens_snapshot_coverage.py` | Archive only initially | Coverage logic is useful evidence, but a permanent auditor is not a Week 1 dependency. |
| `services/gamelens_snapshot_storage.py` | Port/adapt in LL-2 | Keep only `pregame_snapshot_schema` and immutable snapshot reconciliation. Omit `stage_run_schema`, `stage_game_result_schema`, and their writes. |
| `run_gamelens_level1_capture.py` | Port/adapt in LL-3 | Convert to a narrow manual Claim Extraction command over canonical snapshots; no coordinator. |
| `run_gamelens_packet4_coordinator.py` | Do not port | Entrypoint for the deferred coordinator. |
| `run_gamelens_packet4_grade_write.py` | Archive for later review | Historical reviewed grade runner; not required for pregame capture or initial Claim Extraction. |
| `run_gamelens_packet4_level2_write.py` | Archive for later review | Historical Claim Grading write wrapper; reconcile later with the current owner. |
| `run_gamelens_packet4_level3_write.py` | Archive for later review | Historical Feature Enrichment write wrapper; reconcile later with the current owner. |
| `setup_gamelens_claim_table.py` | Port/adapt only if LL-3 needs it | Prefer verifying/adapting the existing claim table over creating a new parallel table. |
| `setup_gamelens_level3_columns.py` | Do not port initially | The 15-column Packet 4 migration is outside the minimal pregame and Claim Extraction path. |
| `setup_gamelens_packet4_receipts.py` | Do not port | Creates the omitted Packet 4 receipt table. |
| `setup_gamelens_postgame_outcome_table.py` | Do not port initially | Creates the duplicate development outcome ledger. |
| `setup_gamelens_snapshot_tables.py` | Port/adapt in LL-2 | Rewrite as a fail-closed verifier/setup for one approved snapshot table only; no stage tables. |
| `qa_gamelens_packet4_dry_grade.py` | Archive only | Retain as historical Packet 4 evidence, not a Learning Lite operator path. |
| `qa_gamelens_packet4_level2.py` | Archive only | Historical Claim Grading QA; reconsider only with later postgame scope. |
| `qa_gamelens_packet4_level3.py` | Archive only | Historical Feature Enrichment QA; reconsider only with later postgame scope. |
| `qa_gamelens_packet4_schema_inventory.py` | Archive only | Useful record of the six-table prototype, not the new schema authority. |
| `qa_gamelens_packet5_admin_inventory.py` | Do not port | Supports deferred Admin/run visibility. |
| `qa_gamelens_packet5_admin_route.py` | Do not port | Supports deferred Admin route. |
| `qa_gamelens_packet5_admin_service.py` | Do not port | Supports deferred Admin service. |

### 3.2 Documentation files

| File in archived `dev` | Decision | Bounded rationale |
|---|---|---|
| `documentation/learning_light/GameLens_Learning_Light_Architecture.md` | Superseded | Renamed and adapted as `documentation/learning_lite/GameLens_Learning_Lite_Architecture.md`. |
| `documentation/learning_light/GameLens_Learning_Light_Sprint.md` | Superseded | Renamed and adapted as `documentation/learning_lite/GameLens_Learning_Lite_Sprint.md`. |
| `documentation/learning_light/README.md` | Superseded | Renamed and adapted as the current Learning Lite index and checkpoint authority. |
| `documentation/live/GameLens_Calibrated_Matchup_Lean_Hotfix.md` | Archive only | Historical release evidence; do not mix its checkpoint edits into the new branch. |
| `documentation/live/GameLens_Development_Dataset_Recreation_Runbook.md` | Archive only | Authoritative record of the six-table prototype, not a Learning Lite build instruction. |
| `documentation/live/GameLens_Learning_Orchestration_Product_Sprint.md` | Archive only | Superseded orchestration direction. |
| `documentation/live/GameLens_Packet_1_Pregame_Capture_Contract.md` | Archive only | Contract evidence and decision history; extract requirements through the current architecture. |
| `documentation/live/GameLens_Packet_2_Shadow_Pregame_Snapshot_Plan.md` | Archive only | Detailed prototype plan; use as evidence, never as an implementation checklist. |
| `documentation/live/GameLens_Packet_3_Production_Level_1.md` | Archive only | Level 1 proof and history; current LL-3 will define the bounded adapter. |
| `documentation/live/GameLens_Packet_4_Postgame_Learning.md` | Archive only | Postgame prototype evidence; outside the initial capture path. |
| `documentation/live/GameLens_Packet_5_Admin_and_Run_Visibility.md` | Archive only | Deferred product direction. |
| `documentation/live/GameLens_Packet_6_End_to_End_Development_Rehearsal.md` | Archive only | Explicitly superseded by Learning Lite. |
| `documentation/live/GameLens_Product_Data_Collection_and_Learning_Handoff.md` | Archive only | Historical handoff; current README owns the new handoff. |
| `documentation/live/README.md` | Archive only | Stable index remains available at the archived commit; it is intentionally not copied onto `learning-lite`. |
| `documentation/live/go_plan.md` | Archive only | Historical packet execution plan, not current authority. |
| `documentation/live/runtime_configuration.md` | Archive only | Preserve environment-safety evidence; re-document only settings actually selected by future Learning Lite code. |

---

## 4. File-by-file test matrix

Tests travel only with an approved behavior. Tests for omitted infrastructure remain archive evidence and must not pull that infrastructure onto `learning-lite` by dependency.

| Test file in archived `dev` | Decision | Behavior it verifies |
|---|---|---|
| `tests/_gcp_stubs.py` | Port/adapt as needed | Minimal local Google Cloud import stubs for approved focused tests only. |
| `tests/queries/test_gamelens_snapshot_queries.py` | Port/adapt in LL-2 | Bounded slate selection and pregame evidence loading. |
| `tests/services/test_game_service_pregame_capture.py` | Port/adapt in LL-2 | Live/pregame parity, no final-score query, and no completed-game write. |
| `tests/services/test_gamelens_admin_run_visibility_service.py` | Do not port | Deferred Admin service. |
| `tests/services/test_gamelens_claim_storage.py` | Port/adapt in LL-3 | Capture-linked claim schema and idempotent reconciliation. |
| `tests/services/test_gamelens_learning_contract.py` | Port/adapt in LL-2 | Deterministic IDs, hash stability, postgame rejection, and canonical decision rules. |
| `tests/services/test_gamelens_level1_service.py` | Port/adapt in LL-3 | Frozen snapshot validation and existing extractor adaptation. |
| `tests/services/test_gamelens_level2_storage.py` | Archive for later review | Development Claim Grading storage semantics. |
| `tests/services/test_gamelens_level2_validation.py` | Archive for later review | Bounded Claim Grading wrapper. |
| `tests/services/test_gamelens_level3_features.py` | Archive for later review | Pregame-only feature projection. |
| `tests/services/test_gamelens_level3_storage.py` | Archive for later review | Packet 4 Level 3 schema/write semantics. |
| `tests/services/test_gamelens_packet4_coordinator.py` | Do not port | Deferred general coordinator. |
| `tests/services/test_gamelens_packet4_receipts.py` | Do not port | Omitted receipt ledger. |
| `tests/services/test_gamelens_postgame_grading.py` | Port/adapt later | Frozen-capture grading candidate after Week 1 stability. |
| `tests/services/test_gamelens_postgame_storage.py` | Do not port initially | Duplicate outcome storage. |
| `tests/services/test_gamelens_snapshot_capture.py` | Port/adapt in LL-2 | Capture safety, verification, retry, identity, and honest skip/failure behavior; remove receipt-only cases. |
| `tests/services/test_gamelens_snapshot_coverage.py` | Archive only initially | Coverage classification/auditor behavior. |
| `tests/services/test_gamelens_snapshot_handoff.py` | Port/adapt in LL-2 | End-to-end pregame safety boundary without production writes. |
| `tests/services/test_gamelens_snapshot_storage.py` | Port/adapt in LL-2 | Immutable snapshot reconciliation only; remove stage table cases. |
| `tests/services/test_gamelens_stage_game_backfill.py` | Do not port | Backfill for omitted stage receipts. |
| `tests/test_admin_run_visibility_routes.py` | Do not port | Deferred Admin route. |
| `tests/test_claim_training_schema.py` | Port/adapt in LL-3 | Canonical claim schema exposure and required capture lineage. |
| `tests/test_qa_gamelens_packet4_dry_grade.py` | Archive only | Historical Packet 4 dry-grade CLI. |
| `tests/test_qa_gamelens_packet4_level2.py` | Archive only | Historical Packet 4 Level 2 QA. |
| `tests/test_qa_gamelens_packet4_level3.py` | Archive only | Historical Packet 4 Level 3 QA. |
| `tests/test_qa_gamelens_packet4_schema_inventory.py` | Archive only | Six-table development inventory. |
| `tests/test_qa_gamelens_packet5_admin_inventory.py` | Do not port | Deferred Packet 5 inventory. |
| `tests/test_run_gamelens_level1_capture.py` | Port/adapt in LL-3 | Narrow manual Claim Extraction runner. |
| `tests/test_run_gamelens_packet4_coordinator.py` | Do not port | Deferred coordinator entrypoint. |
| `tests/test_run_gamelens_packet4_grade_write.py` | Archive for later review | Historical reviewed grade write runner. |
| `tests/test_run_gamelens_packet4_level2_write.py` | Archive for later review | Historical Level 2 write runner. |
| `tests/test_run_gamelens_packet4_level3_write.py` | Archive for later review | Historical Level 3 write runner. |
| `tests/test_setup_gamelens_level3_columns.py` | Do not port initially | Deferred 15-column migration. |
| `tests/test_setup_gamelens_packet4_receipts.py` | Do not port | Omitted receipt table setup. |

All 92 files in the archived `main...dev` comparison are represented in Sections 3 and 4.

---

## 5. Proposed bounded checkpoint worklists

### LL-2 candidate set — no schema approval implied

- selective seams from `queries/game_queries.py`;
- reduced evidence loader from `queries/gamelens_snapshot_queries.py`;
- pregame-safe shared builder seams from `services/game_service.py`;
- pure capture rules from `services/gamelens_learning_contract.py`;
- reduced capture coordinator from `services/gamelens_snapshot_capture.py`;
- snapshot-only storage from `services/gamelens_snapshot_storage.py`;
- snapshot-only setup/verifier adapted from `setup_gamelens_snapshot_tables.py`;
- the focused LL-2 tests named in Section 4.

### LL-3 candidate set — separate approval after LL-2 proof

- schema exposure from `agg/gamelens_training/create_claim_training_examples_table.py`;
- capture-linked claim reconciliation from `services/gamelens_claim_storage.py`;
- frozen-snapshot adapter from `services/gamelens_level1_service.py`;
- narrow manual runner adapted from `run_gamelens_level1_capture.py`;
- the focused LL-3 tests named in Section 4.

### Explicitly excluded from LL-2 and LL-3

- every Packet 4 coordinator and receipt file;
- every Packet 5 Admin/run-visibility file;
- stage run/result storage and backfill;
- postgame outcome table/storage;
- Level 2 and Level 3 write wrappers and schema migration;
- production deployment, traffic changes, data migration, or backfill.

---

## 6. Review gates before implementation

Christian remains the decision maker for code and schema changes. Before LL-2 begins:

1. approve or revise the LL-2 candidate set;
2. inspect the current production snapshot/claim schema situation read-only;
3. decide the snapshot table name and exact additive schema separately;
4. confirm the manual fallback and kill-switch behavior;
5. approve the focused test list;
6. create a new bounded implementation checkpoint—without merging archived `dev`.

Until those gates are satisfied, this matrix is documentation and planning evidence only.
