# Learning Lite: LL-4 through LL-10 reassessment

**Review date:** 2026-09-30  
**Repository / branch:** `csells10/meow` / `learning-lite`  
**Reviewed branch head:** `c8e7992e468166078dddad8c31175e992ea74cf2`  
**Verified main head:** `41eea7af08b51c364c6b575fc2f84ef4b1f28ea0`  
**Status:** Proposed re-plan for review; no implementation or production activation authorized by this document.  
**Scope:** Repository documentation and source review. This review did not query BigQuery, inspect deployed configuration, run workers, rerun tests, or change production behavior.

## Decision in brief

Keep LL-0 through LL-3 accepted at their documented scope. LL-3 is complete **for its bounded development proof**. It is not a production release, a slate proof, or a Claim Extraction proof.

Keep the lean architecture and existing calculation owners. Replace the expired calendar with evidence-based gates:

**LL-4 readiness review → LL-4 development Claim Extraction → LL-5 in-season development rehearsal → LL-6 activation readiness and rollback → LL-7 first prospective operating cohort → LL-8 manual postgame cycle → LL-10 advisory calibration when the sample warrants it.**

LL-9 is a separate read-only data-readiness track. It can be reviewed earlier without becoming a dependency for LL-4 or production capture. A bounded LL-8 development rehearsal can also precede production activation once LL-4 is accepted and the captured game's final evidence is available.

The immediate next step is a small LL-4 file/schema/test proposal, including a fresh decision on LL-FIND-001. Do not reopen LL-3 merely because later operating requirements are unfinished.

## Navigation

- [Established baseline](#established-baseline)
- [Findings that change the remaining plan](#findings-that-change-the-remaining-plan)
- [Revised sequence and exit gates](#revised-sequence-and-exit-gates)
- [What to review before LL-4 implementation](#what-to-review-before-ll-4-implementation)
- [Open decisions and recommended defaults](#open-decisions-and-recommended-defaults)
- [Documentation reconciliation](#documentation-reconciliation)
- [Evidence and source references](#evidence-and-source-references)

## Established baseline

The README stage map and September 30 closure take precedence over older dated “next checkpoint” instructions.

| Area | Established result | Boundary still outstanding |
|---|---|---|
| LL-0 / LL-1 | Direction, branch roles, archived development preservation, and salvage baseline accepted | Archived files remain candidates, not an implementation checklist |
| LL-2 | Shared pregame builder, deterministic identity/hash, postgame rejection, and side-effect isolation accepted | Does not establish operating readiness |
| LL-3 | One genuinely upcoming game's immutable development snapshot, read-back, identical retry, conflict protection, and guarded recovery accepted | No production target, production invocation, slate capture, claim rows, or schedule |
| Matchup Lens | Separate released endpoint brought onto `learning-lite` through the main reconciliation | Its frozen contract governs; the old M1 proposal is not new work to repeat |
| Claim-learning owners | Existing extraction, validation, enrichment, and calibration workers are present | A canonical-snapshot LL-4 adapter and its bounded storage contract are not present on the reviewed branch |
| Findings | LL-FIND-002 resolved for the proved JSON/hash issue | LL-FIND-001 remains Open; its one-proof waiver expired |

### LL-3 evidence to carry forward

The closure records:

- Snapshot: `capture_62ecbfb862ef9f3ed05a5626`, game `20261001_PIT@CLE`.
- Capture: `2026-09-30T13:20:59.094384Z`; scheduled kickoff: `2026-10-02T00:15:00Z`.
- Payload SHA-256: `f7b37766f17aedd41b52293ed829241c355ca7600654c314eac81777fb5e804b`.
- Destination: existing `nfl-stream-406420.GameLens_dev.pregame_snapshots`.
- First write/read-back passed; identical retry preserved the first timestamp and made no material change.
- A conflicting in-memory candidate was rejected through a read-only client. This was not a second attempted BigQuery write or a concurrent-writer proof.
- Final reconciliation: eight unique rows, including seven other historical rows; the one failed August artifact was exported and removed by the guarded recovery.
- Reported local gates: 27 LL-2/product tests and 32 LL-3/recovery tests passed.

These are documented proof results, not cloud checks repeated during this review. The 19 float/int inspection differences are expected representation diagnostics under the corrected hash rule, not unfinished LL-3 work. The valid snapshot must remain immutable.

## Findings that change the remaining plan

### 1. The missed dates change the operating cohort, not the safety requirements

The August rehearsal window and September 9 Week 1 safety target have passed. LL-5 cannot now be a live preseason rehearsal, and LL-7 cannot deliver missing Week 1 pregame captures.

Replace those targets with the first upcoming, explicitly selected regular-season slate **after readiness gates pass**. Do not force a Week 4 deadline because PIT–CLE supplied the LL-3 proof. Games missed before activation remain missing. Historical reconstructions may support separately labeled research, but cannot become contemporaneous pregame evidence.

Reading an authentic frozen snapshot and extracting claims after kickoff is different from capturing a new payload after kickoff. LL-4 may process the former using the original capture time; it must never do the latter. Extraction time must remain separate from capture time.

### 2. LL-4 has reusable calculations, but no approved persistence boundary

The current `build_claim_training_examples.py` already extracts six supported claim types and supplies source paths. Its existing keys identify claims by game and claim attributes, without capture identity. The standalone batch writer appends and can delete rows by `run_id` through `--replace-run`. Those are not the desired immutable, capture-scoped retry semantics.

The source schema includes the legacy claim fields, but does not itself establish the current live table schema or approved capture-lineage additions. The current production claim table and archived development claim table must be inspected read-only before any schema decision.

**Implication:** Reuse `extract_claim_rows` and the existing calculation owner. Add a thin adapter, canonical lineage, and a narrowly scoped reconciliation boundary. Do not use the legacy batch writer as the LL-4 write path.

### 3. The archived adapter is useful evidence, not a drop-in component

At archived `dev@26287205f420f569d81ccfcb28a8e8e0656fc24b`:

- `gamelens_level1_service.py` validates snapshots, reuses the extractor, attaches lineage, replaces legacy keys with capture-aware keys, and nulls postgame fields.
- Its orchestration also writes `stage_game_results` receipts, including for zero-claim results.
- Its hash/validation imports point to the broader archived learning contract, not the corrected current pregame contract.
- Archived claim storage supplies useful immutable-conflict and read-back reconciliation logic, but also uses temporary staging-table creation/deletion.
- Archived tests mix valuable safety assertions with receipt and setup expectations.

**Implication:** Select functions and tests deliberately. Remove receipt dependencies. Reuse the current storage-stable hash contract. Explicitly decide whether staging is necessary; its table operations must not arrive unnoticed through a wholesale port.

### 4. LL-FIND-001 is a data-quality decision, not a snapshot bug

The four snap-total metrics can preserve missing values as zeroes and receive misleading rankings. The documented issue is bounded away from Matchup Lean and confidence, but can affect rank/tier/context and some edge language.

The expired waiver explicitly excluded later-checkpoint use. It does not carry over to LL-4 simply because the snapshot was validly stored.

**Recommended treatment:** controlled synthetic fixtures can support LL-4 mechanics. Before using the real PIT–CLE snapshot as accepted LL-4 proof, document a new scoped decision or resolution. A development-only waiver must identify permitted use and exclude those results from learning conclusions. Before production capture, prefer correction at the first trustworthy ownership boundary and the finding's full reconciliation criteria; any alternative requires a separate explicit decision.

Never “clean” the frozen snapshot to hide the issue.

### 5. Single-game safety is not slate or operational readiness

The current runner and capture service enforce development-only operation and accept one game. They do not establish bounded slate selection, upstream pipeline health, scheduling, a production kill switch, or concurrent-write safety.

A supplied metric pipeline run ID is lineage, not proof that Facts, windows, and rankings formed a healthy, consistent upstream state.

**Implication:** LL-5 needs a small bounded-slate proof; LL-6 needs explicit upstream readiness, destination, invocation, overlap prevention, and rollback decisions. Keep the first implementation manual and serial unless actual use justifies more.

### 6. The frozen /game response does not automatically capture the separate lens-context endpoint

The released `/game/<id>/lens-context` is a separate read-only contract. LL-3 freezes the pregame `/game` payload and evidence context. This does not prove preservation of every separate endpoint response or frontend-derived six-lens statement.

**Recommended LL-4 scope:** supported claims already present in the frozen `/game` payload. If learning about six-lens frontend output becomes a requirement, define its point-in-time evidence and versioning as a separate checkpoint. Do not rebuild M1 or reconstruct that output from current rankings.

### 7. League Discovery and calibration have different evidence dependencies

LL-9 primarily depends on rankings, dates, windows, tags, and history quality. It does not need claim writes or a production Learning Lite rollout.

LL-10 needs eligible graded/enriched claims and meaningful sample coverage. The current calibration worker has a provisional `MIN_SAMPLE_ROWS = 30`; thirty correlated claims from one or two games are not thirty independent observations.

The weekly movement research note also records that ranking builds recompute historical as-of dates with table replacement. Historical dates in the current table are not automatically immutable “what was known then” evidence.

**Implication:** Bring forward narrow LL-9 feasibility review if useful. Keep LL-10 sample-driven, with game/week coverage, version compatibility, and unavailable-label handling explicitly reviewed.

## Revised sequence and exit gates

All gates below are proposals. Checkpoint acceptance must be recorded with exact source commit, files, commands, observed results, data effects, limitations, and recovery point.

| Stage | Revised scope and dependencies | Proposed exit gate |
|---|---|---|
| **LL-4 readiness review** | Read-only review of current extractor, schemas, archived candidates, and canonical snapshot contract; decide LL-FIND-001 scope | Exact development source/target, schema delta or no-change decision, identity/version policy, file list, focused tests, and proof procedure reviewed before implementation |
| **LL-4 — Development Claim Extraction** | One canonical snapshot through the existing extractor and a thin adapter; dry-run default; development-only reconciliation | Controlled claim-bearing and zero-claim fixtures pass; accepted real-snapshot proof yields reproducible claims or explicit zero result; paths resolve; lineage/hash agree; new rows have null postgame targets; identical retry makes no change; conflicts and invalid snapshots fail closed; read-back reconciles; no production writes |
| **LL-5 — In-season development rehearsal** | Replace expired preseason rehearsal with a small named upcoming regular-season slate, only after LL-4 and applicable data-quality decisions | Every selected game has an explicit disposition; captures are genuinely pre-kickoff; same-evidence product parity holds; existing captures are preserved; one game can fail without corrupting siblings; zero claims, missing context, empty slate, schedule changes, and retries are exercised; runtime/query cost and operator effort recorded |
| **LL-6 — Invocation and activation readiness** | Choose production destination and smallest invocation after a known-safe upstream state; preserve manual fallback; design independent disable control | Development shadow invocation and disable/rollback rehearsal pass; failed/mixed upstream state blocks new capture; destinations/IAM/schema and serialized invocation are reviewed; product regression evidence passes; release plan names exact source and recovery point. Production deployment/write proof, if needed, is a separately authorized sub-gate |
| **LL-7 — First prospective operating cohort** | Replace Week 1 with the first named upcoming production cohort after LL-6 and explicit activation | A representative first game and bounded slate are reconciled before kickoff; every candidate is captured, skipped with reason, or failed with evidence; capture success is distinguished from extraction failure; retries preserve history; application remains healthy; no historical gaps are backfilled as captures |
| **LL-8 — First manual postgame cycle** | Existing Claim Grading and Feature Enrichment owners; production cohort after LL-7, or a separately approved development rehearsal after LL-4 | Authentic capture and final-score readiness checked; accepted completed-game Facts gate claim grading; missing evidence stays unavailable; feature inputs are explicitly pregame-only; read-back joins to capture/claim/version; retry preserves source evidence and prior valid results; one unavailable game does not block siblings; no runtime promotion |
| **LL-9 — League Discovery readiness** | Optional read-only track; existing 2025/2026 rankings, windows, tags, and movement semantics | One reproducible query/report proves intended date/window coverage, source lag, tag filtering, rank direction, sample counts, and honest missing history; distinguishes team-value movement from relative-rank movement and recomputed history from immutable history; no API/UI required |
| **LL-10 — Advisory Language Calibration** | After a reviewed sample of compatible, graded and enriched claims exists; not “Week 2” by calendar | Versioned report includes claim surface, feature definition, row and distinct-game counts, week coverage, available/unavailable denominators, baseline, validation rate, uncertainty/limitations, and recommendation. Insufficient evidence is a valid result. Any runtime promotion remains a separate review |

### Operating details that should not be lost in the re-plan

- **First valid capture wins:** decide a useful capture window before operation. Re-running every morning must not create a new cohort merely to obtain a newer payload.
- **Already captured games:** read and reuse the stored record. A later live payload changing is not permission to replace history. LL-3's candidate inspector rebuilds from current evidence; LL-4 should read by capture ID instead.
- **Schedule changes:** current identity includes kickoff. Review rescheduling/postponement policy before slate operation so a kickoff change cannot silently produce two competing canonical records.
- **Retry after kickoff:** existing pre-kickoff evidence can support extraction retry; an uncaptured game cannot be rescued by backdating.
- **Coverage:** logs and a bounded query/summary should distinguish capture success, extraction pending/failed, zero claims, skip, and failure. Do not invent claim rows or a permanent receipt table to represent zero.
- **Rollback:** disable new learning invocation and preserve evidence. Deleting valid snapshots or turning off the existing metric pipeline is not rollback.
- **Manual LL-8 development proof:** useful for checking the whole evidence chain early, but does not satisfy LL-7's production-operation gate or imply a useful calibration sample.

## What to review before LL-4 implementation

### A. Source and schema inventory

Perform read-only inspection of:

1. The selected canonical development snapshot, by its recorded capture ID and hash; retain the original timestamps and cohort.
2. `Analytics.gamelens_claim_training_examples`: actual field names, types, modes, partitions, clustering, consumer expectations, row cohorts, and duplicate-key conventions.
3. Archived `GameLens_dev.claim_training_examples`, if it still exists: current schema and rows. Its historical existence is not a current schema guarantee.
4. Existing extraction/validation/enrichment/calibration consumers: their `run_id`, `claim_key`, table-selection, and update behavior.

Prepare an exact additive-field proposal only after this inventory. Candidate lineage concepts are `capture_id`, `learning_run_id`, `source_payload_sha256`, `extraction_version`, `extracted_at`, and upstream run identity when available. Some may already exist in the chosen table.

Legacy rows cannot truthfully acquire missing snapshot lineage. If a later production in-place extension is selected, nullable legacy fields plus strict validation for new LL rows may be appropriate; do not guess a required-field migration or rewrite old records now.

### B. Extraction contract

Review a small input/output example for each supported claim type:

| Existing claim type | Frozen source path |
|---|---|
| `game_profile` | `game_profile[i]` |
| `core_area_comparison` | `core_area_comparison[i]` |
| `core_area_summary` | `matchup_breakdown.core_area_summaries[i]` |
| `category_summary` | `matchup_breakdown.category_summaries[i]` |
| `metric_highlight` | `matchup_breakdown.metric_highlights[i]` |
| `team_comparison_metric` | `team_comparison[i]` |

Confirm that each emitted row's path resolves to the correct original claim, rather than merely being a nonempty string. Distinguish supported absence from malformed input. Keep current headline selection and football calculations unless separately scoped.

The current context builder can read final scores/model outcomes and emits `final_margin_bucket = "unknown"` when scores are absent. LL-4 therefore needs both input rejection and an explicit output target-null contract, including this non-null placeholder. A pregame claim row must not inherit postgame fields simply because the historical extractor supports completed-game QA.

Use the current canonical numeric normalization for hash checks. Validate stored capture eligibility against the stored capture timestamp, not extraction wall-clock time.

### C. Identity, versioning, and reconciliation

Decide explicitly:

- Stable cohort versus per-attempt ID: attempts must not create new training cohorts.
- Capture-scoped key construction: the archive's capture + source path + type + rank scheme is a useful candidate.
- Extraction version and configuration identity, including `headline_top_metrics`.
- Behavior when extraction rules change: fail on incompatible replay by default; evaluate a separately versioned result only under a later approved policy.
- Immutable extracted fields versus fields owned by later grading/enrichment.
- Preservation of first-write audit values on retry, and preservation of later labels/features.
- Partial-write recovery and duplicate detection before and after reconciliation.
- Serial execution initially; no exactly-once/concurrency claim based solely on sequential no-op proof.

Do not let an extraction retry null out already graded targets. “Null targets at extraction” applies to newly prepared/inserted rows; existing later-stage fields must survive reconciliation.

### D. Candidate file and test boundary

| Candidate | Proposed treatment |
|---|---|
| `agg/gamelens_training/build_claim_training_examples.py` | Reuse pure extraction functions; protect legacy behavior; avoid its batch writer |
| `agg/gamelens_training/create_claim_training_examples_table.py` | Expose a reusable schema only if necessary; no automatic production table creation |
| `services/gamelens_level1_service.py` | Adapt the archived pure preparation seam to the current snapshot/hash contract; omit receipt orchestration |
| `services/gamelens_claim_storage.py` | Adapt only reviewed development reconciliation and read-back behavior |
| `run_gamelens_level1_capture.py` | Narrow command taking an explicit capture ID; development guard; dry-run default; explicit write mode |
| Current pregame contract and snapshot storage | Reuse; any new helper must be separately justified within the bounded file list |
| Setup helper | Only if live schema inventory proves it necessary and its exact operations are reviewed |

Focused tests should cover:

- Valid claim-bearing and valid zero-claim snapshots.
- Missing capture; mismatched identity, hash, environment, status, or capture timing.
- Postgame payload rejection and all new-row target fields null.
- Stable keys and source-path resolution for supported surfaces.
- Duplicate input/stored keys; conflicting values; changed extraction version/configuration.
- Identical replay, partial-write recovery, and read-back count/content reconciliation.
- Preservation of first audit values and later validation/features.
- Dry-run and zero-claim paths performing no claim mutation.
- Development-only writes and no stage receipts/coordinator dependencies.
- Existing pregame/live product parity and protected claim-language behavior.

Adapt the salvage-matrix tests for service, storage, schema, and runner. Do not import receipt-specific test expectations. Report actual test results only after the future implementation runs them.

### E. LL-4 proof package

Before the first accepted real-data LL-4 write, have a reviewable package containing:

- Exact source commit and bounded changed-file list.
- Fresh schema inventory and approved development target.
- LL-FIND-001 resolution or expressly scoped later-checkpoint decision.
- Dry-run row counts, representative source-path examples, key uniqueness, target-null checks, and source hash.
- Exact write scope and expected read-back/retry/conflict results.
- Recovery procedure for a failed claim write without deleting valid snapshots or unrelated claims.
- Explicit statement that no production claim schema/write, route, scheduling, deployment, grading, or calibration is included.

## Open decisions and recommended defaults

| Decision | Recommended default | Needed by |
|---|---|---|
| LL-FIND-001 treatment | Correct at the first trustworthy owner; if a development-only waiver is chosen, define scope, exclusion from conclusions, and expiry | Real-snapshot LL-4 proof; production separately |
| LL-4 development target | Verify and reuse the existing development claim table if compatible; preserve historical rows | LL-4 implementation |
| Claim identity/version policy | Stable capture-scoped identity; reject incompatible re-extraction rather than overwrite | LL-4 implementation |
| Claim write mechanism | Smallest game/capture-scoped insert/reconcile path; no legacy replace-run; review any temporary staging | LL-4 implementation |
| Zero-claim observability | Structured summary/log with capture/hash/extraction version; retained checkpoint evidence | LL-4 |
| Production snapshot destination | Choose explicitly after inspecting access and consumers; do not promote or relabel development rows | LL-6 |
| Production claim-table evolution | Prefer compatible reuse of the existing owner; successor only if schema/consumer evidence requires it | LL-6 |
| Upstream readiness | Positive evidence of an accepted consistent metric state, not just a clock or supplied run ID | LL-5 design / LL-6 activation |
| Capture window and first cohort | Named upcoming regular-season slate after gates; first valid capture remains canonical | LL-5 / LL-7 |
| Invocation and overlap | Manual, serial, bounded invocation first; automation later if operator experience warrants it | LL-6 |
| Rescheduling | Explicit canonical identity/disposition policy; never silent duplication or replacement | LL-5 |
| Separate six-lens learning | Exclude from LL-4; define a separate frozen evidence contract if desired | Future scope decision |
| LL-9 timing | Optional narrow read-only movement/history proof; no dependency on LL-4 | When useful |
| LL-10 sufficiency | Review distinct games/weeks, surface coverage, denominators, and baseline in addition to the existing provisional 30-row threshold | Before calibration interpretation |
| Future algorithm target | Remain deferred; not a blocker for evidence collection | Separate research proposal |

These are proposed defaults, not decisions silently adopted by this review.

## Documentation reconciliation

This report intentionally preserves the existing dated checkpoint record. If the re-plan is accepted, make a bounded documentation follow-up:

1. Add a dated README stage map linking this review, retaining LL-3's development-only closure.
2. Replace future-facing preseason/Week 1 wording in Sprint outcomes, scope tiers, LL-5 through LL-8, stop conditions, and definition of done with prospective-cohort wording. Retain historical dates as history.
3. Reframe Architecture's Week 1 operating section as the initial production operating boundary, without changing its safety rules.
4. Refresh Salvage Matrix checkpoint annotations where older candidate placement differs from implementation: LL-2 used the small pregame contract; one-game snapshot queries/storage landed in LL-3; no snapshot setup path was required.
5. Record the eventual LL-FIND-001 decision append-only in the Findings Log. Do not mark it resolved merely because this review recommends a path.
6. Keep the frozen New API contract and release handoffs authoritative for Matchup Lens; do not revive historical M1 as a prerequisite.

No accepted checkpoint needs to be renumbered. No permanent coordinator, stage ledger, duplicate outcome table, Admin UI, public League Discovery surface, or automatic calibration promotion is needed to execute this proposed sequence.

## Evidence and source references

All repository conclusions above refer to the inspected revision, not an assertion about uninspected live data. Main was checked at the SHA listed at the top. The README was read first, including its current stage map and September 30 closure.

### Current planning authority

- [README and LL-3 closure](https://github.com/csells10/meow/blob/c8e7992e468166078dddad8c31175e992ea74cf2/documentation/learning_lite/README.md)
- [Sprint](https://github.com/csells10/meow/blob/c8e7992e468166078dddad8c31175e992ea74cf2/documentation/learning_lite/GameLens_Learning_Lite_Sprint.md)
- [Architecture](https://github.com/csells10/meow/blob/c8e7992e468166078dddad8c31175e992ea74cf2/documentation/learning_lite/GameLens_Learning_Lite_Architecture.md)
- [Findings Log](https://github.com/csells10/meow/blob/c8e7992e468166078dddad8c31175e992ea74cf2/documentation/learning_lite/GameLens_Learning_Lite_Findings_Log.md)
- [Salvage Matrix: claim schema/storage/adapter/runner, focused tests, and exclusions](https://github.com/csells10/meow/blob/c8e7992e468166078dddad8c31175e992ea74cf2/documentation/learning_lite/GameLens_Learning_Lite_Salvage_Matrix.md)

### Current implementation evidence

- [Existing extraction owner](../../agg/gamelens_training/build_claim_training_examples.py): `make_claim_key`, `extract_payload_context`, `extract_claim_rows`, source-path builders, and `load_to_bigquery`.
- [Claim schema source](../../agg/gamelens_training/create_claim_training_examples_table.py): legacy schema and non-enforced key uniqueness.
- [Snapshot capture](../../services/gamelens_snapshot_capture.py), [storage](../../services/gamelens_snapshot_storage.py), [one-game loader](../../queries/gamelens_snapshot_queries.py), and [manual runner](../../run_gamelens_snapshot_capture.py): current development-only boundary.
- [Claim Grading owner](../../agg/gamelens_training/update_claim_training_validation.py), [Feature Enrichment owner](../../agg/gamelens_training/update_claim_training_features.py), and [Language Calibration owner](../../agg/gamelens_training/build_claim_language_calibration.py): existing downstream responsibilities and provisional sample threshold. These received targeted source review, not execution or a full downstream audit.
- [Frozen Matchup Lens contract](../New%20API/GameLens_Matchup_Lens_API_Contract_Decision_Record.md) and [release handoff](../New%20API/handoffs/Chunk_F_Release_Handoff.md): separate released endpoint and release evidence.
- [Weekly Metric Movement research note](../GameLens_Weekly_Metric_Movement.md): proposed movement semantics and ranking-history caveats.

### Archived selective-reuse evidence

At `26287205f420f569d81ccfcb28a8e8e0656fc24b`:

- [Level 1 adapter](https://github.com/csells10/meow/blob/26287205f420f569d81ccfcb28a8e8e0656fc24b/services/gamelens_level1_service.py)
- [Claim storage](https://github.com/csells10/meow/blob/26287205f420f569d81ccfcb28a8e8e0656fc24b/services/gamelens_claim_storage.py)
- [Adapter tests](https://github.com/csells10/meow/blob/26287205f420f569d81ccfcb28a8e8e0656fc24b/tests/services/test_gamelens_level1_service.py)
- [Storage tests](https://github.com/csells10/meow/blob/26287205f420f569d81ccfcb28a8e8e0656fc24b/tests/services/test_gamelens_claim_storage.py)
- [Runner tests](https://github.com/csells10/meow/blob/26287205f420f569d81ccfcb28a8e8e0656fc24b/tests/test_run_gamelens_level1_capture.py)

### Limits of this review

No current BigQuery claim-table schema or row inventory, upstream run health, production schedule, deployment, or new test result was verified. Those remain explicit later gates. No NFL calendar assumption is needed to select the next operating slate: select it from the authoritative schedule when the approved implementation is ready.
