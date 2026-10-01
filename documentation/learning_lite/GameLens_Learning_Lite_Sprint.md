# GameLens Learning Lite Sprint

**Document status:** Living sprint refreshed 2026-10-01; LL-3 bounded development proof remains accepted; remaining stages are readiness-gated and unimplemented
**Created:** 2026-08-21
**Owner:** GameLens product stewardship
**Repository:** `csells10/meow`
**Planning and implementation branch:** `learning-lite`
**Production authority:** `main`
**Historical safety target:** 2026-09-09 has passed; only actual pre-kickoff captures for upcoming games may enter the new cohort
**Current checkpoint:** LL-3 development exit gate met; LL-4 remains a proposed next checkpoint
**Next action:** use the README's read-only LL-4 readiness prompt. Review exact files/schema/tests and report any real-snapshot-use decision for Open `LL-FIND-001`; do not begin implementation or source correction.

The October 1 gates below supersede the original August and Week 1 execution calendar. Original dates remain historical labels; the September 30 reassessment records the reasoning. No production activation or LL-4 implementation is authorized by this refresh. The Matchup Lens endpoint released on `main` after this plan was written; its frozen contract in `documentation/New API/` governs the shipped endpoint.

Companion documents:

- [Learning Lite README](./README.md)
- [Learning Lite Architecture](./GameLens_Learning_Lite_Architecture.md)
- [Learning Lite Salvage Matrix](./GameLens_Learning_Lite_Salvage_Matrix.md)
- [Learning Lite Findings Log](./GameLens_Learning_Lite_Findings_Log.md)
- [GameLens Product Ideas](../GameLens_Product_Ideas.md)
- [Archived live packet suite at the preserved `dev` head](https://github.com/csells10/meow/tree/26287205f420f569d81ccfcb28a8e8e0656fc24b/documentation/live)

---

## 1. Sprint decision

Build the smallest trustworthy path that preserves each eligible regular-season pregame read and feeds the existing claim-learning workers.

Do not continue directly into the previously planned Packet 6 coordinator. Do not merge preserved `dev` wholesale into `main` or `learning-lite`. Keep `dev` at its historical prototype head, start future implementation from the production-safe baseline, and selectively reuse only the pieces that support Learning Lite.

The sprint is intentionally changeable. Checkpoints exist to make decisions visible, not to force completion of obsolete scope.

---

## 2. Sprint outcome

The initial prospective operating cohort succeeds when:

> Before kickoff, GameLens preserves an immutable, pregame-safe copy of its response with enough lineage to interpret it later. Claim Extraction reads that frozen record and may happen after kickoff; it never reconstructs missed pregame evidence.

Unavailable rankings and zero claims remain valid outcomes. No learning conclusion or Language Calibration output is required for initial operation. The next development finish line is one snapshot translated into trustworthy claim rows or an explicit zero-claim result.

---

## 3. Product and engineering principles

1. `main` is the production authority.
2. Existing `/game` behavior is protected.
3. The historical `dev` branch is preserved as salvage/archive evidence.
4. Reuse existing calculation owners.
5. Add one durable pregame snapshot record before adding operational tables.
6. Persist football evidence and learning results; log routine plumbing.
7. Treat unavailable context and zero claims as honest data.
8. Keep preseason evidence separate from regular-season learning cohorts.
9. Keep Feature Enrichment inputs pregame-only.
10. Keep Language Calibration advisory and human-reviewed.
11. Preserve League Discovery source data even if no view or interface is built.
12. Keep the current checkpoint bounded; no Admin, frontend, or modeling expansion.

---

## 4. Scope tiers

### 4.1 Required before initial production operation

- archive/recovery reference for the historical `dev` head;
- verify the current branch relationship to `main`; preserve accepted Learning Lite work, with any reconciliation separately scoped;
- pregame-safe shared `/game` builder;
- deterministic capture identity;
- canonical payload hash;
- postgame-field rejection;
- one immutable snapshot store;
- one-game and bounded-slate capture path;
- Claim Extraction from the canonical snapshot;
- snapshot lineage on claim rows;
- identical-retry safety;
- explicit zero-claim and unavailable-ranking behavior;
- manual invocation and verification fallback;
- kill switch or equivalent fail-closed production control;
- proof that existing `/game`, confidence, Matchup Lean, language support, and frontend behavior remain unchanged.

### 4.2 Helpful if the must-have path is already stable

- automatic invocation after the existing morning load;
- a concise capture/claim coverage summary;
- a bounded completed-game manual Claim Grading and Feature Enrichment rehearsal;
- a single-command operator wrapper, provided it does not introduce a general coordinator framework.

### 4.3 Deferred

- automatic postgame learning orchestration;
- per-stage operational tables;
- run-visibility backend or frontend;
- League Discovery readiness analysis (LL-9), query/view work, and public UI; optional and off the capture/claim critical path;
- automatic Language Calibration scheduling;
- automatic calibration-to-runtime promotion;
- production modeling or prediction work;
- any schema designed only for a hypothetical future consumer.

---

## 5. Checkpoint sequence

A checkpoint closes on recorded evidence, not a calendar date. LL-0 through LL-3 retain their dated closure evidence; LL-3 is accepted for development only. The active sequence is LL-4 readiness review → LL-4 → LL-5 → LL-6 → separately authorized LL-7 → LL-8 → LL-10 when sufficient evidence exists. LL-9 is optional. A separately scoped development LL-8 rehearsal can follow LL-4 before production activation.

LL-0 through LL-3 below retain their historical target and proof details; later dated README closure evidence wins over earlier counts or statuses. LL-4's exact schema, file list, and focused tests still require review.

### LL-0 — Direction and documentation

**Target:** 2026-08-21
**Status:** Planning complete; implementation not started

Deliverables:

- Learning Lite architecture;
- living sprint;
- checkpoint README;
- explicit supersession boundary for the previous Packet 6 direction;
- descriptive terminology for the existing Levels;
- initial Week 1 scope and deferrals.

Exit gate:

- the new documentation clearly separates existing production capability, reusable development work, proposed work, and deferred work;
- no code, data, or production behavior changed.

### LL-1 — Preserve and inventory

**Target:** before implementation
**Status:** Accepted as the current planning baseline; no implementation code has been ported

Objective:

Preserve the current development work and identify exactly what should be reused.

Required work:

1. [x] record current `main` (`b93c41c`) and historical `dev` (`2628720`) commit SHAs;
2. [x] verify `dev` retains the recoverable historical prototype at `26287205f420f569d81ccfcb28a8e8e0656fc24b`;
3. [x] inventory the documented six-table `GameLens_dev` prototype shape; defer current live schema verification to LL-3, immediately before any persistence decision;
4. [x] draft a file-by-file salvage matrix covering all 92 changed files: port/adapt, archive only, superseded, or do not port;
5. [x] identify the focused tests attached to each salvage candidate;
6. [x] confirm `learning-lite` starts from current production `main`;
7. [x] keep `dev` unchanged as the recovery and salvage source.

Exit gate:

- current work is recoverable;
- no ambiguous file remains in the port list;
- the bounded LL-2 worklist is approved.

### LL-2 — Minimal pregame contract

**Target:** 2026-08-24 through 2026-08-26
**Status:** Accepted on `learning-lite`; 23 focused tests passed locally; read-only data review completed

Objective:

Port or adapt only the pure safety boundaries needed to construct and identify a pregame response. LL-2 is deliberately side-effect-free: it creates no table and writes no snapshot.

Candidate reuse:

- `GameDetailsEvidence` or equivalent evidence container;
- side-effect-free evidence loading;
- shared response builder;
- `get_pregame_game_details(...)` or equivalent pregame-only entry point;
- deterministic capture ID;
- payload SHA-256;
- postgame-field detection and rejection;
- canonical-capture decision logic;
- focused unit tests.

Explicitly excluded from LL-2:

- snapshot table creation or verification;
- snapshot writes or retry reconciliation;
- Claim Extraction writes;
- deployment, scheduling, or production wiring.

Required proof:

- live and pregame builders produce equivalent pregame product sections from the same evidence;
- pregame mode does not query final score;
- pregame mode cannot call the completed-game outcome writer;
- postgame-shaped payloads fail closed;
- existing live route behavior remains unchanged.

Exit gate:

- [x] one eligible controlled fixture produced a valid, hashable, side-effect-free pregame payload;
- [x] live/pregame product-section parity, no final-score query, no completed-game write, and fail-closed postgame rejection were proven;
- [x] no table, persistence, deployment, production, route, auth, CORS, frontend, or Matchup Lens behavior changed;
- [x] Christian reviewed and accepted the LL-2 contract mechanics; the pre-existing snap-metric data issue is tracked separately as `LL-FIND-001`.

### LL-3 — Immutable snapshot persistence

**Target:** 2026-08-26 through 2026-08-28
**Status:** Accepted for bounded development proof on 2026-09-30. The failed August row was exported and removed by guarded one-row recovery; an upcoming PIT–CLE snapshot passed write/read-back, identical retry, and read-only conflict proof. No production activation.

Objective:

Persist the one irreplaceable new product datum without recreating the prior operational platform.

Required work:

- define or reuse one pregame snapshot table;
- retain the full response payload and required lineage;
- implement insert, identical no-op retry, and conflicting-payload rejection/quarantine behavior;
- provide one manual read-back verification;
- avoid `stage_runs`, `stage_game_results`, and receipt storage.

Approved target boundary:

- reuse `nfl-stream-406420.GameLens_dev.pregame_snapshots` exactly as verified;
- preserve its seven historical rows;
- retain its 22-field schema, daily `captured_at` partition, and `game_id`, `season_type` clustering;
- do not create, replace, migrate, truncate, or delete the table;
- the `LL-FIND-001` waiver applies only to the controlled LL-3 development-fixture proof and expires at this checkpoint's exit gate.

Approved failed-proof recovery exception:

- export and preserve the complete `capture_78f546a459294669fd10da22` row before cleanup;
- delete only that exact development row after the unchanged export SHA-256 is supplied back to the guarded command;
- keep the ordinary LL-3 storage seam insert-only and expose no general delete interface;
- preserve all seven historical rows and every production table;
- repeat the corrected first-write/read-back and identical-no-op proofs before accepting LL-3.

Required proof:

- first valid write is readable;
- identical retry performs no material change;
- stored hash matches the canonical payload;
- conflicting retry does not replace history;
- unavailable rankings remain valid snapshot content;
- production tables are untouched during development proof.

Exit gate:

- [x] One real upcoming game has a verified immutable development snapshot and recovery path. `20261001_PIT@CLE` was captured before kickoff as `capture_62ecbfb862ef9f3ed05a5626` with canonical SHA-256 `f7b37766f17aedd41b52293ed829241c355ca7600654c314eac81777fb5e804b`; read-back verified, identical retry made no change, and a read-only-client conflict candidate was rejected. The final development table count was eight unique rows, including seven other historical rows and no failed August row. Full evidence is in the README.

### LL-4 — Claim Extraction from snapshot

**Historical target:** 2026-08-28 through 2026-08-31; expired  
**Start gate:** accepted LL-3 development proof plus a reviewed LL-4 file/schema/test proposal  
**Status:** Not started; read-only readiness review is next

Objective: reuse the existing Level 1 calculation owner against one canonical snapshot, with a thin adapter and development-only reconciliation.

Before implementation:

- inspect current claim schemas read-only, including consumer expectations; do not infer live schema from setup scripts;
- identify the exact development target, additive fields if any, candidate files, and focused tests;
- decide stable capture-scoped keys, extraction version/configuration, and immutable versus later-stage fields;
- keep LL-FIND-001 Open; no inherited waiver. A real-snapshot proof needs a separately recorded scoped decision or resolution. Controlled synthetic fixtures can prove mechanics;
- reuse the corrected current canonical hash; omit the archived receipt layer and legacy append/replace-run writer.

Required proof:

- valid claim-bearing and zero-claim fixtures succeed;
- input identity/hash/capture timing are validated against the frozen record, including after kickoff;
- source paths resolve to their original claims; output keys are stable and unique;
- all new-row postgame targets are null, including legacy placeholders such as `final_margin_bucket`;
- identical replay creates no duplicates, preserves first-write audit values, and does not reset later grading/features;
- conflicts, duplicate keys, partial writes, and read-back mismatch have bounded fail-closed/recovery behavior;
- dry-run and zero-claim operation make no claim mutation; structured summaries expose the result without stage receipts.

Exit gate: one accepted canonical development snapshot yields reproducible, read-back-verified claim rows or a documented zero-claim result; focused proof passes and README records exact data effects. No production schema/write, deployment, scheduling, new claim surface, or source correction is included.

### LL-5 — In-season development rehearsal

**Historical target:** final preseason rehearsal, 2026-08-31 through 2026-09-02; expired  
**Start gate:** LL-4 accepted and applicable data-quality decisions recorded  
**Status:** Not started

Exercise a small named upcoming regular-season slate in development. Select games when ready; do not force a replacement Week 4 deadline.

Required proof:

- eligible selection, pre-kickoff timing, same-evidence product parity, and truthful missing context/zero claims;
- every selected game has a capture/extraction/skip/failure disposition;
- retries reuse existing canonical records; changed live evidence cannot replace them;
- an empty slate is a successful no-op; schedule changes have an explicit policy;
- one failed game does not corrupt or block safe siblings;
- cohort/environment separation holds; runtime, query cost, and manual effort are recorded.

Exit gate: documented bounded rehearsal supports or rejects moving to invocation readiness. Use serial execution initially; sequential retry proof does not establish concurrent-writer safety.

### LL-6 — Invocation readiness and rollback

**Historical target:** 2026-09-02 through 2026-09-06; expired  
**Start gate:** LL-5 accepted  
**Status:** Not started

Choose the smallest invocation after a known-safe upstream metric state. Preserve Schedule → Stats → Scores → Facts → Windows → Rankings and existing product behavior.

Required decisions/proof:

- explicit production snapshot/claim destinations, exact schema plan, permissions, and source commit;
- positive evidence of accepted consistent upstream data; a supplied run ID or time of day alone is insufficient;
- failed/mixed upstream state blocks new capture; empty eligible slate is a no-op;
- manual fallback, serialized invocation, first-capture window, and overlap prevention;
- a development shadow invocation and disable/rollback rehearsal pass;
- disabling Learning Lite leaves the existing application/data pipeline healthy and preserves valid evidence.

Exit gate: activation plan and rollback evidence are reviewable. This does not activate production. Any deployment or first production write is a separately authorized sub-gate before LL-7 operation; do not relabel development captures as production.

### LL-7 — First prospective production cohort

**Historical target:** Week 1, 2026-09-07 through 2026-09-13, including the September 9 safety target; expired  
**Start gate:** LL-6 accepted and production activation explicitly authorized  
**Status:** Not started

Operate on the first named upcoming regular-season cohort selected after readiness. Missing Week 1 or other historical captures remain missing.

Required proof:

- a representative first game and bounded slate are captured and verified before kickoff;
- every candidate is captured, honestly skipped with reason, or failed with recoverable evidence;
- capture success is distinguished from extraction pending/failure and successful zero claims;
- extraction may safely retry after kickoff using the authentic frozen record;
- unavailable rankings remain honest; no silent prior-season/preseason identity substitution;
- retries preserve original timestamps/hashes; existing product behavior remains healthy.

Exit gate: complete cohort accounting and a sustainable manual operating path, with evidence and next postgame action recorded. No post-kickoff reconstruction is accepted as a new pregame capture.

### LL-8 — First manual postgame learning cycle

**Start gate:** authentic captures, accepted LL-4 extraction, final scores, and accepted completed-game Facts  
**Status:** Not started; not required before kickoff

Normally follows LL-7. A separately approved development rehearsal may use the LL-3 capture after LL-4 acceptance and final evidence readiness without waiting for production activation.

Required rules/proof:

- valid capture plus final score gates game grading; accepted Facts additionally gate claim grading;
- existing Claim Grading and Feature Enrichment owners retain calculation ownership;
- Feature Enrichment sees only an explicit pregame projection, even if executed after final;
- missing evidence remains unavailable; safe sibling games may proceed;
- results and versions reconcile to immutable snapshots and claim rows;
- retries preserve source evidence and valid later-stage fields.

Exit gate: one bounded manual cycle is verified and its operating burden recorded. No automatic calibration, duplicate outcome ledger, coordinator, or runtime promotion follows.

### LL-9 — Optional League Discovery data readiness

**Start gate:** separate decision to investigate ranking-history usefulness  
**Status:** Deferred; optional and off the critical path

League Discovery means exploring team/league trends from existing rankings and lens tags. It is not required for snapshot capture or claim extraction.

If requested, verify dates/windows, source lag, product-facing tag coverage/filtering, rank direction, sample counts, reproducible movement, and honest insufficient history. Distinguish a team's own metric movement from changes in relative rank. Recomputed historical ranking dates are not automatically immutable point-in-time evidence.

Exit gate: one reviewed read-only query/report establishes usefulness and limitations. No API, frontend, overall power-rating model, or physical table is required.

### LL-10 — Controlled advisory Language Calibration

**Historical timing:** “Week 2 or later”; superseded by sample readiness  
**Start gate:** reviewed sample of compatible graded/enriched claims with meaningful game/week coverage  
**Status:** Deferred

Required output:

- sample size in rows and distinct games, week coverage, and available/unavailable denominators;
- validation rate, comparison baseline, affected claim surface, and feature/context definition;
- extraction/feature/formula/ruleset versions, limitations, and uncertainty;
- explicit `promote`, `continue_observing`, `caution`, `block`, or `insufficient_evidence` recommendation.

Review the worker's provisional 30-row threshold rather than treating correlated claims as independent games. Insufficient evidence is a valid result.

Exit gate: a reproducible advisory report is reviewed. Runtime promotion is a separate checkpoint; calibration must not automatically change Matchup Lean, confidence, Model Trust, or language.

---

## 6. Checkpoint documentation requirements

Every checkpoint must update `README.md` with:

- date and status;
- branch and commit;
- objective;
- exact files changed;
- tests/commands and results;
- data reads/writes and row counts;
- production effects;
- game IDs and payload hashes when relevant;
- decisions and rejected alternatives;
- known limitations;
- recovery point;
- next checkpoint.

Also update this Sprint when scope, sequencing, status, or acceptance criteria change.

Checkpoint evidence should distinguish:

- planned;
- implemented locally;
- proven against controlled fixtures;
- proven against real development data;
- deployed in shadow;
- active in production.

These states are not interchangeable.

---

## 7. Data contracts to protect

### Pregame Snapshot

- immutable full response;
- pre-kickoff timestamp;
- deterministic identity;
- canonical hash;
- source dates and versions;
- ranking availability reason;
- full lineage needed to reproduce interpretation.

### Claim row

- one claim within one canonical capture;
- deterministic claim key;
- source field path;
- claim type and layer;
- pregame values and context;
- postgame targets null at extraction;
- versioned later grading/features.

### League Discovery source

- time-aware ranking rows;
- window type;
- rank and percentile;
- metric hierarchy;
- repeated lens tags;
- source freshness and lag.

### Language Calibration

- versioned analytical recommendation;
- evidence count and baseline;
- no automatic runtime authority.

---

## 8. Risk register

| Risk | Consequence | Current response |
|---|---|---|
| Scope expands back into Packet 6–8 | Initial operating path becomes fragile | Enforce must-have/deferred tiers |
| Rich initial-cohort claims are expected | Pressure to manufacture context | Treat zero claims and unavailable rankings as valid |
| Snapshot is recreated after kickoff | Irrecoverable leakage | Pre-kickoff timestamp, denylist, hash, and first-valid-capture rule |
| Existing `/game` changes accidentally | Product regression | Shared builder parity tests and live-route default preservation |
| Preserved `dev` is repurposed or reset | Proven work and breadcrumbs become hard to recover | Keep `dev` at `26287205f420f569d81ccfcb28a8e8e0656fc24b`; do new work only on `learning-lite` |
| Preseason evidence enters production learning | Misleading cohort | Separate dataset/cohort and explicit rejection |
| Feature Enrichment sees validation targets | Training leakage | Explicit input allowlist and target denylist |
| Calibration becomes a catch-all | Unowned ideas and unstable schema | Ownership table and versioned advisory outputs |
| Every concern creates a new table | Hobby project gains enterprise maintenance burden | View/query/log first; table only after repeated need |
| Future algorithm overwrites evidence | Research cannot be reproduced | Append/version outputs; never mutate source observations |
| Missing snap totals are ranked as real zeroes | Misleading rank, tier, and edge language can enter a pregame response | The LL-3 development-proof waiver expired at its exit gate; `LL-FIND-001` is Open before production capture |

---

## 9. Stop conditions

Pause the sprint if:

- existing production `/game` behavior cannot remain unchanged;
- capture cannot prove it occurred before kickoff;
- a retry can replace canonical evidence;
- the target table or environment is ambiguous;
- a proposed feature uses evaluated-game postgame data;
- the historical `dev` state is no longer recoverable at `26287205f420f569d81ccfcb28a8e8e0656fc24b`;
- an upcoming kickoff would require skipping verification or rollback;
- a deferred Admin, frontend, or coordinator dependency becomes mandatory without a new scope decision.

---

## 10. Definition of done for the initial operating slice

The initial operating slice is done when:

1. historical `dev` remains recoverable at its recorded commit;
2. the Learning Lite implementation is based on current `main`;
3. `/game` behavior remains unchanged;
4. one canonical pregame snapshot can be captured and verified;
5. identical retries are safe;
6. Claim Extraction uses the frozen payload;
7. zero claims and unavailable context are honest outcomes;
8. the first selected game and bounded slate have a documented manual fallback;
9. learning can be disabled without disabling the existing application;
10. every game in the selected prospective cohort has a recorded capture/skip/failure disposition;
11. the README contains the evidence and exact next postgame action;
12. no deferred component was smuggled into the release path.

---

## 11. Open decisions

Branch roles are now locked: `main` is production, `learning-lite` owns current Learning Lite work, and `dev` preserves the earlier prototype. Remaining decisions must not be answered implicitly through code:

1. whether the canonical production snapshot lives in a dedicated dataset or the existing Analytics dataset;
2. whether the existing claim table is extended in place or receives a controlled production successor;
3. whether invocation stays manual after a known-healthy upstream state; manual and serial is the initial recommendation;
4. the minimum useful capture verification query;
5. whether one small coverage summary is enough without an operational ledger;
6. when League Discovery receives its first data-readiness QA;
7. what evidence threshold makes the first 2026 Language Calibration batch meaningful;
8. whether a future algorithm is intended to predict claim reliability, matchup structure, or another explicitly defined target.

Immediate LL-4 decisions are narrower: exact development claim target/schema, identity/version/reconciliation policy, and the LL-FIND-001 real-data-use boundary. Production destinations/invocation belong to LL-6; League Discovery and model research can wait. Open decisions must not be answered implicitly through code.

---

## 12. Change log

### 2026-08-21 — LL-0 planning baseline

- Adopted the Learning Lite name and direction.
- Reframed the earlier packet platform as archive/salvage evidence.
- Selected one immutable pregame snapshot as the first justified new durable record.
- Confirmed reuse of the existing analytical spine and claim-learning calculation owners.
- Preserved League Discovery optionality through existing ranking and lens-tag data.
- Replaced Level-only planning language with Snapshot, Claim Extraction, Claim Grading, Feature Enrichment, and Language Calibration.
- Defined Language Calibration as a versioned advisory editor rather than a catch-all.
- Made the Week 1 target evidence preservation, not rich conclusions.
- Deferred coordinator, run ledger, Admin visibility, frontend, automatic calibration, and modeling scope.
- Made checkpoint README updates mandatory.

### 2026-08-26 — LL-2 controlled-fixture exit proof

- Added one pure pregame identity, canonical JSON/SHA-256, and fail-closed postgame-field contract.
- Routed the existing live and new pregame entry points through one shared response builder.
- Proved pregame mode cannot query final score or invoke the completed-game writer.
- Preserved Matchup Lean, confidence, Model Trust, claim-language support, Lens Tags, routes, auth, CORS, and frontend behavior.
- Passed 23 focused tests, Python compilation, and whitespace validation.
- Created no table, persistence, deployment, production, or Matchup Lens behavior.
- Stopped at review before LL-3 or Matchup Lens M1.

### 2026-08-26 — LL-2 local acceptance review

- Christian fast-forwarded local `learning-lite` to `ebba299ee7f5511126ed02f2e164bf4235b4b567`.
- Reproduced all 23 passing tests and the documented capture ID and payload SHA-256.
- Performed read-only BigQuery checks against Schedule, Facts, Windowed Metrics, Rankings, and schema metadata.
- Confirmed 32-team downstream coverage, aligned 2026-08-23 source dates, zero future-source/negative-lag rows, complete lens-tag coverage, and non-null `ARRAY<STRING>` storage.
- Accepted LL-2's contract mechanics.
- Logged systemic zero/missing snap-total semantics as `LL-FIND-001`; Matchup Lean and confidence are protected, but ranking/tier/edge language may be misleading.
- Created or changed no table, row, deployment, route, schedule, traffic, or production behavior.

### 2026-08-26 — LL-3 scoped implementation, pre-write

- Recorded Christian's controlled-fixture waiver of `LL-FIND-001`; it expires at the LL-3 exit gate.
- Verified and selected the existing `nfl-stream-406420.GameLens_dev.pregame_snapshots` table with seven unique historical rows.
- Added strict JSON-domain validation, one-game read-only evidence loading, exact-table verification, insert-only reconciliation, and a dry-run-default manual runner.
- Added no setup path because the approved table already exists.
- Passed 26 LL-2/product regression tests and 20 focused LL-3 tests.
- Preserved all protected product, route, auth, CORS, frontend, Model Trust, claim-language, metric-registry, and live-query files.
- Created, replaced, altered, or written no table or row; the manual development proof remains outstanding.

---

### 2026-09-30 — LL-3 development proof closure

- Christian fast-forwarded local `learning-lite` to `c4d2974`; 27 LL-2/product tests and 32 focused LL-3/recovery tests passed.
- Exported the exact failed `20260827_PIT@BUF` development row outside the repository (evidence SHA-256 `6b6080425a04780c23e7e6789df008a0b54a540918aab6f1f29a4654b868108c`), then the guarded deletion affected one row and verified its absence.
- Captured `20261001_PIT@CLE` before kickoff in the existing `GameLens_dev.pregame_snapshots` table: `capture_62ecbfb862ef9f3ed05a5626`, payload SHA-256 `f7b37766f17aedd41b52293ed829241c355ca7600654c314eac81777fb5e804b`, rankings available, 90 lens tags.
- First write passed read-back; identical retry retained the original timestamp and made no change. Read-only inspection confirmed semantic equality and identical candidate/stored/fresh hashes; 19 raw integral-float/int representation differences remained visible by design.
- A deliberately conflicting in-memory candidate was rejected while a read-only client blocked all non-SELECT queries. Final table inventory: eight rows, eight distinct captures, one new row, zero failed August rows, seven other historical rows.
- `LL-FIND-002` is resolved. `LL-FIND-001` returns to Open at this exit gate. No production write, deployment, route, schedule, frontend, or Claim Extraction change occurred.

---

### 2026-10-01 — Documentation and prompt refresh

- Replaced expired future operating dates with readiness gates; historical dates and accepted LL-0 through LL-3 proof remain visible.
- Added the LL-4 readiness gate, bounded null-target/lineage/replay proof, in-season rehearsal, and distinct activation/operation gates.
- Kept LL-FIND-001 Open, source diagnosis deferred, and LL-9 optional; no implementation, waiver, cloud operation, or production change.

---

## 13. Immediate next action

Use [the README's current next-chat prompt](./README.md#current-next-chat-prompt--ll-4-readiness-only) for a bounded read-only LL-4 file/schema/test proposal. Stop with concrete decisions and unknowns; do not implement or reopen LL-3. Do not use the archived LL-3 prompt or historical M1 proposal.
