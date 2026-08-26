# GameLens Learning Lite Sprint

**Document status:** Living sprint; LL-2 implemented locally and awaiting review
**Created:** 2026-08-21
**Owner:** GameLens product stewardship
**Repository:** `csells10/meow`
**Planning and implementation branch:** `learning-lite`
**Production authority:** `main`
**Safety target:** ready by Wednesday, 2026-09-09 at 8:20 p.m. Eastern, before the regular-season opener
**Current checkpoint:** LL-2 — minimal pregame contract; controlled-fixture exit proof passed, awaiting review
**Next action:** review LL-2 before separately authorizing either LL-3 or Matchup Lens M1

Companion documents:

- [Learning Lite README](./README.md)
- [Learning Lite Architecture](./GameLens_Learning_Lite_Architecture.md)
- [Learning Lite Salvage Matrix](./GameLens_Learning_Lite_Salvage_Matrix.md)
- [GameLens Product Ideas](../GameLens_Product_Ideas.md)
- [Archived live packet suite at the preserved `dev` head](https://github.com/csells10/meow/tree/26287205f420f569d81ccfcb28a8e8e0656fc24b/documentation/live)

---

## 1. Sprint decision

Build the smallest trustworthy path that preserves each eligible regular-season pregame read and feeds the existing claim-learning workers.

Do not continue directly into the previously planned Packet 6 coordinator. Do not merge preserved `dev` wholesale into `main` or `learning-lite`. Keep `dev` at its historical prototype head, start future implementation from the production-safe baseline, and selectively reuse only the pieces that support Learning Lite.

The sprint is intentionally changeable. Checkpoints exist to make decisions visible, not to force completion of obsolete scope.

---

## 2. Sprint outcome

The Week 1 success condition is:

> Before kickoff, GameLens can preserve an immutable, pregame-safe copy of the response it actually produced, retain enough lineage to interpret it later, and extract whatever legitimate claims exist without changing the existing product.

Week 1 does not require useful current-season rankings, non-zero claims, a learning conclusion, or Language Calibration output.

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
12. Do not let the Week 1 target expand into an Admin, frontend, or modeling project.

---

## 4. Scope tiers

### 4.1 Must have before the opener

- archive/recovery reference for the historical `dev` head;
- fresh implementation baseline from current `main`;
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
- League Discovery source-data QA for 2025 and available 2026 rows;
- a bounded completed-game manual Claim Grading and Feature Enrichment rehearsal;
- a single-command operator wrapper, provided it does not introduce a general coordinator framework.

### 4.3 Deferred

- automatic postgame learning orchestration;
- per-stage operational tables;
- run-visibility backend or frontend;
- public League Discovery;
- automatic Language Calibration scheduling;
- automatic calibration-to-runtime promotion;
- production modeling or prediction work;
- any schema designed only for a hypothetical future consumer.

---

## 5. Checkpoint sequence

Dates are targets, not promises. A checkpoint closes only when its evidence is documented in `README.md`.

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
**Status:** Implemented locally on `learning-lite`; 23 focused tests passed; awaiting review

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
- [ ] Christian has reviewed and accepted the LL-2 checkpoint record.

### LL-3 — Immutable snapshot persistence

**Target:** 2026-08-26 through 2026-08-28
**Status:** Not started

Objective:

Persist the one irreplaceable new product datum without recreating the prior operational platform.

Required work:

- define or reuse one pregame snapshot table;
- retain the full response payload and required lineage;
- implement insert, identical no-op retry, and conflicting-payload rejection/quarantine behavior;
- provide one manual read-back verification;
- avoid `stage_runs`, `stage_game_results`, and receipt storage.

Required proof:

- first valid write is readable;
- identical retry performs no material change;
- stored hash matches the canonical payload;
- conflicting retry does not replace history;
- unavailable rankings remain valid snapshot content;
- production tables are untouched during development proof.

Exit gate:

- one real or approved shadow game has a verified immutable snapshot and recovery path.

### LL-4 — Claim Extraction from snapshot

**Target:** 2026-08-28 through 2026-08-31
**Status:** Not started

Objective:

Reuse the existing Level 1 calculation owner against the canonical snapshot.

Required work:

- adapt the existing extractor rather than creating a second extractor;
- add or preserve capture lineage on claim rows;
- use a deterministic claim key within the capture;
- ensure all postgame fields remain null at extraction;
- treat zero claims as a successful observable result;
- write through a game-scoped idempotent boundary or retain a manual dry/write split.

Required proof:

- zero-claim snapshot succeeds honestly;
- controlled claim-bearing fixture produces stable keys;
- identical retry creates no duplicates;
- source field paths lead back to the frozen payload;
- no final score, actual winner, or validation target enters a Level 1 row.

Exit gate:

- one canonical snapshot can be translated into reproducible claim rows or a documented zero-claim result.

### LL-5 — Final preseason rehearsal

**Target:** 2026-08-31 through 2026-09-02, subject to schedule and implementation readiness
**Status:** Not started

Objective:

Exercise the production-shaped pregame path on a small bounded preseason slate while keeping all evidence development-only.

Required proof:

- eligible-game selection is correct;
- capture occurs before kickoff;
- payload matches the product response for the same evidence;
- retry behavior is deterministic;
- missing context and zero claims are truthful;
- runtime and resource usage are reasonable;
- failure of one game does not corrupt another game;
- no preseason row enters the regular-season learning cohort.

Exit gate:

- a documented rehearsal supports or rejects production wiring with evidence.

If LL-2 through LL-4 are not ready safely, do not rush them merely to use the preseason window. Controlled fixtures and a later shadow proof remain preferable to a fragile deadline.

### LL-6 — Production-shaped invocation and rollback

**Target:** 2026-09-02 through 2026-09-06
**Status:** Not started

Objective:

Choose the smallest safe way to invoke capture after the existing morning data path.

Preferred order:

1. keep the existing Schedule → Stats → Scores → Facts → Windows → Rankings path unchanged;
2. invoke Learning Lite only after a known-safe metric state;
3. select eligible future regular-season games;
4. freeze snapshots;
5. extract claims;
6. return a simple structured summary;
7. preserve a manual fallback;
8. provide a kill switch that disables learning without disabling the existing application.

Required proof:

- failed or mixed metric state does not create a misleading capture;
- an empty eligible slate is a successful no-op;
- the production learning destination is explicit;
- rollback disables only the new learning path;
- existing application health and game pages remain healthy.

Exit gate:

- one deliberate shadow invocation and one rollback rehearsal pass.

### LL-7 — Week 1 pregame operation

**Target:** 2026-09-07 through the 2026-09-09 safety deadline and regular-season opener, then the 2026-09-13 main slate
**Status:** Not started

Objective:

Preserve the first valid regular-season pregame evidence without forcing current-season conclusions.

Expected behavior:

- rankings or claim rows may be unavailable;
- existing insufficient-context language remains authoritative;
- snapshot capture is still valuable when claims equal zero;
- no prior-season or preseason evidence is silently represented as current regular-season identity;
- operator verifies the opener snapshot before kickoff;
- the Sunday slate repeats the same bounded process.

Exit gate:

- every eligible Week 1 game is accounted for as captured, honestly skipped with reason, or failed with recoverable evidence;
- no post-kickoff reconstruction is represented as a pregame capture.

### LL-8 — First postgame learning cycle

**Target:** after final scores and accepted Week 1 Facts are available
**Status:** Not started; not required before kickoff

Objective:

Run the existing Claim Grading and Feature Enrichment calculations manually against eligible Week 1 captures.

Required rules:

- valid capture plus final score gates game grading;
- accepted completed-game Facts additionally gate Claim Grading;
- Feature Enrichment receives only the explicit pregame projection;
- missing evidence is unavailable, not failure theater;
- one game does not block safe siblings;
- no automatic Language Calibration or runtime change follows.

Exit gate:

- results reconcile to snapshots and claim rows;
- Week 1 limitations are reported plainly;
- the README records whether the process is sustainable without additional orchestration.

### LL-9 — League Discovery data readiness

**Target:** after the Week 1 capture path is stable
**Status:** Deferred analysis checkpoint

Objective:

Prove that the source data needed by IDEA-001 remains available without committing to a product build.

Required checks:

- historical and current ranking dates;
- intended windows;
- lens-tag coverage;
- product-facing versus internal-only tags;
- source-date and lag visibility;
- reproducible movement calculations;
- honest insufficient-history behavior.

Possible output:

- one reviewed query or view definition;
- no API or frontend requirement.

### LL-10 — Controlled Language Calibration

**Target:** after a meaningful claim sample exists; likely Week 2 or later
**Status:** Deferred

Objective:

Run a human-reviewed calibration batch and determine whether any feature or claim surface has enough evidence to justify changed language.

Required output:

- sample size;
- validation rate;
- comparison baseline;
- affected claim surface;
- feature/context definition;
- proposed treatment;
- formula and ruleset version;
- explicit `promote`, `continue_observing`, `caution`, `block`, or `insufficient_evidence` recommendation.

Promotion is a separate checkpoint. A useful result must not automatically modify runtime behavior.

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
| Scope expands back into Packet 6–8 | Week 1 path becomes fragile | Enforce must-have/deferred tiers |
| Rich Week 1 claims are expected | Pressure to manufacture context | Treat zero claims and unavailable rankings as valid |
| Snapshot is recreated after kickoff | Irrecoverable leakage | Pre-kickoff timestamp, denylist, hash, and first-valid-capture rule |
| Existing `/game` changes accidentally | Product regression | Shared builder parity tests and live-route default preservation |
| Preserved `dev` is repurposed or reset | Proven work and breadcrumbs become hard to recover | Keep `dev` at `26287205f420f569d81ccfcb28a8e8e0656fc24b`; do new work only on `learning-lite` |
| Preseason evidence enters production learning | Misleading cohort | Separate dataset/cohort and explicit rejection |
| Feature Enrichment sees validation targets | Training leakage | Explicit input allowlist and target denylist |
| Calibration becomes a catch-all | Unowned ideas and unstable schema | Ownership table and versioned advisory outputs |
| Every concern creates a new table | Hobby project gains enterprise maintenance burden | View/query/log first; table only after repeated need |
| Future algorithm overwrites evidence | Research cannot be reproduced | Append/version outputs; never mutate source observations |

---

## 9. Stop conditions

Pause the sprint if:

- existing production `/game` behavior cannot remain unchanged;
- capture cannot prove it occurred before kickoff;
- a retry can replace canonical evidence;
- the target table or environment is ambiguous;
- a proposed feature uses evaluated-game postgame data;
- the historical `dev` state is no longer recoverable at `26287205f420f569d81ccfcb28a8e8e0656fc24b`;
- Week 1 timing would require skipping verification or rollback;
- a deferred Admin, frontend, or coordinator dependency becomes mandatory without a new scope decision.

---

## 10. Definition of done for the Week 1 slice

The Week 1 slice is done when:

1. historical `dev` remains recoverable at its recorded commit;
2. the Learning Lite implementation is based on current `main`;
3. `/game` behavior remains unchanged;
4. one canonical pregame snapshot can be captured and verified;
5. identical retries are safe;
6. Claim Extraction uses the frozen payload;
7. zero claims and unavailable context are honest outcomes;
8. the opener and main slate have a documented manual fallback;
9. learning can be disabled without disabling the existing application;
10. every eligible Week 1 game has a recorded capture/skip/failure disposition;
11. the README contains the evidence and exact next postgame action;
12. no deferred component was smuggled into the release path.

---

## 11. Open decisions

Branch roles are now locked: `main` is production, `learning-lite` owns current Learning Lite work, and `dev` preserves the earlier prototype. Remaining decisions must not be answered implicitly through code:

1. whether the canonical production snapshot lives in a dedicated dataset or the existing Analytics dataset;
2. whether the existing claim table is extended in place or receives a controlled production successor;
3. whether the Week 1 path is automatically invoked or manually confirmed after the morning load;
4. the minimum useful capture verification query;
5. whether one small coverage summary is enough without an operational ledger;
6. when League Discovery receives its first data-readiness QA;
7. what evidence threshold makes the first 2026 Language Calibration batch meaningful;
8. whether a future algorithm is intended to predict claim reliability, matchup structure, or another explicitly defined target.

Open decisions must not be answered implicitly through code.

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

---

## 13. Immediate next action

LL-2 has reached its controlled-fixture exit gate. Review the README checkpoint record and implementation commits. Then explicitly choose one of three paths:

1. accept LL-2 and separately authorize LL-3;
2. accept LL-2 and separately authorize Matchup Lens M1; or
3. request a bounded LL-2 correction.

Do not begin persistence, Matchup Lens implementation, deployment, scheduling, or production work until that decision is recorded.
