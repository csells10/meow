# GameLens Packet 3 — Production-Safe Level 1 Plan

**Status:** Ready for review — planning complete; implementation has not started  
**Created:** 2026-08-13  
**Branch:** `dev`  
**Predecessor:** [Packet 2 — Shadow Pregame Snapshot Plan](./GameLens_Packet_2_Shadow_Pregame_Snapshot_Plan.md)  
**Sprint authority:** [GameLens Learning Orchestration Product Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md)  
**Production behavior changed:** No  
**Production learning data written:** No  

---

## The short version

Packet 2 froze exactly what GameLens showed before kickoff. Packet 3 turns one
approved frozen response into the individual claims GameLens made and saves
those claims safely.

```text
GameLens_dev.pregame_snapshots
        one approved capture
                 ↓
existing Level 1 claim extractor
                 ↓
deterministic snapshot-aware claim identities
                 ↓
GameLens_dev claim rows + one per-game Level 1 receipt
```

Packet 3 does not rebuild `/game`, recapture a game, grade a result, run
Levels 2–4, or connect anything to the 8:00 a.m. production load.

## Why this packet exists

The historical Level 1 worker already knows how to identify useful claims in a
saved GameLens payload. It was designed for bounded filesystem QA runs, not for
an accumulating game-by-game learning cohort.

That distinction creates five real gaps:

1. it reads payload JSON files instead of canonical BigQuery snapshots;
2. its legacy claim key is game-oriented rather than capture-oriented;
3. its BigQuery path can delete a whole `run_id` and append replacement rows;
4. the existing table lacks explicit `learning_run_id`, `capture_id`,
   pipeline lineage, and source-payload hash fields; and
5. a valid capture that extracts zero claims leaves no durable proof that Level
   1 processed it.

Packet 3 closes those gaps through a thin adapter and storage boundary. It does
not rewrite the football or claim-extraction logic.

## Packet 2 handoff — facts Packet 3 may trust

Packet 3 inherits these completed development proofs:

- `GameLens_dev.pregame_snapshots` contains six canonical 2026 preseason
  snapshots;
- each snapshot has a deterministic `capture_id` and stable
  `learning_run_id=gamelens_2026_preseason_v1`;
- each saved payload was captured before kickoff;
- postgame fields were rejected from the canonical payload;
- saved hashes were verified after BigQuery read-back;
- all six saved responses exactly matched authenticated live `/game`;
- ARI–LV contains populated ranking and `lens_tags` evidence;
- five early-season games honestly contain no ranking-derived comparison
  evidence;
- `stage_runs` records one row per coordinator attempt;
- `stage_game_results` records one row per attempt, stage, and game;
- the nine-row historical result backfill is idempotent; and
- CAR–ARI on August 6 remains `capture_missing` and cannot enter Level 1.

Packet 3 must not reinterpret or repair any of those records.

## Authoritative inputs

### Canonical product input

Level 1 reads exactly one row from:

```text
GameLens_dev.pregame_snapshots
```

The selected row must have:

- a requested `capture_id`;
- one matching `game_id`;
- a nonblank `learning_run_id`;
- `environment=dev`;
- `capture_status=captured`;
- a valid saved `payload_sha256`;
- a JSON-object `response_payload`; and
- no populated postgame field prohibited by the Packet 1 contract.

The adapter recalculates the payload hash and reruns the pregame payload
validator before extraction. A mismatch is a visible failure with zero claim
writes.

### Operational evidence only

`stage_runs` and `stage_game_results` answer what a coordinator attempted.
They may support reconciliation and receipts, but they are not prediction
evidence and must never substitute for a canonical snapshot.

### Explicitly forbidden inputs

Packet 3 must not read:

- live `/game` over HTTP;
- a newly rebuilt game response;
- final scores or Model Outcome;
- postgame Facts, validation labels, or feature outputs;
- the August 6 CAR–ARI game without a canonical snapshot; or
- historical filesystem payloads through the new snapshot entry point.

## What already exists and will be reused

| Existing code | Packet 3 use |
|---|---|
| `extract_payload_context(...)` | Build the extractor context from the saved response, with canonical lineage supplied by the adapter |
| `extract_claim_rows(...)` | Preserve the existing claim selection and football semantics |
| `build_claim_key(...)` in `gamelens_learning_contract.py` | Create the snapshot-aware production-safe claim identity |
| `validate_pregame_payload(...)` and `payload_sha256(...)` | Revalidate the saved input before extraction |
| `BigQuerySnapshotStorage.read_snapshot(...)` | Load the approved canonical capture |
| `stage_game_results` | Record Level 1 success, no-op, failure, and zero-claim completion |
| Existing claim-training schema | Remain the broad row shape that later Levels 2–3 enrich |
| Historical filesystem CLI | Continue serving historical QA through its existing adapter |

No claim-selection logic should be copied into the new coordinator or storage
class.

## Required implementation shape

Packet 3 should add one small service boundary, provisionally:

```text
services/gamelens_level1_service.py
```

Its public responsibility should resemble:

```python
extract_level1_from_capture(
    capture_id,
    pipeline_run_id,
    write=False,
)
```

The exact name may change during implementation, but the responsibility may
not expand. The service:

1. loads one canonical snapshot;
2. validates identity, environment, hash, and pregame safety;
3. creates extractor context from the saved response;
4. calls the existing extractor;
5. converts legacy rows to the canonical lineage and key contract;
6. validates uniqueness and Level 1 null boundaries;
7. returns a dry-run summary or performs a game-scoped MERGE; and
8. writes one per-game stage result when a deliberate write is requested.

It does not coordinate a slate or alter `app.py`.

## Identity contract

| Field | Meaning | Packet 3 rule |
|---|---|---|
| `pipeline_run_id` | ETL execution whose healthy evidence supported capture | Preserve from the snapshot's metric-pipeline lineage; do not invent a daily value during replay |
| `learning_run_id` | Stable season + phase + ruleset cohort | Copy exactly from the snapshot |
| `capture_id` | Immutable pregame source | Copy exactly from the snapshot |
| `claim_key` | One claim inside that capture | Build with the shared capture-aware contract |
| `game_id` | Schedule/game join identity | Must agree between snapshot header and snapshot row |
| `payload_sha256` | Exact frozen-response proof | Copy only after recalculation matches |

The canonical claim key is:

```text
build_claim_key(
    capture_id,
    source_field_path,
    claim_type,
    claim_rank,
)
```

The BigQuery logical MERGE key is:

```text
learning_run_id + claim_key
```

Because `claim_key` already incorporates `capture_id`, including
`capture_id` again in the MERGE key is redundant. It remains a required
stored and inspectable lineage field.

The historical extractor's `make_claim_key(...)` remains available for its
filesystem QA path. The new snapshot adapter must replace that legacy key on
the rows it prepares; it must not silently change historical artifacts.

## Development table contract

Packet 3 creates or verifies a development-only claim table, provisionally:

```text
GameLens_dev.claim_training_examples
```

It reuses the existing claim-training row shape so Packet 4 can adapt the
existing Level 2 and Level 3 workers rather than translate a second model.
The development schema adds these required lineage fields:

- `learning_run_id STRING REQUIRED`;
- `capture_id STRING REQUIRED`;
- `pipeline_run_id STRING`;
- `source_payload_sha256 STRING REQUIRED`;
- `extraction_version STRING REQUIRED`; and
- `extracted_at TIMESTAMP REQUIRED`.

For compatibility during the transition, existing `run_id` is populated with
the same value as `learning_run_id`. This alias must be documented and tested;
Packet 3 must not let the two values disagree.

Existing postgame columns remain nullable because Packet 4 will later enrich
the same rows. At Level 1, every postgame or validation field must be null,
including:

- `actual_team`, `actual_side`, and `validation_result`;
- `validated_flag` and `elevated_deserved_flag`;
- actual gap/rank/percentile fields;
- `actual_winner`, `model_result`, and final-score fields; and
- game-level QA validation rates.

The setup script must be idempotent and must fail closed outside dev. Packet 3
does not create a production dataset or production learning table.

## Write contract

The current historical `--replace-run` behavior is not used for canonical
snapshot ingestion.

A deliberate Packet 3 write must:

1. stage all validated claim rows for one capture;
2. reject duplicate logical keys inside the staged rows;
3. MERGE on `learning_run_id + claim_key`;
4. insert missing claims;
5. treat byte-for-byte/logically identical matches as unchanged;
6. fail on an existing row whose immutable Level 1 fields conflict;
7. never delete unrelated games or claims; and
8. report input, inserted, unchanged, and conflict counts.

Updating postgame fields belongs to Packet 4. Packet 3 must not update an
existing claim's Level 1 source fields.

## Zero-claim captures

Zero claims can be an honest early-season result. A zero-row claim table cannot
prove that the game was processed, so Packet 3 writes a
`stage_game_results` row with:

- `stage_name=level1_claim_extraction`;
- the canonical `game_id`, `capture_id`, and `learning_run_id`;
- `status=success`;
- `reason=zero_claims_extracted`;
- `input_count=1`;
- `output_count=0`; and
- the preserved upstream pipeline reference.

A retry finds that logical receipt and produces a no-change result. It does not
turn an honest zero-claim capture into a failure.

## Failure and no-op behavior

| Condition | Result |
|---|---|
| Capture ID not found | Failure; no claim write |
| Snapshot identity or hash mismatch | Failure; no claim write |
| Populated postgame field found | Failure; no claim write |
| Snapshot is not from dev during this packet | Failure; no claim write |
| Duplicate extracted claim keys | Failure; no claim write |
| Existing immutable claim conflicts | Failure; preserve existing row |
| First valid nonzero extraction | Success; insert missing rows |
| First valid zero-claim extraction | Success with `zero_claims_extracted` receipt |
| Identical replay | No data changes; truthful unchanged/no-op summary |
| CAR–ARI August 6 requested | Failure/not eligible because no canonical capture exists |

One bad capture must not damage any other game's claim rows.

## Observability and reconciliation

Every deliberate write returns a readable summary containing:

- attempt ID and stage name;
- requested capture and game IDs;
- stable learning run ID;
- payload hash;
- extracted claim count;
- unique claim-key count;
- inserted, unchanged, and conflict counts;
- claim counts by type/layer;
- zero-claim reason when applicable;
- start/finish time and duration;
- source table and target table; and
- whether a stage receipt was saved.

The claim count must reconcile three ways:

```text
extractor output
= unique staged claim keys
= inserted + unchanged rows for the selected capture
```

The receipt's `output_count` equals the canonical claim count for that capture,
not merely the number inserted during a retry.

## Required test ladder

### Pure and adapter tests

- missing capture fails closed;
- row/header game identity mismatch fails;
- payload hash mismatch fails;
- postgame leakage fails;
- canonical context uses snapshot lineage;
- all prepared rows have `run_id == learning_run_id`;
- snapshot-aware claim keys are deterministic;
- duplicate extracted keys fail before storage;
- every postgame/validation field is null;
- historical filesystem extraction behavior remains unchanged.

### Storage tests

- setup creates/verifies only the approved dev objects;
- storage refuses a production runtime;
- MERGE uses `learning_run_id + claim_key`;
- first write inserts only missing claims;
- identical retry changes zero rows;
- immutable conflict fails visibly;
- one game's write cannot delete another game's rows;
- stage result records the correct per-game counts.

### Handoff tests

- the populated ARI–LV-style snapshot produces the expected claim families;
- an honest empty-evidence snapshot can complete with zero claims;
- `lens_tags` and evidence context survive Packet 2 but are not mistaken for
  standalone user-facing claims;
- no internal `/game` HTTP call occurs; and
- no final-score or outcome query is introduced.

## Development-cloud proof sequence

Cloud work remains deliberately gated.

### Gate 1 — read-only inventory

Confirm:

- the six Packet 2 snapshots are unchanged;
- the selected populated capture is ARI–LV;
- at least one honest zero-claim/empty-evidence capture is available;
- the target table does not yet contain an unexpected cohort; and
- runtime targets are development-only.

### Gate 2 — table setup

Create or verify `GameLens_dev.claim_training_examples` idempotently. Stop
before inserting claims.

### Gate 3 — dry read

Read one selected canonical snapshot, validate it, run extraction in memory,
and print the exact claim summary. Write nothing.

### Gate 4 — dry write plan

Build and validate the rows and the intended MERGE result without executing
the mutation. Print expected inserted/unchanged/conflict counts.

### Gate 5 — deliberate populated-capture write

Write one populated capture. Read it back and reconcile identities, count,
hash, claim types, and null postgame fields.

### Gate 6 — identical replay

Run the identical populated capture again. Require zero inserts, zero updates,
zero conflicts, unchanged total count, and a truthful no-op/unchanged receipt.

### Gate 7 — zero-claim proof

Process one valid empty-evidence capture. Require zero claim rows and one
successful `zero_claims_extracted` game-level receipt. Repeat it and prove no
duplicate receipt or claim row.

### Gate 8 — bounded slate proof

Only after the one-game paths pass, process the remaining canonical Packet 2
captures sequentially. One failure must not discard or mutate another game's
result.

## Required evidence for Packet 3 GO

Packet 3 receives GO only when:

1. the companion plan was reviewed before implementation;
2. focused tests and relevant Packet 1–2 regressions pass;
3. the new code reads canonical snapshots rather than rebuilding GameLens;
4. every claim carries complete capture and cohort lineage;
5. the first populated write reconciles exactly;
6. the identical replay changes zero rows;
7. a zero-claim capture remains visibly processed;
8. historical filesystem QA remains available;
9. no postgame value enters a Level 1 row;
10. all writes remain in `GameLens_dev`;
11. `app.py`, production Scheduler behavior, `/game`, and the frontend remain
    unchanged; and
12. the document is updated with exact attempt IDs, counts, and evidence.

## What Packet 3 does not do

Packet 3 does not:

- wire Snapshot Capture or Level 1 after the 8:00 a.m. load;
- merge learning code to `main`;
- create production learning datasets;
- recapture or repair missed games;
- grade Matchup Lean or Model Trust;
- run Levels 2, 3, or 4;
- change claim language, confidence, picks, or frontend behavior;
- expose the dev claim table through Admin; or
- decide production retention and deployment topology.

Those responsibilities stay with their later packets.

## Decisions deliberately deferred

Packet 4 decides how final scores and accepted Facts admit a game to outcome
grading and Levels 2–3.

Packet 5 decides production operational-ledger retention and Admin
reconciliation.

Packet 6 decides controlled Level 4 batching and weekly reporting.

Packet 7 decides the production dataset, kill switch, final service/job
topology, 8:00 a.m. handoff, deployment, and rollback.

## Review checklist for Christian

Before implementation, confirm that this plan answers yes:

- Does Level 1 read only the frozen Packet 2 snapshot?
- Can every claim be traced back to one capture?
- Can a retry run without deleting or duplicating anything?
- Can a valid game with zero claims still be seen?
- Is the historical QA extractor preserved?
- Are all writes development-only?
- Are the 8:00 a.m. load and production app still untouched?

If those answers remain yes, Packet 3 implementation may begin in small,
separately tested commits.

## Documentation handoff

This plan is the next-chat starting point. A new chat should read, in order:

1. [Live documentation index](./README.md);
2. [Product Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md);
3. this Packet 3 plan;
4. [Packet 2 evidence](./GameLens_Packet_2_Shadow_Pregame_Snapshot_Plan.md);
5. [Packet 1 rulebook](./GameLens_Packet_1_Pregame_Capture_Contract.md); and
6. [architecture handoff](./GameLens_Product_Data_Collection_and_Learning_Handoff.md).

Then inspect the current `dev` versions of the extractor, table-setup module,
snapshot storage, learning contract, and handoff tests before changing code.
The plan governs the boundary; current code governs implementation details.
