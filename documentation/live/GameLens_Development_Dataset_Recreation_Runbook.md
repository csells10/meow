# GameLens Development Dataset Recreation Runbook

**Status:** Active development schema and go-day planning reference  
**Created:** 2026-08-18  
**Repository:** `csells10/meow`  
**Branch represented:** `dev`  
**Current dataset:** `nfl-stream-406420.GameLens_dev`  
**Current table count:** 6  
**Scope:** Recreate or verify the empty Packet 1–4 development structures from code; record Packet 5's no-new-table reconciliation; define the production-migration requirement that Packet 8 must satisfy
**Production authorization:** None. Every current setup entry point fails closed outside `GAMELENS_ENVIRONMENT=dev`.

---

## 1. The short version

`GameLens_dev` is now a six-table evidence system. The table definitions do
not live in this document and should never be copied by hand into the BigQuery
console. Python schema functions and setup entry points in the repository are
the authority.

For a new, empty development dataset, run the five setup entry points below in
order. The first entry point creates the dataset in the same location as the
configured League source and creates three Packet 2 tables. The next four
steps create the claim table, add the Packet 4 Level 3 fields, create the game
grade table, and create the Packet 4 receipt table.

These commands create or verify **structure only**. They do not restore
captures, claims, grades, or receipts. Historical evidence must be preserved
or migrated separately; it must never be reconstructed from postgame data.

## 2. Current six-table inventory

| Order | Table | Grain / logical identity | Fields after Packet 4 | Partition | Clustering | Code-owned schema/setup |
|---:|---|---|---:|---|---|---|
| 1 | `pregame_snapshots` | One immutable `capture_id`; canonical capture is selected by game/cohort rules | 22 | `captured_at` | `game_id, season_type` | `services/gamelens_snapshot_storage.py::pregame_snapshot_schema`; `setup_gamelens_snapshot_tables.py` |
| 2 | `stage_runs` | One attempt-level stage receipt, identified by `attempt_id + stage_name` | 13 | none | none | `services/gamelens_snapshot_storage.py::stage_run_schema`; `setup_gamelens_snapshot_tables.py` |
| 3 | `stage_game_results` | One game-stage result per `attempt_id + stage_name + game_id` | 17 | `recorded_at` | `attempt_id, game_id, status` | `services/gamelens_snapshot_storage.py::stage_game_result_schema`; `setup_gamelens_snapshot_tables.py` |
| 4 | `claim_training_examples` | One deterministic claim per `learning_run_id + claim_key`, linked to `capture_id` | 132 | `game_date` | `learning_run_id, game_id, claim_type, claim_layer` | base schema from `services/gamelens_claim_storage.py`, then 15 Level 3 fields from `services/gamelens_level3_storage.py`; two setup steps |
| 5 | `game_model_outcomes` | One frozen-capture grade per `learning_run_id + capture_id` | 13 | `graded_at` | `learning_run_id, game_id, grade_version` | `services/gamelens_postgame_storage.py::postgame_outcome_schema`; `setup_gamelens_postgame_outcome_table.py` |
| 6 | `postgame_learning_stage_receipts` | One immutable receipt per `attempt_id + receipt_scope + game_id + stage_name` | 31 | `recorded_at` | `attempt_id, game_id, stage_name, status` | `services/gamelens_packet4_receipts.py::packet4_receipt_schema`; `setup_gamelens_packet4_receipts.py` |

Field counts are a dated Packet 4 checkpoint. Setup code and its schema
verification remain authoritative when later packets add approved fields.

## 3. Dependency order

```text
configured League source dataset
    -> create GameLens_dev in the same BigQuery location
    -> pregame_snapshots
    -> stage_runs
    -> stage_game_results
    -> claim_training_examples base schema (117 fields)
    -> add Level 3 evidence fields (117 -> 132)
    -> game_model_outcomes
    -> postgame_learning_stage_receipts
```

The outcome and Packet 4 receipt tables do not depend on claim-table data, but
the order above mirrors packet ownership and makes a clean rebuild easy to
audit.

Important: `setup_gamelens_claim_table.py` verifies the Packet 3 base schema.
On a fresh namespace it must run before
`setup_gamelens_level3_columns.py`. After the 15-field migration, the
Level 3 setup is the final claim-table verification for the Packet 4 shape.
Do not rerun the base verifier against the already expanded 132-field table and
interpret its expected mismatch as data corruption.

## 4. Development recreation procedure

### Preconditions

1. Work from the intended reviewed `dev` commit.
2. Authenticate with an identity that may create/update tables only in the
   approved development project and dataset.
3. Set `GAMELENS_ENVIRONMENT=dev` and the current required runtime variables.
4. Confirm `GAMELENS_PROJECT_ID` and the configured League source dataset.
5. Confirm the target is exactly `<project>.GameLens_dev`.
6. Confirm whether the target namespace is empty. This sequence creates and
   verifies; it does not drop or truncate.

Run:

```bash
python setup_gamelens_snapshot_tables.py
python setup_gamelens_claim_table.py
python setup_gamelens_level3_columns.py --confirm-dev-schema-update
python setup_gamelens_postgame_outcome_table.py
python setup_gamelens_packet4_receipts.py --confirm-dev-setup
```

Each command prints a small JSON receipt to standard output. Redirection such
as `> setup_receipt.json` is optional local shell behavior; the Python code
does not save JSON files to the repository. In Cloud Run or a Cloud Run Job,
standard output becomes Cloud Logging unless an operator deliberately redirects
it to a mounted or local file.

### Expected results

- the dataset exists in the League source dataset's BigQuery location;
- all six tables exist;
- the final claim table contains the 15 approved Level 3 fields and has the
  Packet 4 shape;
- tables created in the clean namespace receive the code-owned partition and
  clustering settings;
- a repeat of an idempotent setup step reports no structural change;
- no evidence row is inserted, updated, deleted, or reconstructed; and
- every setup refuses a non-development runtime.

### Current verification coverage

The setup entry points do not all verify the same amount of an already
existing table:

| Setup | Existing-object verification today |
|---|---|
| Snapshot/Packet 2 setup | Exact schemas for all three tables; creation code sets the snapshot and game-result layouts, but the verifier does not currently compare existing partition/clustering metadata |
| Packet 3 claim setup | Exact base schema plus partition and clustering |
| Level 3 additive setup | Required field presence and compatible types |
| Packet 4 outcome setup | Exact schema plus partition and clustering |
| Packet 4 receipt setup | Exact schema; creation code sets partition/clustering, but the verifier does not currently compare existing layout metadata |

Therefore a clean-namespace rehearsal proves the intended creation layout. For
an existing namespace, the read-only inventory must explicitly inspect
partition and clustering metadata as well as fields. Packet 8's production
migration entry point must close this verification gap before go-day; do not
claim a complete production schema verification from the current dev setup
receipts alone.

## 5. Verification

Use code-owned setup results plus a read-only BigQuery inventory. At minimum,
record:

```text
project
dataset
dataset location
reviewed Git commit
table name
field count
partition field
clustering fields
setup status
verification timestamp
```

The Packet 4 inventory command is safe only with its explicit read-only gate:

```bash
python qa_gamelens_packet4_schema_inventory.py \
  --dev-read-only \
  --game-id <known-development-game-id>
```

That command also reads upstream score, Facts, and existing Analytics
dependencies for the selected game. Use it as a handoff/admission diagnostic,
not as a schema migration and not as proof that every six-table row count is
correct.

Packet 5 reconciled table grains and identities before building Admin reads.
`stage_runs`, `stage_game_results`, and
`postgame_learning_stage_receipts` overlap operationally but serve different
packet-era receipt contracts. The protected endpoint now derives its views
from the six tables without consolidating them or adding storage. Packet 6
must preserve that decision unless a concrete, reviewed gap is proven.

## 6. Structure recreation is not evidence restoration

The setup entry points intentionally do not:

- copy rows from another dataset;
- replay a missed pregame capture;
- regenerate `capture_id`, `claim_key`, or hashes;
- infer claims from a final-game payload;
- backfill grades without the canonical capture;
- rebuild attempt receipts from logs; or
- drop, truncate, or replace an existing table.

If a go-day namespace needs historical data, create a separately reviewed
migration/backfill plan with source and destination row counts, hashes or
logical-key reconciliation, immutable lineage preservation, dry-run output,
an idempotent retry, and rollback. A missing pregame capture remains
`capture_missing`; it is not a candidate for restoration.

## 7. Production go-day requirement

The current scripts are deliberately development-only. Packet 8 owns the
production dataset name, service/job topology, IAM, retention, activation, and
rollback. Before production learning can be enabled, Packet 8 must provide one
reviewed migration entry point or deployment step that:

1. imports the same schema functions used by the development setup code;
2. accepts an explicitly approved production dataset rather than hard-coding
   `GameLens_dev`;
3. creates the dataset in the approved source location;
4. applies versioned migrations in dependency order;
5. verifies schema, partitioning, and clustering after every step;
6. fails closed on an unexpected existing schema;
7. emits a compact migration receipt to Cloud Logging;
8. never drops or truncates by default;
9. can run before application traffic is enabled; and
10. is proven first against a clean disposable development namespace.

Production activation must stop if the migration receipt, runtime identity,
dataset location, table inventory, or code commit does not match the approved
release record. Creating tables is a separate gate from enabling learning
writes.

## 8. Go-day sequence

```text
approve production names, location, IAM, retention, and commit
    -> run schema migration with learning disabled
    -> verify six-table inventory and permissions
    -> migrate approved historical evidence, if any, under a separate plan
    -> reconcile source/destination counts and logical identities
    -> deploy the learning-capable revision at 0% normal traffic
    -> run read-only/shadow checks
    -> enable the smallest approved write boundary
    -> follow one game through capture, Level 1, grade, Levels 2–3, receipts
    -> keep the kill switch and prior serving revision as rollback
```

Do not make a production service create missing tables lazily during a normal
game request or Scheduler run. Schema migration is a pre-activation release
gate.

## 9. JSON and Artifact Registry hygiene

The Packet 4 CLIs print JSON to stdout. The many `packet4_*.json` files used
during manual proof existed only because the shell redirected stdout into the
repository working directory.

Current protections:

- `.gitignore` excludes `*.json` from Git;
- `.dockerignore` excludes `*.json` from Docker build context; and
- `.gcloudignore` excludes `*.json` from local Cloud Build upload context.

Therefore these proof files are not part of new container images or local
Cloud Build source uploads. Artifact Registry image cost is driven by image
layers and retention, not by BigQuery tables; BigQuery storage cost is
separate. Keep only durable evidence in BigQuery and documentation, delete
temporary local `packet4_*.json` files after review, and manage old container
images through an explicit Artifact Registry cleanup policy rather than
deleting database evidence.

## 10. Ownership by future packet

| Decision | Owner |
|---|---|
| Reconcile the six current tables with protected Admin and existing ledgers | Packet 5 — complete without new storage |
| Rehearse the existing Packet 2–4 workers end to end | Packet 6 — no new table planned |
| Add Level 4 / weekly structures only after their plan and grain are approved | Packet 7 |
| Approve production dataset names, migration entry point, IAM, retention, activation, and rollback | Packet 8 |
| Change a Packet 1–5 schema | The packet that needs the change, with this inventory and setup sequence updated in the same commit |

## 11. Closure checklist

- [ ] Reviewed commit recorded.
- [ ] Runtime is explicitly development for current setup scripts.
- [ ] Project, dataset, and location verified.
- [ ] Six expected tables present.
- [ ] Final claim-table Level 3 migration verified.
- [ ] Partitions and clustering match code.
- [ ] No setup step changed evidence rows.
- [ ] Any data migration has its own approved reconciliation receipt.
- [x] Packet 5 used the table grains and added no summary storage.
- [ ] Packet 6 preserved the six-table grains during its development rehearsal.
- [ ] Packet 8 supplied and rehearsed a production-safe migration entry point.
- [ ] Learning remained disabled until schema and IAM verification passed.
