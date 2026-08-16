# GameLens Calibrated Matchup Lean Hotfix

**Status:** Released to production and forward-merged to `dev` on 2026-08-16.
Revision `nfl-games-app-main-00155-qaf` serves 100% of normal traffic;
`nfl-games-app-main-00153-jol` is the rollback revision. No historical rebuild
or data write occurred.

**Temporary branch:** `agent/calibrated-matchup-lean-hotfix` — deleted locally and from GitHub after release  
**Base:** `main@7b54cead`  
**Production branch:** `main`  
**Long-lived development branch:** `dev`  
**Main merge:** `175e1d0b79ca5d7a61d606cfe08133dd836a95ee`  
**Dev forward merge:** `0b4d4ad93b6b617b0b6ea2871764353375e0e2c7`  
**Production build:** `896a7a28-e968-4d1d-a224-db5f1ede8d34`

## Purpose

Make the user-facing Matchup Lean confidence label more honest without changing
the selected team or rebuilding historical seasons.

The approved rule is:

```text
If outcome confidence is High and core_gap is below 0.45:
    display Medium
Otherwise:
    preserve the existing label
```

This is confidence calibration, not winner logic.

## One-source rule

```text
/game calls the shared calibration helper once
    -> the saved payload contains the effective confidence
    -> claim extraction copies the saved confidence
    -> Admin reports the stored confidence
```

Admin's existing 2025 calibrated preview remains historical research evidence.
It is not the production calculation owner. The normal Admin sections must
report persisted values from the selected run rather than independently
recalculate the live label.

## Explicit non-changes

The hotfix must preserve:

- target team and target side;
- Matchup Lean direction;
- profile type and profile-strength label;
- legacy raw signal confidence;
- Model Outcome and Model Trust;
- underlying metrics and Core Area values;
- the Week 1–2 Low-confidence cap; and
- the existing production ETL and learning-packet wiring.

It does not rebuild 2025, rewrite historical payloads, or write BigQuery rows.

## Focused implementation

- `services/confidence_calibration.py` owns the rule and version metadata.
- `services/game_service.py` calls the helper after existing outcome
  confidence is formed.
- `qa_calibrated_matchup_lean_parity.py` reads the existing historical
  claim table and applies the same helper in memory.
- Focused tests cover the threshold, preservation rules, integration,
  Week 1–2 precedence, visual reconciliation, and visible conflicts.

## Visual 2025 proof

Run from the hotfix branch:

```bash
python qa_calibrated_matchup_lean_parity.py
```

This command is read-only. It does not call `/game`, run ETL, replace a run,
or write BigQuery.

Live output recorded on 2026-08-16:

```text
CALIBRATED MATCHUP LEAN — READ-ONLY PARITY QA
------------------------------------------------
run_id: full_2025_reg_post_claim_matrix_pilot
source: nfl-stream-406420.Analytics.gamelens_claim_training_examples

Source                       Low    Medium    High    Total
Stored 2025 baseline         154        93      22      269
Shared helper result         154       103      12      269
Admin benchmark              154       103      12      269

High → Medium moves: 10
Conflicting game rows: 0
RESULT: PASS
```

This confirms that the shared production helper reproduces the approved Admin
benchmark across all 269 historical games without modifying the historical
table. A mismatch, unexpected label, or conflicting per-game value returns
`RESULT: FAIL` and stops the release.

## Automated test proof

Focused command:

```bash
python -m pytest \
  tests/test_confidence_calibration.py \
  tests/test_game_service_confidence_calibration.py \
  tests/test_qa_calibrated_matchup_lean_parity.py \
  -q
```

Recorded focused result on 2026-08-16:

```text
7 passed, 6 subtests passed in 2.22s
```

Full repository regression command:

```bash
python -m pytest -q
```

Recorded full result on 2026-08-16:

```text
123 passed, 77 subtests passed in 10.61s
```

The tests verify the 0.45 boundary, unchanged labels outside the rule,
preservation of the pick and raw confidence, Week 1–2 precedence, exact visual
reconciliation, visible failure on conflicting source rows, and compatibility
with the complete main-based test suite.

## Production release receipt

| Check | Recorded result |
|---|---|
| Pull request | [#6](https://github.com/csells10/meow/pull/6), merged to `main` |
| Main merge | `175e1d0b79ca5d7a61d606cfe08133dd836a95ee` |
| Cloud Build | `896a7a28-e968-4d1d-a224-db5f1ede8d34`, `SUCCESS` |
| Candidate revision | `nfl-games-app-main-00155-qaf`, initially 0% normal traffic |
| Rollback revision | `nfl-games-app-main-00153-jol` |
| Candidate health | HTTP 200 |
| Live representative QA | `20250928_GB@DAL`: High → Medium at `core_gap=0.29`; pick, profile, raw signal, Model Outcome, and Model Trust unchanged |
| Candidate errors | None found in the bounded log review |
| Promotion | `00155-qaf` deliberately moved to 100% normal traffic |
| Production health | HTTP 200 after promotion |
| Post-promotion errors | None found for `00155-qaf` in the 15-minute review window |
| Main → dev | [#7](https://github.com/csells10/meow/pull/7), merge `0b4d4ad93b6b617b0b6ea2871764353375e0e2c7` |
| Dev forward-merge build | Source commit `0b4d4ad93b6b617b0b6ea2871764353375e0e2c7`, `SUCCESS` |
| Dev release-documentation build | Source commit `43e2f824860f69a9daeb4cda05fc8fb310f378ce`, `SUCCESS` |
| Temporary branch cleanup | Local and GitHub branches deleted |
| Historical writes | None; the 2025 table was read only |

## Release sequence

1. Run the focused tests and the live read-only 2025 parity command. **Complete.**
2. Review the branch diff against current `main` and run the full regression
   suite. **Complete.**
3. Merge the approved branch into `main`. **Complete.**
4. Allow the existing Cloud Build configuration to create a 0%-traffic
   candidate. **Complete.**
5. Validate health, representative `/game` behavior, revision identity, and
   rollback anchor. **Complete.**
6. Promote traffic only after an explicit decision. **Complete.**
7. Merge released `main` forward into `dev`; do not reimplement the rule.
   **Complete.**
8. Delete the temporary branch only after both long-lived branches contain the
   identical released code. **Complete.**

## Current evidence

- Live read-only BigQuery parity passed for all 269 games.
- The shared helper exactly matched the Admin benchmark:
  Low 154 / Medium 103 / High 12.
- Ten High labels moved to Medium, with zero conflicting game rows.
- Seven focused tests and six subtests passed locally in 2.22 seconds.
- The full repository suite passed: 123 tests and 77 subtests in 10.61 seconds.
- Python compilation passes for all changed Python files.
- Pull request #6 merged to `main`; pull request #7 forward-merged the
  released code into `dev`.
- Both resulting `dev` Cloud Builds completed successfully, and the temporary
  hotfix branch was removed locally and from GitHub.
- The candidate and normal production URL returned HTTP 200.
- The live representative comparison passed every preservation check.
- No severity-ERROR entries were found in the bounded candidate or
  post-promotion log reviews.
- No historical data was rebuilt, rewritten, or inserted.

This document records a completed release. Any later confidence-rule change
requires a new reviewed and traceable release.
