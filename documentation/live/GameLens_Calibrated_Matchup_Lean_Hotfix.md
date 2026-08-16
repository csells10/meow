# GameLens Calibrated Matchup Lean Hotfix

**Status:** Isolated main-based branch implemented; draft review and live
read-only parity QA remain. No merge, build, deployment, traffic promotion, or
data write has occurred.

**Branch:** `agent/calibrated-matchup-lean-hotfix`  
**Base:** `main@7b54cead`  
**Production branch:** `main`  
**Long-lived development branch:** `dev`

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

Expected output:

```text
Source                       Low    Medium    High    Total
Stored 2025 baseline         154        93      22      269
Shared helper result         154       103      12      269
Admin benchmark              154       103      12      269

High → Medium moves: 10
Conflicting game rows: 0
RESULT: PASS
```

A mismatch, unexpected label, or conflicting per-game value must return
`RESULT: FAIL` and stop the release.

## Release sequence

1. Run the focused tests and the live read-only 2025 parity command.
2. Review the branch diff against current `main`.
3. Merge the approved branch into `main`.
4. Allow the existing Cloud Build configuration to create a 0%-traffic
   candidate.
5. Validate health, representative `/game` behavior, revision identity, and
   rollback anchor.
6. Promote traffic only after an explicit decision.
7. Merge released `main` forward into `dev`; do not reimplement the rule.
8. Delete the temporary branch only after both long-lived branches contain the
   identical released commit.

## Current evidence

- Seven focused tests pass locally.
- Python compilation passes for all changed Python files.
- Synthetic visual reconciliation produces the expected
  `154 / 103 / 12` result.
- The branch is four commits ahead of `main` and contains only the shared
  helper, `/game` integration, focused tests, and read-only QA.
- Live read-only BigQuery parity remains the next gate.

No merge or deployment is authorized by this document alone.
