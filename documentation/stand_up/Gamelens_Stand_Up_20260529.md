Tomorrow Plan — Core Area Durability Confidence Calibration
Current State

Today we completed the first full audit cycle for the new confidence-calibration idea:

core_area_durability_context_v0

This feature is currently audit-only.

It does not change:

/game API behavior
winner pick
matchup lean
profile_type
Model Trust
frontend copy
production confidence labels
BigQuery runtime tables

The purpose is to test whether High Confidence should be harder to earn when the model has a loud signal but weaker Core Area durability.

The core idea:

Same pick.
Same lean.
More honest confidence label.
What Was Tested

We tested an audit-only simulation:

If current confidence = High
and core_gap < 0.45
then simulated confidence = Medium

Otherwise keep current confidence unchanged.

This is not a production rule yet.

The test was run against existing game-level confidence audit CSVs for:

2023
2024
2025

using the new script:

agg/gamelens_training/build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim.py

New output files:

core_durability_confidence_simulation_games.csv
core_durability_confidence_ladder_summary.csv
core_durability_confidence_action_summary.csv
Main Finding

The simulation behaved well across all three seasons.

Season-by-season impact
Season	Current High	Simulated High	Lift	Current Medium	Simulated Medium	Medium Change
2023	64.29%	72.73%	+8.44	62.77%	62.16%	-0.61
2024	68.33%	79.17%	+10.84	67.42%	65.60%	-1.82
2025	57.14%	75.00%	+17.86	64.52%	61.76%	-2.76
Pooled 2023–2025 impact
Label	Current games	Current correct %	Simulated games	Simulated correct %
High	110	65.14%	47	76.60%
Medium	276	64.86%	339	63.31%
Low	421	53.99%	421	53.99%

High-specific pooled result:

Current High:
110 games
65.14% correct
57.82% claim validation
4 severe misses
26 close misses

Simulated High:
47 games
76.60% correct
62.52% claim validation
1 severe miss
9 close misses
Plain-English interpretation

The simulation made High Confidence:

smaller
cleaner
more accurate
better validated
less severe-miss prone

Medium absorbed the softened High games without collapsing.

Low stayed unchanged.

This directly addresses the original Admin-page concern:

High Confidence was not clearly behaving better than Medium Confidence.
Important Guardrails

This is not ready for production.

Do not yet wire into:

game_service.py
model_trust_service.py
/game API
frontend
Admin UI
runtime confidence labels

Also do not treat this as:

core_gap < 0.45 = bad pick

The correct interpretation is:

core_gap < 0.45 may mean the lean is still usable,
but it may not deserve High Confidence.

The threshold is currently a research threshold, not a football law.

Why This Is Not Kitchen Soup

The current simulation is intentionally narrow:

Only High → Medium softening is tested.
Medium → Low is not tested.
Medium → High is not tested.
Low is untouched.
Winner picks are untouched.
Profile type is untouched.

That keeps the feature focused on the original problem:

High Confidence should earn the High label.

Recommended Next Steps
Step 1 — Commit the audit-only script

Commit the new file:

agg/gamelens_training/build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim.py

Suggested commit message:

Add core durability confidence simulation audit

Longer message:

Add v0.1.3 confidence calibration audit script for Core Area durability simulation.

This introduces an audit-only confidence simulation that softens current High Confidence games to Medium when core_gap is below the durability review threshold. The simulation preserves winner picks, matchup lean, profile type, runtime behavior, BigQuery state, and frontend output.

New outputs include simulated confidence game assignments, current vs simulated confidence ladder summaries, and calibration action summaries. This is intended for review-only confidence calibration testing and does not introduce production confidence rules.
Step 2 — Run v0.1.3 from BigQuery source

CSV mode passed. Next we should confirm the script works directly from BigQuery rows.

python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2023_reg_post_claim_matrix_pilot \
  --season 2023 \
  --dry-run
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --season 2024 \
  --dry-run
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2025_reg_post_claim_matrix_pilot \
  --season 2025 \
  --dry-run

Goal:

Confirm the new audit script works as a real audit worker,
not only as a CSV post-processor.
Step 3 — Human-review softened games

Review games where:

calibration_action_v0 = soften_high_to_medium

Use this to inspect:

import pandas as pd
from pathlib import Path

runs = {
    "2023": "test_2023_core_durability_sim",
    "2024": "test_2024_core_durability_sim",
    "2025": "test_2025_core_durability_sim",
}

base = Path("qa/gamelens_confidence_calibration_audit_v0_1_3_runs")
frames = []

for season, run_id in runs.items():
    p = base / run_id / "core_durability_confidence_simulation_games.csv"
    df = pd.read_csv(p)
    df["season"] = season
    frames.append(df)

out = pd.concat(frames, ignore_index=True)
softened = out[out["calibration_action_v0"] == "soften_high_to_medium"].copy()

cols = [
    "season",
    "game_id",
    "model_result_normalized",
    "final_margin_abs",
    "confidence_group",
    "simulated_confidence_group_v0",
    "core_gap",
    "core_area_durability_band",
    "signal_gap",
    "profile_strength_label",
    "profile_type",
    "actual_claim_validation_rate",
    "audit_failure_tags",
    "calibration_reason_v0",
]

print(
    softened[cols]
    .sort_values(["season", "model_result_normalized", "core_gap"])
    .to_string(index=False)
)

Questions to answer:

Were softened incorrect games truly overconfident?
Were softened correct games still acceptable as Medium?
Were any softened games obvious Highs that the rule unfairly punished?
Step 4 — Human-review retained High misses

Review games that stayed High but were still incorrect:

retained_high_misses = out[
    (out["simulated_confidence_group_v0"] == "High") &
    (out["model_result_normalized"] == "incorrect")
]

Questions:

Were these mostly close misses?
Were any severe misses?
Did claim validation collapse?
Do these suggest a future second guardrail?

Important:

Do not add a second guardrail yet.
Document first.
Step 5 — Consider Admin-only visibility later

If human review still supports the feature, the next safe product step is an Admin/debug section:

High Confidence Durability Review

Suggested groups:

Current High
Simulated Retained High
Softened High → Medium

Suggested columns:

games
graded games
correct %
claim validation %
severe misses
close misses
avg core_gap
avg signal_gap
avg team_comp_edge_score

No frontend user-facing change yet.

Stopping Point Recommendation

Tomorrow should not start with production implementation.

Tomorrow should start with:

1. Commit audit-only script.
2. Verify BigQuery-source mode.
3. Review softened games.
4. Review retained High misses.
5. Decide whether this deserves Admin-only visibility.

The working conclusion:

core_area_durability_context_v0 looks promising as an audit-supported confidence calibration feature.

It improves High Confidence quality across 2023–2025 while keeping Medium usable and Low unchanged.

But it should stay audit-only until human review confirms the softened and retained groups make football/product sense.
One-Line Handoff
We found a promising audit-only confidence calibration feature: High Confidence games with insufficient Core Area durability can be softened to Medium without changing picks. Across 2023–2025, simulated High improved from 65.14% to 76.60%, claim validation improved, severe misses dropped, Medium stayed usable, and Low was unchanged. Next step is BigQuery-source verification plus human review of softened and retained High games before any production/API/frontend work.