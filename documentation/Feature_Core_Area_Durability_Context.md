GameLens Confidence Calibration Feature Guide — core_area_durability_context_v0

Last updated: May 28, 2026

1. Executive Summary

core_area_durability_context_v0 is a new audit-only confidence calibration feature candidate for GameLens.

Its purpose is to help answer:

When GameLens labels a matchup as High Confidence, is the broader Core Area foundation durable enough to justify that label?

This feature is not winner prediction.

It is also not a new football metric, not matchup lean, not Model Trust, not frontend behavior, and not production confidence logic yet.

Current status:

Feature type: confidence calibration metadata
Current stage: audit-only
Production use: disabled
/game API exposure: not added
Frontend usage: none
Runtime confidence changes: none
Winner-pick changes: none

The feature currently exists to test whether some current High Confidence games should be softened to Medium Confidence when the directional signal is loud but broader Core Area support is not durable enough.

The most important product idea:

Same pick.
Same matchup lean.
More honest confidence label.
2. Why This Feature Exists

The original problem came from Admin page review.

The concern was:

High Confidence games were not clearly outperforming Medium Confidence games.

That is a confidence calibration problem.

GameLens can have a valid lean but still speak too loudly.

For example:

Team A edge / High Confidence

may be too strong if:

signal_gap is loud
profile_type is confirmed_edge
profile_strength is Strong Profile
but core_gap is not durable

In that case, the better product read may be:

Team A edge / Medium Confidence
Reason: signal profile is loud, but Core Area durability is not strong enough for High.

This does not mean the matchup lean was wrong.

It means the confidence label may have been too aggressive.

3. What This Feature Is
Feature name
core_area_durability_context_v0
Core idea

core_gap measures how much separation exists in the broader Core Area read.

The first audit version uses this durability threshold:

core_gap >= 0.45

as the review point for whether a current High Confidence game should remain High.

First simulation rule
If current confidence = High
and core_gap < 0.45
then simulated confidence = Medium

Otherwise keep current confidence unchanged
Pseudocode
def simulate_confidence(row):
    if row["confidence_group"] == "High" and row["core_gap"] < 0.45:
        return "Medium"
    return row["confidence_group"]

This rule is intentionally narrow.

It only tests:

High → Medium softening

It does not test:

Medium → Low
Medium → High
Low → Medium
winner changes
matchup lean changes
profile_type changes
4. What This Feature Is Not

This feature does not:

pick winners
change predicted_team
change matchup_lean
change profile_type
change profile_strength_label
change Model Trust
change frontend copy
rewrite /game API responses
write to BigQuery
automatically downgrade production confidence
automatically promote Medium games

It also does not mean:

core_gap < 0.45 = bad pick

A game below the threshold can still be a valid lean.

The feature only asks:

Should that lean really be labeled High Confidence?

5. Why Not Use Conflicting Profile?

A tempting idea was:

If a High Confidence confirmed edge performs poorly, maybe it should become Conflicting Profile.

That is not the right boundary.

profile_type = confirmed_edge should describe the pregame directional shape.

It answers:

Did the pregame evidence point clearly toward one side?

outcome_confidence_label answers:

How loudly should GameLens speak about that side?

A poor result does not automatically mean the profile was conflicting.

The better future state is:

profile_type = confirmed_edge
profile_strength_label = Strong Profile
outcome_confidence_label = Medium
calibration_reason = insufficient_core_area_durability_for_high

That preserves the matchup read while softening the confidence.

6. New Audit Fields

The v0.1.3 audit script adds or formalizes these fields.

Game-level durability fields
core_area_durability_band
core_area_durability_sort
Current band logic
unknown         sort 0
weak            core_gap < 0.35
borderline      0.35 <= core_gap < 0.40
durable         0.40 <= core_gap < 0.45
strong_durable  0.45 <= core_gap < 0.50
very_strong     core_gap >= 0.50
Simulation fields
simulated_confidence_group_v0
calibration_action_v0
calibration_reason_v0
Example values
{
  "confidence_group": "High",
  "simulated_confidence_group_v0": "Medium",
  "calibration_action_v0": "soften_high_to_medium",
  "calibration_reason_v0": "high_signal_but_insufficient_core_area_durability",
  "core_gap": 0.39,
  "core_area_durability_band": "borderline"
}
7. Script Added

New script:

agg/gamelens_training/build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim.py

This script is a uniquely named v0.1.3 audit worker so it does not overwrite the existing v0.1.2 script.

New output folder pattern
qa/gamelens_confidence_calibration_audit_v0_1_3_runs/<run_id>/
New output files
core_durability_confidence_simulation_games.csv
core_durability_confidence_ladder_summary.csv
core_durability_confidence_action_summary.csv
Important

This script is audit-only.

It does not change runtime behavior.

8. How To Run The Simulation From Existing CSVs

This was the first testing path.

It uses already-built v0.1.2 audit CSVs.

2025
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id test_2025_core_durability_sim \
  --season 2025 \
  --input-game-csv qa/gamelens_confidence_calibration_audit_v0_1_2_runs/full_2025_reg_post_claim_matrix_pilot/game_level_confidence_audit.csv \
  --dry-run
2024
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id test_2024_core_durability_sim \
  --season 2024 \
  --input-game-csv qa/gamelens_confidence_calibration_audit_v0_1_2_runs/full_2024_reg_post_claim_matrix_pilot/game_level_confidence_audit.csv \
  --dry-run
2023
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id test_2023_core_durability_sim \
  --season 2023 \
  --input-game-csv qa/gamelens_confidence_calibration_audit_v0_1_2_runs/full_2023_reg_post_claim_matrix_pilot/game_level_confidence_audit.csv \
  --dry-run
9. How To Inspect A Single Simulation Run

Example for 2025:

python - <<'PY'
import pandas as pd

p = "qa/gamelens_confidence_calibration_audit_v0_1_3_runs/test_2025_core_durability_sim/core_durability_confidence_ladder_summary.csv"
df = pd.read_csv(p)

cols = [
    "ladder_version",
    "confidence_group",
    "games",
    "graded_games",
    "correct_game_pct",
    "claim_validation_pct",
    "severe_misses",
    "close_misses",
    "avg_core_gap",
    "avg_signal_gap",
]

print(df[cols].to_string(index=False))
PY

This compares:

current confidence ladder
vs
simulated_core_area_durability_v0 ladder
10. Evidence From Testing

The simulation was tested across:

2023
2024
2025

using the rebuilt yearly data.

Data rebuilt
Season	Payloads collected	Claim rows	Games scored	High Confidence games
2023	285	8,078	269	28
2024	285	8,129	269	60
2025	existing full run	8,067	269	22

The 2023 and 2024 rows were rebuilt through the full pipeline:

Payload collection
Stage 1 claim extraction
Stage 1 BigQuery write
Stage 2 validation
Stage 2 BigQuery write
Stage 3 features
Stage 3 BigQuery write
Confidence audit
11. Season-by-Season Results
2023
Label	Current correct %	Simulated correct %
High	64.29%	72.73%
Medium	62.77%	62.16%
Low	54.24%	54.24%

Interpretation:

High improved by +8.44 points.
Medium barely changed.
Low stayed unchanged.

This mattered because 2023 was the season that made the fixed threshold idea look less clean. The simulation still behaved reasonably.

2024
Label	Current correct %	Simulated correct %
High	68.33%	79.17%
Medium	67.42%	65.60%
Low	56.52%	56.52%

Interpretation:

High improved by +10.84 points.
Medium dropped only -1.82 points.
Low stayed unchanged.

2024 was the largest High Confidence sample, with 60 High games.

This was the strongest stress test.

2025
Label	Current correct %	Simulated correct %
High	57.14%	75.00%
Medium	64.52%	61.76%
Low	51.72%	51.72%

Interpretation:

High improved by +17.86 points.
Medium dropped -2.76 points but remained usable.
Low stayed unchanged.

2025 was the original pain point season.

12. Compact Cross-Season Impact
Season	Current High	Sim High	Lift	Current Medium	Sim Medium	Medium Change
2023	64.29%	72.73%	+8.44	62.77%	62.16%	-0.61
2024	68.33%	79.17%	+10.84	67.42%	65.60%	-1.82
2025	57.14%	75.00%	+17.86	64.52%	61.76%	-2.76

Main conclusion:

High improved every season.
Medium stayed usable.
Low stayed unchanged.

That is exactly the desired behavior for a targeted High Confidence calibration feature.

13. Pooled 2023–2025 Result

Across all three seasons:

Label	Current games	Current correct %	Sim games	Sim correct %
High	110	65.14%	47	76.60%
Medium	276	64.86%	339	63.31%
Low	421	53.99%	421	53.99%
High-specific pooled comparison
Metric	Current High	Simulated High
Games	110	47
Correct %	65.14%	76.60%
Claim validation %	57.82%	62.52%
Severe misses	4	1
Close misses	26	9
Interpretation

The simulation made High:

smaller
cleaner
more accurate
better validated
less severe-miss prone

Medium absorbed the softened High games and remained usable.

Low was unchanged.

14. Why This Result Is Important

This feature directly addresses the original issue.

Original problem:

High Confidence was not clearly better than Medium.

After simulation:

High becomes meaningfully better than Medium.

Current pooled ladder:

High:   65.14%
Medium: 64.86%
Low:    53.99%

Simulated pooled ladder:

High:   76.60%
Medium: 63.31%
Low:    53.99%

That is a healthier confidence ladder.

The model is not changing the pick.

It is making the confidence label more honest.

15. What This Proves

This testing supports:

core_area_durability_context_v0 is useful audit-supported confidence calibration metadata.

It also supports:

High Confidence should probably require durable Core Area support.

It does not prove:

core_gap < 0.45 should be a permanent production rule.

The threshold is currently a testing threshold.

It should not be treated as a football law.

16. Why This Should Not Be Production Yet

There are still important cautions.

16.1 The threshold is fixed

The current test uses:

core_gap < 0.45

This worked well across 2023–2025, but 2026 could behave differently.

Do not overfit the number.

16.2 Some softened games were still correct

The below-threshold High group still won more than half the time.

So this is not saying:

below 0.45 = bad pick

It is saying:

below 0.45 = may not deserve High Confidence
16.3 Human review still needed

Before production, the softened games should be reviewed manually.

We need to confirm:

Did these games feel like High labels were too loud?
Did the confidence softening make product sense?
Were correct softened games still reasonable as Medium?
Were retained High misses mostly close or explainable?
17. How To Review Softened Games

Use this script after running the v0.1.3 simulations.

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

Questions to ask:

Were softened incorrect games truly overconfident?
Were softened correct games still acceptable as Medium?
Were any softened games obvious Highs that the rule unfairly punished?
18. How To Review Retained High Misses

Use this to inspect games that stayed High but were still wrong.

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

retained_high_misses = out[
    (out["simulated_confidence_group_v0"] == "High") &
    (out["model_result_normalized"] == "incorrect")
].copy()

cols = [
    "season",
    "game_id",
    "final_margin_abs",
    "core_gap",
    "core_area_durability_band",
    "signal_gap",
    "team_comp_edge_score",
    "actual_claim_validation_rate",
    "audit_failure_tags",
]

print(
    retained_high_misses[cols]
    .sort_values(["season", "final_margin_abs"])
    .to_string(index=False)
)

Questions to ask:

Are retained High misses mostly close?
Are any retained High misses severe?
Do they have weak claim validation?
Do they suggest a second guardrail later?

Do not add a second guardrail yet.

Document first.

19. How To Run From BigQuery Source

CSV test mode already worked.

Next, verify the script works from BigQuery source too.

2023
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2023_reg_post_claim_matrix_pilot \
  --season 2023 \
  --dry-run
2024
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --season 2024 \
  --dry-run
2025
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2025_reg_post_claim_matrix_pilot \
  --season 2025 \
  --dry-run

This confirms the v0.1.3 script can operate as a real audit worker, not only as a CSV post-processor.

20. Suggested Admin Use Later

A future Admin tab could include:

High Confidence Durability Review
Suggested rows
Current High
Simulated Retained High
Softened High → Medium
Suggested columns
games
graded games
correct %
claim validation %
severe misses
close misses
avg core_gap
avg signal_gap
avg team_comp_edge_score

This would let the Admin page answer:

Is High Confidence earning its label?

21. Possible API Shape Later

Do not add this yet.

But if eventually exposed as metadata, the API could include:

{
  "confidence_calibration": {
    "version": "core_area_durability_context_v0",
    "metadata_only": true,
    "production_use_allowed": false,
    "current_confidence": "High",
    "simulated_confidence": "Medium",
    "calibration_action": "soften_high_to_medium",
    "calibration_reason": "high_signal_but_insufficient_core_area_durability",
    "core_gap": 0.39,
    "core_area_durability_band": "borderline"
  }
}

Important:

metadata_only = true
production_use_allowed = false

until explicitly graduated.

22. Future Frontend Use

Do not display this prominently yet.

Possible future safe uses:

Admin/debug display
QA-only badge
confidence tooltip
developer-only diagnostics

Possible future wording:

The lean is clear, but Core Area durability is not strong enough for a High Confidence label.

Avoid wording like:

This team is weaker.
The model changed its pick.
The edge is no longer confirmed.

Those would be wrong.

23. Recommended Product Rules
Rule 1: Keep profile type separate from confidence calibration

Bad:

High softened to Medium, so confirmed_edge becomes conflicting_profile.

Good:

confirmed_edge stays confirmed_edge.
confidence softens to Medium.
Rule 2: Do not change winner picks

This feature does not decide teams.

It calibrates label loudness.

Rule 3: Do not use this for Medium → Low yet

The current tested use case is only:

High → Medium

Medium-to-Low should be a separate future audit question.

Rule 4: Treat 0.45 as a research threshold

Do not treat it as permanent.

Rule 5: Human-review before production

The statistics are promising, but product sense still matters.

24. Graduation Criteria

Before this becomes production behavior, require:

1. Simulated High improves correct % across tested seasons.
2. Simulated High improves claim validation.
3. Simulated High reduces severe misses.
4. Simulated Medium does not collapse.
5. Low stays unchanged.
6. Human review confirms softened games make product sense.
7. Human review confirms retained High misses are explainable enough.
8. BigQuery-source mode passes.
9. Admin/debug review is available before frontend user-facing rollout.

Current status:

Criterion	Status
High improves across seasons	Passed
Claim validation improves	Passed in pooled result
Severe misses reduce	Passed in pooled result
Medium remains usable	Passed
Low unchanged	Passed
Human review softened games	Needed
Human review retained High misses	Needed
BigQuery-source mode	Needed
Admin/debug display	Future
25. Suggested Commit Message
Add core durability confidence simulation audit

Longer version:

Add v0.1.3 confidence calibration audit script for Core Area durability simulation.

This introduces an audit-only confidence simulation that softens current High Confidence games to Medium when core_gap is below the durability review threshold. The simulation preserves winner picks, matchup lean, profile type, runtime behavior, BigQuery state, and frontend output.

New outputs include simulated confidence game assignments, current vs simulated confidence ladder summaries, and calibration action summaries. This is intended for review-only confidence calibration testing and does not introduce production confidence rules.
26. Final Current-State Summary

core_area_durability_context_v0 is a promising audit-supported confidence calibration feature.

Across 2023–2025:

Current High:   110 games, 65.14% correct
Simulated High: 47 games, 76.60% correct

High claim validation improved:

57.82% → 62.52%

High severe misses dropped:

4 → 1

Medium stayed usable:

64.86% → 63.31%

Low stayed unchanged:

53.99% → 53.99%

This suggests the feature is doing the right job:

It makes High Confidence harder to earn without changing the model’s pick.

Recommended stopping point:

Commit as audit-only.
Do not wire to production yet.
Run BigQuery-source verification.
Review softened games.
Review retained High misses.
Then consider Admin/debug visibility.

That is the right next step.