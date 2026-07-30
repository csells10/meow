Starting mission:

Clean working order
Find/open build_confidence_calibration_audit.py
Add core_area_durability_band and core_area_durability_sort
Run it on current 2025 data to confirm the new fields work
Identify what already exists for 2023/2024 pipeline prerequisites
Build or rerun 2023 claim-training rows
Build or rerun 2024 claim-training rows
Attach validation
Attach Level 3 features
Rerun confidence audit by season
Compare whether core_gap >= 0.40–0.50 still improves High Confidence


2025 audit suggests core_area_durability_band is a useful diagnostic lens.
A simulated High-retention floor around core_gap >= 0.45 improved retained High accuracy from 57.14% to 75.00%, while downgraded High games performed poorly at 33.33%.
This supports further cross-season validation, but sample size remains too low for production confidence changes.

important note:
High Confidence should not mean guaranteed win.
High Confidence should mean the matchup foundation was durable enough that even misses are explainable/close more often than catastrophic.

Current Conclusion:
2025 confidence audit now shows a meaningful Core Area durability ladder. 
Across all games, stronger core_area_durability_band generally aligned with better game accuracy and better claim validation. 
The strongest signal was very_strong durability: 39 games, 81.58% correct, 62.66% claim validation. 
High Confidence below strong_durable remained fragile, while Medium + very_strong performed extremely well. 
This supports cross-season validation before any runtime confidence cap or promotion rule.

Cleanest finding:
Medium + very_strong durability performed extremely well in 2025, going 23–4 overall. 
Most stayed Medium because signal_gap was below the current High-style loudness threshold, usually 4–7.
This suggests the current confidence system may overweight signal_gap and underweight broad Core Area durability.
However, two severe misses inside this bucket show that very_strong durability alone is not safe enough for automatic High promotion.

Current research hypothesis:

2025 audit suggests the strongest Medium promotion candidate is not all Medium + very_strong games. 
The cleaner candidate is Medium + Strong Profile + confirmed_edge + very_strong Core Area durability.
That bucket went 9–0 with 81.9% average claim validation in 2025.
This is promising but too small for production and must be validated across fuller 2023/2024 claim-training runs.

Current best learning

I’d write this down:

The cleanest 2025 promotion-candidate bucket is:
Medium + Strong Profile + confirmed_edge + very_strong Core Area durability + signal_gap = 7.

This bucket went 9–0, with strong claim validation and mostly comfortable margins.
This suggests the current High Confidence logic may be too dependent on crossing a louder signal_gap threshold, while underweighting very strong Core Area durability.

Findings:
2025 audit suggests current High Confidence is over-rewarding loud signal_gap and Team Comparison strength when Core Area durability is weaker.

The mirror bucket confirmed this:
High + Strong Profile + confirmed_edge + weak/borderline/durable durability went 3–6–1, despite signal_gap values from 8–11.

Meanwhile, Medium + Strong Profile + confirmed_edge + very_strong durability went 9–0, with signal_gap fixed at 7.

This suggests the current confidence ladder may be too dependent on signal loudness and not dependent enough on broad Core Area durability.










################



2025 Core Area Durability Roll-Up
1. The 2025 audit ran cleanly

Full 2025 run completed with:

8,067 claim rows loaded
269 games scored
22 High Confidence games
93 Medium Confidence games
154 Low Confidence games
0 BigQuery writes
0 runtime behavior changes

So this was exactly what we wanted: a dry-run-only audit with no production impact.

2. core_area_durability_band is working

The new fields landed correctly:

core_area_durability_band
core_area_durability_sort

Across all 269 games, the distribution was:

Band	Games
weak	189
borderline	18
durable	16
strong_durable	7
very_strong	39

That gives us enough spread to treat this as a useful audit lens.

3. Durability produced a real 2025 ladder

Across all 2025 games, stronger durability generally aligned with better outcomes and better claim validation:

Durability band	Games	Correct %	Claim validation %
weak	189	50.94%	46.49%
borderline	18	53.85%	42.70%
durable	16	63.64%	48.01%
strong_durable	7	75.00%	58.88%
very_strong	39	81.58%	62.66%

That is the biggest 2025 finding.

Plain English:

The stronger the broad Core Area foundation, the more trustworthy the matchup read tended to be.

This is stronger than a random one-off split because it improved both game correctness and claim validation.

4. Current High Confidence looks too signal-gap dependent

Current High Confidence overall:

22 games
57.14% correct

When testing High-retention by core_gap, the strongest 2025 split appeared around:

core_gap >= 0.45

At that floor:

Group	Games	Correct %
Current High	22	57.14%
Retained High, core_gap >= 0.45	12	75.00%
Downgraded High, core_gap < 0.45	10	33.33%

That is very promising, but still low sample and audit-only.

5. The fragile High bucket was very revealing

The mirror bucket was loud:

High + Strong Profile + confirmed_edge + weak/borderline/durable durability

That group went:

10 games
3 correct
6 incorrect
1 no-pick/tie

The key detail: those games still had loud signal profiles, often signal_gap from 8–11 and strong Team Comparison scores. So they looked loud, but the broader Core Area foundation was not durable enough.

That supports this hypothesis:

Current High Confidence may over-reward loud signal shape when Core Area durability is weaker.

6. The Medium promotion candidate bucket was even cleaner

The best-looking “maybe this should have been High” bucket was:

Medium
+ Strong Profile
+ confirmed_edge
+ very_strong Core Area durability

That group went:

9 games
9 correct
0 incorrect
81.9% average claim validation

Even more interesting: every one had:

signal_gap = 7

That suggests they were probably just below the current High threshold, despite having a very strong broad matchup foundation.

Plain English:

Some Medium games looked more deserving of High than some current High games.

That is the most important product insight from today.

7. But we are not ready for a production rule

We should not write this yet:

if core_gap < 0.45:
    downgrade High to Medium

And we should not write this yet either:

if Medium + Strong Profile + confirmed_edge + very_strong:
    promote to High

Why?

Because 2025 is only one season slice, and very strong durability still had misses. Some misses were close, but a few had weak claim validation. The feature is promising, not proven.

Is this a Level 4 feature?
My answer: candidate Level 4 calibration feature, not official Level 4 yet.

Existing Level 4 in your project has mostly meant claim-language calibration: should GameLens speak more strongly, more softly, or block stronger wording?

This new idea is slightly different.

It is about:

confidence calibration

Not just claim language.

So I would label it for now as:

core_area_durability_context_v0
audit-only
confidence-calibration candidate
metadata-only

Eventually, if it survives cross-season validation, it could become something like:

Level 4 Confidence Calibration Feature

or:

Level 4B Outcome Confidence Calibration

But I would avoid calling it official Level 4 today because that makes it sound more mature than it is.

Next plan
Step 1

Build fuller 2023 and 2024 claim-training data into BigQuery.

Step 2

Run the same confidence audit on:

2023
2024
2025
combined 2023–2025
Step 3

Check whether these 2025 findings survive:

core_gap >= 0.45 improves retained High
Medium + Strong Profile + confirmed_edge + very_strong stays strong
High + weak/borderline/durable stays fragile
claim validation improves with durability
severe misses reduce in retained High
Step 4

Only then decide whether this becomes:

metadata-only API context
Admin dashboard matrix
runtime confidence guardrail

My honest read: this is the first feature in a while that looks like it may genuinely improve confidence calibration without turning the model into a homemade decision tree. It is still a measuring stick, not a steering wheel — but it is a good measuring stick. 🧭

Started collecting 2023 data...
Collected 2023 data.

2023 did not reproduce the clean 2025 Core Area durability ladder. 
Durability still may help High Confidence retention, especially around core_gap >= 0.45, but the all-game durability bands were not monotonic.
The 2025 Medium + very_strong promotion signal weakened sharply in 2023, where Medium + very_strong went only 37.5%.
Conclusion: core_area_durability_band remains useful audit metadata, but not a standalone Level 4 confidence feature yet.

What 2023 says about High retention

Current High:

28 games
64.29% correct

The 0.45 floor:

Retained High: 11 games, 72.73% correct
Downgraded High: 17 games, 58.82% correct

So core_gap >= 0.45 still improves the retained High bucket, but it does not isolate bad High games nearly as cleanly as 2025 did.

Key difference vs 2025

In 2025, core_gap < 0.45 looked pretty fragile:

Downgraded High: 33.33% correct

In 2023, core_gap < 0.45 is not awful:

Downgraded High: 58.82% correct

That means a hard cap like:

if core_gap < 0.45:
    downgrade High

would probably throw away some useful 2023 Highs.

Best current read

The durability idea is still useful, but not as a standalone rule.

Better interpretation:

Core Area durability helps explain High Confidence quality,
but core_gap alone is not stable enough to become a production confidence gate.

The feature may still become part of a Level 4 confidence calibration layer, but probably as an interaction feature, not a solo cutoff.

Something like:

core_area_durability_band
+ profile_strength_label
+ profile_type
+ signal_gap
+ claim_validation / claim-quality features
+ severe-miss risk
Current standing after 2023
Hypothesis	Status
Durability is useful audit metadata	✅ Yes
core_gap >= 0.45 improves retained High	⚠️ Often, but not cleanly
core_gap < 0.45 reliably catches bad Highs	❌ Not in 2023
Medium + very_strong should promote to High	❌ Weakened by 2023
Worth testing on 2024	✅ Absolutely

Started collecting 2024 data...
Finished collecting 2024 data.

core_gap >= 0.45 is the strongest cross-season High-retention review threshold so far, but it should remain audit-only until tested as an interaction feature.

Across 2023–2025, current High Confidence went 65.14% across 110 games.
Retaining only High games with core_gap >= 0.45 improved accuracy to 76.60% across 47 games, while the below-threshold group fell to 56.45%.
This supports core_area_durability_band as a confidence calibration candidate, especially for High-retention review.
However, because below-threshold High games still win more than half the time, core_gap should not become a standalone hard downgrade rule.

Across 2023–2025, High Confidence games with core_gap >= 0.45 performed materially better than High games below 0.45. 
Retained High improved from 55.56% to 76.60% correct, claim validation improved from 54.19% to 62.34%, and severe miss rate dropped from 4.76% to 2.13%.
This supports core_area_durability_context_v0 as a candidate confidence-calibration feature.
However, because below-threshold High games still won 55.56%, this should not become a standalone hard downgrade rule.

    Candidate Level 4 confidence-calibration feature
    Status: audit-proven directionally across 3 seasons
    Production status: not ready
    Best use: interaction feature / confidence guardrail candidate

core_gap >= 0.45 group had lower avg signal_gap but much better accuracy.

core_area_durability_context_v0
Status: cross-season audit-supported
Use: candidate Level 4 confidence-calibration interaction feature
Production: not yet
Rule type: not standalone

Across 2023–2025, all current High Confidence games were already Strong Profile + confirmed_edge, but core_gap >= 0.45 still separated a much stronger retained High group: 76.60% correct vs 55.56% below threshold, despite the below-threshold group having slightly higher average signal_gap. This suggests Core Area durability adds real calibration signal beyond profile shape and signal loudness.

core_area_durability_context_v0
Status: audit-supported across 2023, 2024, and 2025
Use case: High Confidence softening simulation
Production status: not yet

core_area_durability_context_v0
Audit-supported across 2023–2025
Useful for High Confidence softening simulation
Not production yet
No frontend change yet
No /game API change yet






#########Master write up:

GameLens Confidence Calibration Notes
Core Area Durability Context v0 — Testing Write-Up
1. Why we started this

The original problem was not that GameLens was choosing the wrong team too often overall.

The specific concern was sharper:

High Confidence was not behaving like High Confidence.

In the Admin view, the label ordering looked suspicious. Medium-confidence Strong Profile games sometimes performed as well as or better than High-confidence Strong Profile games. That means the model might still have a valid matchup lean, but the confidence label was too loud.

So the question became:

Can we make the High Confidence label more honest without changing the winner pick?

That distinction matters a lot.

This work is not about making one football metric stronger or weaker.
It is about confidence calibration.

2. Key concept
The model has multiple layers
Layer	Meaning
profile_type	Is the matchup direction clear, split, conflicted, or no clear edge?
profile_strength_label	How strong/clean is the matchup profile shape?
outcome_confidence_label	How loudly should the app speak about the outcome?
core_gap	How durable/broad the Core Area separation is
new audit concept	Should a High label be retained or softened?

The important product idea:

A game can still be confirmed_edge, but not deserve High Confidence.

That is the clean middle ground.

We are not saying:

Confirmed Edge → Conflicting Profile

We are saying:

Confirmed Edge / High Confidence
may become
Confirmed Edge / Medium Confidence
if Core Area durability is not strong enough.

Same lean.
Same pick.
Softer confidence. 🎚️

3. New audit feature being tested

Working name:

core_area_durability_context_v0

Current status:

audit-only
confidence calibration candidate
not production
not frontend
not /game API yet

The first simulation is intentionally simple:

If current confidence = High
and core_gap < 0.45
then simulated confidence = Medium

Otherwise keep current confidence

In pseudocode:

def simulate_confidence(row):
    if row["confidence_group"] == "High" and row["core_gap"] < 0.45:
        return "Medium"
    return row["confidence_group"]

This simulation does not change the predicted team.

It only asks:

If weaker-durability High games were softened to Medium, would the confidence ladder behave better?

4. Why core_gap was tested

Earlier, we found that High Confidence games were often getting the High label from a loud signal shape:

Strong Profile
confirmed_edge
high signal_gap

But some of those games had weaker broad Core Area support.

So we tested whether High Confidence performs better when the loud signal is also backed by a durable Core Area foundation.

The guiding idea:

High Confidence should require both:
1. a clean directional profile
2. durable Core Area separation

Not just a loud signal gap.

5. Data rebuilt before testing

We went slow and rebuilt the yearly data properly.

2023 pipeline

Completed:

Payload collection ✅
Stage 1 claim extraction ✅
Stage 1 BigQuery write ✅
Stage 2 validation ✅
Stage 2 BigQuery write ✅
Stage 3 features ✅
Stage 3 BigQuery write ✅
Confidence audit ✅

Key counts:

2023 payloads collected: 285
2023 Stage 1 rows: 8,078
2023 confidence audit games scored: 269
2023 High Confidence games: 28
2024 pipeline

Completed:

Payload collection ✅
Stage 1 claim extraction ✅
Stage 1 BigQuery write ✅
Stage 2 validation ✅
Stage 2 BigQuery write ✅
Stage 3 features ✅
Stage 3 BigQuery write ✅
Confidence audit ✅

Key counts:

2024 payloads collected: 285
2024 Stage 1 rows: 8,129
2024 confidence audit games scored: 269
2024 High Confidence games: 60
2025 baseline

Already available and rerun through the updated audit:

2025 confidence audit games scored: 269
2025 High Confidence games: 22

So the comparison base is:

Season	Games scored	High Confidence games
2023	269	28
2024	269	60
2025	269	22

This gave us a real cross-season test set instead of a one-year hunch.

6. Commands used as breadcrumbs
Payload collection

Example for full season collection:

python qa_collect_gamelens_payloads.py \
  --seasons 2024 \
  --games-per-season 400 \
  --sample-mode even \
  --run-name full_2024_reg_post_claim_matrix_pilot \
  --timeout-seconds 180

Checks:

cat qa/gamelens_payload_runs/full_2024_reg_post_claim_matrix_pilot/qa_review.md

cat qa/gamelens_payload_runs/full_2024_reg_post_claim_matrix_pilot/errors.json

find qa/gamelens_payload_runs/full_2024_reg_post_claim_matrix_pilot/payloads -name "*.json" | wc -l

Expected:

Games selected: 285
Payloads collected: 285
Failed games: 0
errors.json = []
payload count = 285
Stage 1 claim training extraction
python -m agg.gamelens_training.build_claim_training_examples \
  --payload-run qa/gamelens_payload_runs/full_2024_reg_post_claim_matrix_pilot \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --dry-run

Write to BigQuery:

python -m agg.gamelens_training.build_claim_training_examples \
  --payload-run qa/gamelens_payload_runs/full_2024_reg_post_claim_matrix_pilot \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --write-bigquery \
  --replace-run

Verify:

python - <<'PY'
from google.cloud import bigquery

client = bigquery.Client(project="nfl-stream-406420")

query = """
SELECT
  run_id,
  feature_status,
  COUNT(*) AS row_count
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'full_2024_reg_post_claim_matrix_pilot'
GROUP BY run_id, feature_status
ORDER BY feature_status
"""

for row in client.query(query).result():
    print(dict(row))
PY

Expected after Stage 1:

feature_status: partial
row_count: 8129
Stage 2 validation

Dry-run:

python -m agg.gamelens_training.update_claim_training_validation \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --dry-run

Write:

python -m agg.gamelens_training.update_claim_training_validation \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --write-bigquery

Verify:

python - <<'PY'
from google.cloud import bigquery

client = bigquery.Client(project="nfl-stream-406420")

query = """
SELECT
  run_id,
  feature_status,
  COUNT(*) AS row_count,
  COUNTIF(validation_result = 'validated') AS validated_rows,
  COUNTIF(validation_result = 'not_validated') AS not_validated_rows,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_rows
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'full_2024_reg_post_claim_matrix_pilot'
GROUP BY run_id, feature_status
ORDER BY feature_status
"""

for row in client.query(query).result():
    print(dict(row))
PY

2024 verified result:

row_count: 8129
validated_rows: 4170
not_validated_rows: 2890
neutral_or_mixed_rows: 1069
Stage 3 feature update

Dry-run:

python -m agg.gamelens_training.update_claim_training_features \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --dry-run

Write:

python -m agg.gamelens_training.update_claim_training_features \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --write-bigquery

Verify:

python - <<'PY'
from google.cloud import bigquery

client = bigquery.Client(project="nfl-stream-406420")

query = """
SELECT
  run_id,
  feature_status,
  COUNT(*) AS row_count,
  COUNTIF(two_way_context IS NOT NULL) AS rows_with_two_way_context,
  COUNTIF(clean_hierarchy_status IS NOT NULL) AS rows_with_clean_hierarchy,
  COUNTIF(offensive_efficiency_support_bucket IS NOT NULL) AS rows_with_off_eff_support
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'full_2024_reg_post_claim_matrix_pilot'
GROUP BY run_id, feature_status
ORDER BY feature_status
"""

for row in client.query(query).result():
    print(dict(row))
PY

Expected:

row_count: 8129
rows_with_two_way_context: 8129
rows_with_clean_hierarchy: 8129
rows_with_off_eff_support: 8129
7. Audit script work

We created a new uniquely named audit script:

build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim.py

Suggested repo location:

agg/gamelens_training/build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim.py

This script adds audit-only simulated confidence outputs.

New outputs:

core_durability_confidence_simulation_games.csv
core_durability_confidence_ladder_summary.csv
core_durability_confidence_action_summary.csv

New simulation fields include:

simulated_confidence_group_v0
calibration_action_v0
calibration_reason_v0
core_area_durability_band
core_area_durability_sort

The current simulation action is intentionally narrow:

High + core_gap < 0.45 → Medium
Everything else stays unchanged
8. Testing v0.1.3 against existing CSVs

Example 2025 command:

python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id test_2025_core_durability_sim \
  --season 2025 \
  --input-game-csv qa/gamelens_confidence_calibration_audit_v0_1_2_runs/full_2025_reg_post_claim_matrix_pilot/game_level_confidence_audit.csv \
  --dry-run

Then inspect:

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

We repeated this for:

test_2023_core_durability_sim
test_2024_core_durability_sim
test_2025_core_durability_sim
9. Season-by-season simulation results
2023
Label	Current correct %	Simulated correct %
High	64.29%	72.73%
Medium	62.77%	62.16%
Low	54.24%	54.24%

Interpretation:

High improved by +8.44 points.
Medium barely changed.
Low stayed unchanged.

This mattered because 2023 was the season that made us nervous. The durability pattern was less clean, but the simulated confidence softening still behaved well.

2024
Label	Current correct %	Simulated correct %
High	68.33%	79.17%
Medium	67.42%	65.60%
Low	56.52%	56.52%

Interpretation:

High improved by +10.84 points.
Medium dropped only -1.82 points.
Low stayed unchanged.

2024 was the biggest High Confidence sample, with 60 High games, so this was the strongest stress test.

2025
Label	Current correct %	Simulated correct %
High	57.14%	75.00%
Medium	64.52%	61.76%
Low	51.72%	51.72%

Interpretation:

High improved by +17.86 points.
Medium dropped -2.76 points, but remained usable.
Low stayed unchanged.

2025 was the original pain point season, so this was an especially encouraging result.

10. Compact High/Medium impact

The rollup showed this season-by-season impact:

Season	Current High	Sim High	Lift	Current Medium	Sim Medium	Medium Change
2023	64.29%	72.73%	+8.44	62.77%	62.16%	-0.61
2024	68.33%	79.17%	+10.84	67.42%	65.60%	-1.82
2025	57.14%	75.00%	+17.86	64.52%	61.76%	-2.76

The important point:

High improved every season, while Medium stayed usable.

That is exactly the behavior we hoped for.

11. Pooled 2023–2025 result

Across all three seasons:

Label	Current games	Current correct %	Sim games	Sim correct %
High	110	65.14%	47	76.60%
Medium	276	64.86%	339	63.31%
Low	421	53.99%	421	53.99%

High-specific details:

Metric	Current High	Simulated High
Games	110	47
Correct %	65.14%	76.60%
Claim validation %	57.82%	62.52%
Severe misses	4	1
Close misses	26	9

This is the cleanest summary of the finding:

The simulation made High smaller, cleaner, and more deserving of the label. Medium absorbed the softened games without collapsing. Low stayed unchanged.

12. What this proves

This testing supports:

core_area_durability_context_v0 is useful audit-supported confidence calibration metadata.

It also supports:

High Confidence should probably require durable Core Area support.

It does not prove:

core_gap < 0.45 should be a permanent production rule.

The 0.45 threshold is a test threshold, not a football law.

This matters because we do not want to build a fake-precision rule like:

0.449 = not High
0.450 = High

That would be fragile and could overfit.

13. What this feature would do if eventually implemented

Current behavior:

Team A edge / High Confidence

Possible future calibrated behavior:

Team A edge / Medium Confidence
Reason: signal profile is loud, but Core Area durability is not strong enough for High.

The pick does not change.

The lean does not change.

Only the confidence label changes.

That is the key product distinction.

14. What this feature should not do

Do not use this to say:

The team is stronger or weaker.

Do not use this to change:

profile_type = confirmed_edge

Do not automatically relabel poor-performing games as:

conflicting_profile

Why?

Because profile_type describes pregame directional agreement.

A bad outcome does not mean the pregame profile was conflicting.

The right future state is more like:

profile_type = confirmed_edge
profile_strength_label = Strong Profile
outcome_confidence_label = Medium
calibration_reason = insufficient_core_area_durability_for_high
15. Why we should not test Medium → Low yet

We discussed whether weak Medium games should be downgraded to Low.

Current answer:

Not yet.

Reason:

The original problem was High not behaving like High.

The clean first fix is:

Make High harder to earn.

Medium is allowed to be messy. Medium is the “usable but measured” bucket.

If we start pushing Medium into Low too early, we risk making:

High = tiny obvious bucket
Medium = empty
Low = giant junk drawer

That is not better calibration. That is just stricter labeling.

So for now:

High → Medium softening: tested and promising
Medium → Low: observe later only
Medium → High: not ready
Low changes: avoid
16. Recommended next steps
Step 1 — Commit the audit-only script

Commit:

agg/gamelens_training/build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim.py

Suggested commit message:

Add audit-only core durability confidence simulation

Longer commit message:

Add v0.1.3 confidence calibration audit script with Core Area durability simulation.

This introduces an audit-only simulated confidence ladder that softens current High Confidence games to Medium when core_gap is below the durability review threshold. The simulation preserves winner picks and runtime behavior while producing current vs simulated ladder summaries, game-level calibration actions, and action summaries.

No /game API logic, frontend behavior, BigQuery writes, or production confidence rules are changed.
Step 2 — Keep it audit-only

Do not wire this into:

game_service.py
model_trust_service.py
frontend
/game API response
Admin UI

yet.

This is still research.

Step 3 — Run v0.1.3 from BigQuery source, not only CSV

We tested the script against existing CSVs, which is good.

Next verification should confirm the script also runs normally from BigQuery rows.

Example:

python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2025_reg_post_claim_matrix_pilot \
  --season 2025 \
  --dry-run

Then repeat for:

python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --season 2024 \
  --dry-run

and:

python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2023_reg_post_claim_matrix_pilot \
  --season 2023 \
  --dry-run

This proves the script is usable as the new audit worker, not just as a CSV post-processor.

Step 4 — Human-review the softened games

Before production, inspect the games that would move:

High → Medium

Review columns:

season
game_id
model_result_normalized
final_margin_abs
core_gap
signal_gap
profile_strength_label
profile_type
actual_claim_validation_rate
audit_failure_tags
calibration_reason_v0

Suggested Python:

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

Why this matters:

We need to make sure the softened games make football/product sense, not just statistical sense.

Step 5 — Review false softens

Some games below 0.45 were still correct.

We should inspect those and ask:

Were they correct but fragile?
Were they correct with weak claims?
Were they correct by comfortable margin?
Did the softening feel too harsh?

This prevents us from making High too restrictive.

Suggested slice:

false_softens = softened[softened["model_result_normalized"] == "correct"]

This is important because the below-threshold High group still won more than half the time. We are not saying those leans were bad. We are only saying they may not deserve High.

Step 6 — Review retained High misses

Also inspect:

simulated High but incorrect

This helps find what the feature does not catch.

Suggested slice:

retained_misses = out[
    (out["simulated_confidence_group_v0"] == "High") &
    (out["model_result_normalized"] == "incorrect")
]

Questions:

Were these close misses?
Were they severe misses?
Was claim validation weak?
Do they suggest a second guardrail?

Important: do not add a second guardrail yet. Just document.

Step 7 — Define graduation criteria before production

Before this touches runtime, we should require something like:

1. Simulated High improves correct % in all tested seasons.
2. Simulated High reduces severe misses.
3. Simulated High improves claim validation.
4. Simulated Medium does not drop more than ~3 percentage points.
5. Low remains unchanged.
6. Human review agrees softened games are not being unfairly punished.
7. Retained High misses are mostly close/variance misses or explainable.

Based on current rollup, criteria 1–5 look promising.

Criteria 6–7 still need human review.

17. Possible future implementation path
Phase A — Admin only

Add Admin section:

High Confidence Durability Review

Rows:

current High retained
current High softened

Columns:

games
correct %
claim validation %
severe misses
close misses
avg core_gap
avg signal_gap

No production behavior change.

Phase B — API metadata only

Later, expose something like:

{
  "confidence_calibration": {
    "version": "core_area_durability_context_v0",
    "production_use_allowed": false,
    "current_confidence": "High",
    "simulated_confidence": "Medium",
    "calibration_action": "soften_high_to_medium",
    "reason": "high_signal_but_insufficient_core_area_durability",
    "core_gap": 0.39,
    "core_area_durability_band": "borderline"
  }
}

Still no label change.

This lets frontend/Admin display the logic without changing predictions.

Phase C — Production softening, only if approved

Only after more review:

If current confidence = High
and confidence_calibration_action = soften_high_to_medium
then outcome_confidence_label = Medium

But even then:

profile_type remains confirmed_edge
profile_strength can remain Strong Profile
matchup lean remains unchanged
winner pick remains unchanged

The only thing that changes is the confidence label.

18. Final conclusion

The feature looks genuinely promising.

The cleanest summary:

Across 2023–2025, the simulated Core Area durability calibration made High Confidence smaller but much stronger: High improved from 65.14% to 76.60%, claim validation improved from 57.82% to 62.52%, severe misses dropped from 4 to 1, and Medium stayed usable. Low was unchanged.

This directly addresses the original problem:

High Confidence was not earning the High label.

But the correct next step is still caution:

Commit as audit-only.
Do not wire to production.
Human-review softened games.
Run from BigQuery source.
Consider Admin visibility before runtime behavior.

This is the right kind of progress: useful, measured, and not kitchen soup.