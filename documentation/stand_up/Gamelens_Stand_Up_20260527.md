# GameLens Tomorrow Plan — Admin Learning → Expected Claim Quality

## Main Goal

Use the Admin dashboard results to improve GameLens calibration without overfitting to one 2025 run.

The goal is not to hard-code one-off gates like:

- if pressure > X, cap confidence
- if clean mix = Y, promote High
- if caution bucket appears, always downgrade

The goal is to learn where GameLens is calibrated, where it overstates, and how to measure explanation trust before letting the model speak louder.

## Current Learning Summary

The Admin dashboard taught us that High Confidence is not simply better than Medium Confidence.

Medium + Strong Profile + Confirmed Edge performed very well, while High + Strong Profile + Confirmed Edge was more fragile.

Important finding:

High Confidence often had strong-looking pregame signals:

- strong signal gap
- strong core gap
- strong team comparison edge
- supportive two-way context
- repeat-positive offensive efficiency support

But some High games still produced very poor postgame claim validation.

This means High does not just need “more strength.”

High needs durability evidence.

## Key Product Framing

GameLens should separate:

### Game Pick Accuracy

Did the directional lean match the final winner?

### Claim Validation

Did the football explanation hold up?

A game can be:

- correct outcome + strong claims
- correct outcome + weak claims
- wrong outcome + strong claims
- wrong outcome + weak claims
- severe miss + weak claims

This is more useful than simple win/loss grading.

## Main Hypothesis

High Confidence should eventually mean:

```text
Strong matchup shape
+ strong expected explanation quality
+ no major unresolved fragility warning

Medium Confidence can remain the default “usable strong read.”

High should become rarer and better justified.

What Not To Do Tomorrow

Do not immediately edit runtime confidence logic in game_service.py.

Do not add hard-coded gates based only on this one 2025 run.

Do not wire new logic into production confidence yet.

Do not spend Lovable/frontend credits unless a tiny label/visibility issue blocks understanding.

Best Next Backend Direction

Build an offline expected-claim-quality calibration experiment.

Possible new file:

agg/gamelens_training/build_expected_claim_quality.py

Alternative:

agg/gamelens_training/build_claim_language_calibration.py

But a new file may be cleaner because this is a new concept.

What Expected Claim Quality Means

Expected claim quality asks:

Before the game, based on historical claim signatures, how likely is this game’s explanation to hold up?

This should be pregame-safe.

It should not use the same game’s postgame validation to score itself.

Candidate Inputs

Use claim-level fields such as:

claim_type
claim_layer
registry_core_area
registry_category
metric
two_way_context
offensive_efficiency_support_bucket
offensive_efficiency_support_strength
claim_strength_bucket
claim_strength_language_signal
clean_hierarchy_status
First Experiment Scope

Create a dry-run output first.

Do not write to BigQuery first.

Dry-run should create a CSV/JSON output showing:

game_id
outcome_confidence_label
profile_strength_label
profile_type
model_result
final_margin_abs
actual claim_validation_rate
expected_claim_quality_score
expected_claim_quality_bucket
headline_expected_claim_quality_score
supporting_expected_claim_quality_score
claim_rows_scored
low_sample_signature_rows
missing_signature_rows
Important Validation Question

Does expected claim quality separate:

High correct vs High miss
Medium Strong correct vs High fragile miss
wrong but reasonable vs wrong and unsupported
severe misses from close/variance misses
First Scoring Approach

Start simple.

For each claim row, create a signature like:

claim_type + claim_layer + registry_core_area + registry_category + metric + two_way_context + offensive_efficiency_support_bucket

Then calculate historical validation rate for that signature.

To avoid overfitting, use leave-one-game-out logic:

When scoring Game A, exclude Game A from the historical rate calculation.

If the exact signature has too few samples, fall back to broader signatures.

Example fallback order:

full signature
claim_type + claim_layer + metric
claim_type + claim_layer + registry_category
claim_type + claim_layer + registry_core_area
claim_type + claim_layer
global baseline
Output Buckets

Possible buckets:

strong_expected_quality
supportive_expected_quality
mixed_expected_quality
fragile_expected_quality
insufficient_sample

Do not use these buckets to change runtime behavior yet.

Use them for analysis only.

Admin API Future Shape

Eventually add an Admin section like:

expected_claim_quality_matrix

This section should answer:

Did expected claim quality actually predict better claim validation?

Possible output fields:

bucket
games
claim_rows
avg_expected_claim_quality
actual_validation_rate
lift_vs_baseline
sample_warning
Runtime API Future Shape

Eventually add metadata to /game/<game_id>:

"claim_quality_context": {
  "available": true,
  "version": "expected_claim_quality_v0",
  "metadata_only": true,
  "game_expected_claim_quality_score": 0.62,
  "game_expected_claim_quality_bucket": "supportive_expected_quality",
  "headline_expected_claim_quality_score": 0.67,
  "supporting_expected_claim_quality_score": 0.58,
  "confidence_use_allowed": false,
  "diagnostics": {
    "claim_rows_scored": 31,
    "missing_signature_rows": 1,
    "low_sample_claim_rows": 3
  }
}

Important:

This should be metadata only at first.

It should not change:

matchup lean
confidence
winner prediction
model trust
frontend copy
Possible Files Later

Offline calibration:

agg/gamelens_training/build_expected_claim_quality.py

Admin dashboard API:

queries/admin_claim_health_queries.py
services/admin_claim_health_service.py

Runtime /game API later:

services/game_service.py
services/claim_language_response.py

But game_service.py should be downstream, not the starting point.

Tomorrow Work Order
Step 1 — Re-center the goal

Restate the goal:

Build an offline expected claim quality experiment to learn whether explanation trust can improve confidence calibration.

Step 2 — Inspect available fields

Confirm the needed fields exist in:

Analytics.gamelens_claim_training_examples

Fields of interest:

game_id
model_result
final_margin_abs
outcome_confidence_label
profile_strength_label
profile_type
claim_type
claim_layer
registry_core_area
registry_category
metric
validation_result
two_way_context
offensive_efficiency_support_bucket
claim_strength_bucket
clean_hierarchy_status
Step 3 — Build dry-run worker

Create a dry-run script that:

loads rows for one run_id
builds claim signatures
calculates leave-one-game-out historical validation rates
falls back to broader signatures when sample size is low
rolls claim scores up to game level
writes CSV/JSON outputs
Step 4 — Analyze results

Compare expected claim quality against:

actual claim validation
model_result
final margin
High correct
High miss
Medium Strong correct
severe miss
close miss
Step 5 — Decide if it earns next phase

If expected claim quality separates good explanations from fragile explanations, then consider:

adding a BigQuery output table
adding Admin API matrix
later adding /game metadata

If it does not separate, keep it as a learning experiment and do not wire it in.

Success Criteria

This work is successful if it tells us whether GameLens can estimate explanation trust before the game.

Good signs:

high expected quality aligns with higher actual claim validation
fragile expected quality aligns with weaker claim validation
bad High misses show lower expected quality than good High wins
Medium Strong correct games score well
severe misses are flagged as fragile more often than clean variance misses
Guardrail

Only promote this feature if it passes three tests:

The Admin data supports it.
The football/product logic makes sense.
It survives validation outside the exact slice where we discovered it.

Until then, it remains offline calibration metadata.


## My plain-English tomorrow mission

Tomorrow is **not** “fix High Confidence.”

Tomorrow is:

> **Can we measure explanation trust before the game?**

That’s the bridge between what the Admin tab taught us and a smarter model later.



####Notes on how the Admin page can help our models improve:

## First Implementation Step From Admin Learning

The first implementation step is not to change GameLens confidence logic directly.

Instead, build an offline expected-claim-quality calibration layer that helps answer:

> Before the game, did this matchup explanation look trustworthy based on historically similar claim signatures?

This is the first step toward using the Admin dashboard to improve the model without overfitting.

The purpose is to learn whether explanation quality can be estimated pregame, not to immediately cap or promote High Confidence.
Why this is the right first step

The Admin tab taught us:

High Confidence is sometimes too loud
Medium Confidence is currently healthier in key buckets
Strong signal/core separation is not enough
Claim validation and game pick accuracy diverge
Some wrong picks are reasonable; some are bad reasoning misses
Clean/caution mix is useful diagnostically but too blunt for production rules

So the next step should be:

Build a calibration measurement, then test if it earns trust.

The implementation path
Step 1 — Offline experiment

Create something like:

agg/gamelens_training/build_expected_claim_quality.py

This produces CSV/JSON first.

Step 2 — Validate

Check whether expected claim quality separates:

High correct vs High miss
bad reasoning miss vs clean variance miss
Medium Strong correct vs High fragile miss
Step 3 — Add Admin visibility

If useful, add an Admin section:

expected_claim_quality_matrix
Step 4 — Add /game metadata only

Later expose:

"claim_quality_context": {
  "metadata_only": true,
  "game_expected_claim_quality_score": 0.62,
  "game_expected_claim_quality_bucket": "supportive_expected_quality",
  "confidence_use_allowed": false
}
Step 5 — Only later consider confidence changes

Only after validation should this influence game_service.py.

Clean phrasing for your notes
This is the first implementation step in turning the Admin dashboard from a reporting page into a model-improvement loop.

The goal is to use Admin findings to build safer calibration features, not to manually gate confidence with one-season rules.

Expected Claim Quality should begin offline, remain metadata-only at first, and only influence runtime confidence if it proves useful across validation.

That’s the direction I trust. We’re not “fixing High” tomorrow. We’re building the measuring stick that tells us how High should be fixed later.