# GameLens 5/27/2026  Plan — Admin Learning → Expected Claim Quality

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
#####
#####

















#####First step/Direction:

--Inside High Confidence games, does expected claim quality split the good High reads from the fragile High reads?
-Build an offline dry-run worker that estimates pregame Expected Claim Quality from historical claim signatures, then measures whether that score creates lift against the existing 2025 Admin baselines for claim validation, confidence, profile strength, and Core Area alignment.

Other questions For Profile Strength of Clear Lean why are there no high confidence and for Profile Type I feel like the matrix is missing something but I can't put my finger on it. For Game Calibration it looks and appears like a good matrix where standard distribution appears to be present. Core Area Alighnment just has alot of Low confidence games including its large bucket of No clear edge.  It has me wondering about if this area something seems off and I can't put my finger on it.















######



######

tested build expected claim quality but it proved not helpful for helping High Confidence Labels.

GameLens Confidence Calibration Checkpoint — 2026-05-27
Why we did this work

The Admin dashboard raised an uncomfortable but useful question:

If GameLens labels something High Confidence, should that label produce a better correct percentage than Medium Confidence?

The answer should generally be yes, or at minimum High should be close to Medium within the same profile family, especially inside:

Confirmed Edge
Strong Profile

But the 2025 Admin results showed that current High Confidence was not clearly outperforming Medium. That made the goal:

Make High Confidence more deserving of the label, not more common.

What we tested

We first tested an Expected Claim Quality idea.

The goal was to ask:

Before the game, based on historically similar claim signatures, could GameLens estimate whether its explanation was likely to hold up?

That produced useful diagnostics, but it did not cleanly solve High Confidence calibration.

Expected Claim Quality finding

Expected Claim Quality was useful as a diagnostic layer, but not safe as a direct confidence relabeling rule.

It helped show that some High Confidence games had fragile explanations, but the score was not monotonic inside High Confidence:

Bottom third High Expected Quality: weak
Middle third High Expected Quality: strong
Top third High Expected Quality: weak again

That means:

Higher Expected Claim Quality did not reliably mean safer High Confidence.

So we archived Expected Claim Quality as diagnostic-only for now.

Why Expected Claim Quality is not being implemented

We are not wiring Expected Claim Quality into /game, game_service.py, Model Trust, or the frontend.

Reason:

It does not reliably rank High Confidence games from safer to riskier.

Safe use later:

diagnostic context
claim-language review
admin analysis
possible future feature input

Unsafe use now:

promote Medium to High
downgrade High to Medium
change winner confidence
change Matchup Lean

This was a good failure. We avoided shipping a clever-looking but unstable rule.

What the confidence audit tested next

We then shifted to a cleaner question:

Why are current High Confidence games underperforming, and what separates better High games from fragile High games?

The confidence calibration audit looked at game-level fields such as:

signal_gap
core_gap
core_area_split
team_comp_edge_score
profile_type
profile_strength_label
outcome_confidence_label
model_result
final_margin_abs
claim_validation_rate

The audit also checked whether High misses were:

huge signal-gap games
big core-gap games with weak claims
Strong Profile / confirmed_edge games with poor results
late-season or playoff games
loud Team Comparison games
What the confidence audit found
1. Signal gap is not the fix

High misses were not quiet games.

Current High games already had huge signal gaps. In fact, High misses were often just as loud, or louder, than High hits.

So the answer is not:

raise the signal_gap threshold

That would probably make GameLens more selectively loud, but not necessarily more correct.

2. Team Comparison is not the clean fix

Team Comparison was loud in many High misses, but it was also loud in many High hits.

So Team Comparison loudness is useful context, but it does not separate good High from bad High cleanly enough.

The answer is not:

if Team Comparison is loud, trust High

or:

if Team Comparison is loud, distrust High

It is not discriminating enough by itself.

3. High misses were often close but poorly validated

Many High misses were close on the scoreboard, not blowouts.

That matters because a close miss is not the same as a total model failure.

But the more important issue was this:

When High was wrong, the claim validation usually dropped hard.

So some High misses were not just unlucky final-score misses. The football explanation often failed too.

That suggests High Confidence needs a durability check, not just louder pregame signals.

Best current suspect: Core Area durability

The strongest useful clue was core_gap.

Core gap is basically asking:

How much broad Core Area separation exists behind the matchup lean?

The high-retention simulation tested multiple possible Core Area floors:

0.25
0.30
0.35
0.40
0.45
0.50
0.55

This was important because we did not want to cherry-pick one magic threshold.

Simulation shape

The result was promising:

Current High:
22 games
57.14% correct
53.70% claim validation

Retained High with core_gap >= 0.40:
15 games
66.67% correct
~60% claim validation

Retained High with core_gap >= 0.45:
12 games
75.00% correct
~65% claim validation

Retained High with core_gap >= 0.50:
10 games
70.00% correct
~65% claim validation

This suggests the useful range may be around:

core_gap >= 0.40 to 0.50

Not because one number is perfect, but because the pattern improves across a range.

Current best interpretation

GameLens High Confidence currently seems to mean:

The signal is loud
+ the profile is strong
+ the Core Area read confirms the lean

But the better meaning should probably be:

The signal is loud
+ the profile is strong
+ the Core Area read confirms the lean
+ the broad Core Area separation is durable enough

That last part is what current High may be missing.

Why this is not production-ready yet

This is still not ready for /game or game_service.py.

Reasons:

The 2025 sample only has 22 High Confidence games.
The strongest retained-High groups shrink to around 10–15 games.
We have not tested 2023 or 2024 yet.
The pattern is promising, but still could be 2025-specific.
We need to verify that downgrading High does not accidentally damage the confidence ladder elsewhere.

So this should be treated as:

candidate confidence cap hypothesis

not:

production confidence rule
Current recommendation

Do not implement anything yet.

The current best candidate experiment is:

High Confidence Core Area Durability Guardrail

Possible test framing:

If current confidence = High
and profile_strength = Strong Profile
and profile_type = confirmed_edge
but core_gap is below a durability floor,
then cap visible outcome confidence to Medium.

Candidate floor range:

0.40 to 0.50

Initial review point:

0.45

But 0.45 is not final. It is just the middle of the strongest observed range.

What would prove this next

The next validation step is to run the same confidence calibration audit on more seasons.

Ideal next datasets:

2023 season
2024 season
2025 season
multi-season combined run

The key question:

Does the 0.40–0.50 Core Area durability range improve High Confidence across multiple seasons, or only in 2025?

If it holds across seasons, then we can consider a backend implementation.

If it only works in 2025, we leave it as a useful finding but do not ship it.

Current blocker

We do not currently have 2023 and 2024 claim-training data saved in BigQuery.

So the next data-engineering task is:

Backfill or rebuild the GameLens claim-training pipeline for 2023 and 2024 so the same audit can be run cross-season.

This likely means repeating the claim-training flow for those seasons:

collect / load payloads
build claim training examples
attach validation
attach Level 3 features
run confidence calibration audit
compare results
Final checkpoint

The project did not hit a dead end.

It narrowed the search.

We learned:

Expected Claim Quality is diagnostic-only for now.
Signal gap is not the High Confidence fix.
Team Comparison loudness is not the High Confidence fix.
Core Area durability is the best current suspect.
The possible useful range is core_gap >= 0.40–0.50.
Nothing should be implemented until this is tested beyond 2025.

That is real progress. The model did not hand us a final answer, but it did tell us where to look next.

GameLens Confidence Calibration Checkpoint — 2026-05-27
Why we paused

Today started with a concern from the Admin dashboard:

If GameLens labels a matchup High Confidence, that label should generally have a better correct percentage than Medium Confidence, or at least be close within the same profile family.

The goal is not to create more High Confidence labels.

The goal is:

Make High Confidence more deserving of the label.

What we tested first: Expected Claim Quality

We built and tested several versions of an offline build_expected_claim_quality worker.

The idea was:

Can historical claim-signature performance tell us before the game whether a GameLens explanation is likely to hold up?

Finding

Expected Claim Quality was useful as a diagnostic layer, but it did not cleanly solve High Confidence calibration.

The key issue:

Inside High Confidence, higher Expected Claim Quality was not consistently better.

The score was not monotonic inside High Confidence. In some splits, the middle group performed best, while the top group did not.

Decision

Archive Expected Claim Quality for now as:

diagnostic-only

Do not wire it into:

/game API
game_service.py
Model Trust
Matchup Lean
visible confidence
frontend

This was a good stop. We avoided shipping a smart-looking but unstable rule.

What we tested next: Confidence Calibration Audit

We shifted to a better question:

What separates good High Confidence games from fragile High Confidence games?

We created a confidence calibration audit script:

build_confidence_calibration_audit.py

This script is audit-only. It does not write to BigQuery, change API behavior, or alter model output.

It tested whether High Confidence might be too easy to earn when the signal is loud but the broader Core Area support is not durable enough.

What the confidence audit found
Signal gap is not the fix

High misses were not quiet.

In many cases, High misses had very loud signal gaps too. Raising the signal-gap threshold alone probably does not solve the issue.

Team Comparison is not the clean fix

Team Comparison was loud in both High hits and High misses.

That means it may be useful context, but it does not cleanly separate good High from bad High.

Expected Claim Quality is not the direct fix

Expected Claim Quality remains useful for diagnostics, but it should not directly promote or downgrade confidence.

Core Area durability is the best current suspect

The strongest candidate signal was:

core_gap

The idea:

High Confidence should require not only a loud signal, but also enough broad Core Area separation to make the read durable.

Core Gap retention simulation

We tested multiple possible Core Gap floors:

0.25
0.30
0.35
0.40
0.45
0.50
0.55

This was intentionally done to avoid cherry-picking one magic threshold.

Full 2025 run finding

On the full 2025 run, retained High improved around:

core_gap >= 0.40 to 0.50

The strongest-looking area was around:

0.40–0.45

But this was still too small to implement directly.

Cross-season sample check

We then tested the same audit on the existing multi-season sampled run:

larger_240_level3_qa_20260517

Available High sample sizes:

2023: 10 High games
2024: 20 High games
2025: 8 High games
Combined: 38 High games
Cross-season interpretation

The sampled data did not prove a production rule.

But it did show that Core Area durability remains the best current suspect.

The rough signal:

0.40–0.45 may improve retained High
0.50+ may become too strict

So the finding is not:

Hardcode 0.45

The finding is:

High Confidence may need a Core Area durability guardrail.
Current recommended next feature: core_area_durability_band

We are not scrapping the confidence calibration audit.

Instead, the next version should add a readable diagnostic field:

core_area_durability_band

Possible bands:

weak:            core_gap < 0.35
borderline:      0.35 <= core_gap < 0.40
durable:         0.40 <= core_gap < 0.45
strong_durable:  0.45 <= core_gap < 0.50
very_strong:     core_gap >= 0.50

This band is not a production rule.

It is a diagnostic label to make analysis easier:

High + weak durability
High + borderline durability
High + durable
High + strong_durable
High + very_strong
What we should not do yet

Do not implement:

if core_gap < 0.45:
    downgrade High to Medium

That is too simple and too risky.

Do not change:

game_service.py
/game API
frontend confidence labels
Model Trust
Matchup Lean

Yet.

What needs to happen before implementation

Before any production confidence cap, we need fuller data.

Current sampled cross-season data is useful but too small.

Next validation goal:

Build/save fuller 2023 and 2024 claim-training rows into:
nfl-stream-406420.Analytics.gamelens_claim_training_examples

Then rerun the confidence calibration audit over:

full 2023
full 2024
full 2025
combined full 2023–2025

Only if the Core Area durability signal holds across fuller seasons should we consider a backend confidence cap.

Current project status
Archived for now
Expected Claim Quality

Status:

diagnostic-only, not confidence logic
Active audit direction
Confidence Calibration Audit

Next version should add:

core_area_durability_band
core_area_durability_sort
Current best hypothesis
High Confidence is too dependent on loud signal shape.
It may need broader Core Area durability to deserve the label.
Current implementation status
No production changes.
No API changes.
No frontend changes.
Audit-only research.
Next work when ready
Add core_area_durability_band to the confidence calibration audit script.
Keep it diagnostic-only.
Build fuller 2023/2024 claim-training data into BigQuery.
Rerun full cross-season confidence audits.
Only then decide whether a High Confidence cap is justified.
Simple mental model

Current High seems to mean:

The matchup signal is loud.

What we want High to mean:

The matchup signal is loud
and the broad Core Area support is durable enough to trust it.

That is the direction. Not a rule yet — a hypothesis with a promising suspect.

######














######
















1. What problem are we trying to solve?

We are trying to make GameLens High Confidence more deserving of the label.

The goal is not to create more High labels. The concern is that High Confidence should generally perform better than Medium, or at least close to it inside the same profile family like Strong Profile / Confirmed Edge.

2. What triggered this investigation?

The Admin dashboard showed that Medium Confidence was sometimes performing better than High Confidence, especially in strong-looking matchup buckets.

That raised the core calibration question:

Is High Confidence too loud, or is it missing a durability check?

3. What did Expected Claim Quality test?

Expected Claim Quality tested whether historical claim-signature performance could estimate, before the game, whether GameLens’ explanation would hold up.

It used claim-level signatures like:

claim_type
claim_layer
registry_core_area
registry_category
metric
two_way_context
offensive_efficiency_support_bucket
claim_strength_bucket

It used leave-one-game-out scoring to avoid overfitting.

4. What did we learn from Expected Claim Quality?

Expected Claim Quality was useful diagnostically, but not reliable enough to directly fix High Confidence.

Inside High Confidence, higher Expected Claim Quality did not consistently mean safer outcomes. The middle-ranked group sometimes performed better than the top-ranked group.

Decision:

Archive Expected Claim Quality as diagnostic-only.
Do not wire it into /game, confidence, Model Trust, or frontend.
5. Why did we switch to the Confidence Calibration Audit?

Because Expected Claim Quality was answering a claim-quality question, but the real product question was:

Why are current High Confidence games underperforming, and what separates durable High from fragile High?

So we shifted to a game-level audit focused on fields like:

signal_gap
core_gap
core_area_split
team_comp_edge_score
profile_type
profile_strength_label
model_result
final_margin_abs
claim_validation_rate
6. What did the Confidence Calibration Audit find?

It found that signal_gap is not the fix.

High misses were not quiet. They often had loud signal gaps too. So raising signal_gap thresholds probably would not solve the problem.

It also found that Team Comparison loudness is not the clean fix because it appears in both High hits and High misses.

7. What is the best current suspect?

The best current suspect is:

core_gap

The working theory is:

High Confidence should require not only loud matchup signals, but also enough broad Core Area separation to make the read durable.

This became the “Core Area durability” hypothesis.

8. What did the Core Gap retention simulation show?

The simulation tested multiple floors:

0.25
0.30
0.35
0.40
0.45
0.50
0.55

The useful range seemed to be around:

core_gap >= 0.40 to 0.45

But this is not a production rule yet.

The sampled cross-season data suggested the pattern is promising, but small samples mean we should treat it as a hypothesis, not a fix.

9. What is core_area_durability_band, and should it change the model?

core_area_durability_band is a proposed diagnostic label, not a production rule.

Possible bands:

weak:            core_gap < 0.35
borderline:      0.35 <= core_gap < 0.40
durable:         0.40 <= core_gap < 0.45
strong_durable:  0.45 <= core_gap < 0.50
very_strong:     core_gap >= 0.50

It should be added to the audit script first so future analysis can group High games more clearly.

It should not yet downgrade High to Medium.

10. What is the next recommended step?

Next step:

Add core_area_durability_band to the Confidence Calibration Audit only.
Keep it diagnostic-only.

Then, before any production change, build fuller 2023 and 2024 claim-training data into:

nfl-stream-406420.Analytics.gamelens_claim_training_examples

Then rerun the confidence audit on:

full 2023
full 2024
full 2025
combined full 2023–2025

Only if the Core Area durability signal holds across fuller seasons should we consider a backend confidence cap.

One-sentence handoff

GameLens High Confidence appears too dependent on loud signal shape; the best current hypothesis is that High also needs Core Area durability, but this should stay audit-only until fuller cross-season validation supports it.










######









######