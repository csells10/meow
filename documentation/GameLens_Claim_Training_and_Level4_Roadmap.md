GameLens Claim Training + Feature Engineering Handoff
Claim Validation, Feature Testing, and Production Adoption Summary
1. What We Built

Today’s work moved GameLens from being only a matchup explanation API toward becoming a claim-learning system.

The big idea:

GameLens makes pregame claims about a matchup. We are now storing those claims, checking whether they held up after the game, and testing which pregame features help those claims become more trustworthy.

This is different from simply asking:

Did GameLens pick the winner?

Instead, we are asking:

Did GameLens correctly identify the pregame matchup advantage?

Example:

Pregame claim:
Team B has the better Defensive Control profile.

Game result:
Team B loses the game.

Postgame data:
Team B still allowed fewer points per play, limited yards better, or otherwise showed the stronger defensive-control profile.

Claim validation:
Validated.

Why:
The claim was about matchup quality, not final winner.

That distinction is important. A team can lose and still validate a GameLens claim.

2. What We Are Actually Predicting

This system is currently trying to predict whether a pregame claim will be true in the postgame data.

It is not yet directly predicting:

winner
spread
total
margin
moneyline

It is predicting/evaluating something more specific:

If GameLens says Team A is better than Team B in a certain matchup area before the game,
does the postgame data support that claim?

So the target is closer to:

claim correctness

or:

pregame matchup claim validation

not final outcome correctness.

Example claim targets

GameLens might say:

BUF has better Offensive Output.
PHI has better Defensive Control.
KC has a Scoring Efficiency edge.
BAL has a Disruption and Turnovers advantage.
DAL has a metric highlight in third_down_pct.

Each of those becomes a row in the training table.

Then after the game, we ask:

Did that team actually outperform the opponent in the related postgame metric or group?

That is the learning loop.

3. Why This Is Useful

Without this table, GameLens can explain a matchup, but we do not know how trustworthy each explanation is.

With this table, we can start answering:

Which claim types validate most often?
Which claim types are noisy?
Which pregame features support better validation?
When should confidence be capped?
When should the API use softer language?
When should the API say “edge” versus “lean” versus “mixed”?

This is the start of evidence-based language and confidence calibration.

4. New Folder Structure

We organized the training pipeline into:

agg/gamelens_training/
  __init__.py
  create_claim_training_examples_table.py
  build_claim_training_examples.py
  update_claim_training_validation.py
  update_claim_training_features.py

This was an important cleanup step because these scripts are now a real subsystem, not throwaway QA files.

5. Current Pipeline Map
Payload / metrics exist
↓
Level 0 table setup
agg/gamelens_training/create_claim_training_examples_table.py
Creates Analytics.gamelens_claim_training_examples
↓
Level 1 claim row builder
agg/gamelens_training/build_claim_training_examples.py
Builds one row per pregame GameLens claim
↓
Level 2 validation updater
agg/gamelens_training/update_claim_training_validation.py
Adds postgame validation labels from Analytics.game_team_metric_facts_{season}
↓
Level 3 feature updater
agg/gamelens_training/update_claim_training_features.py
Adds engineered features:
  offense_finish_score
  defensive_suppression_score
  two_way_edge_score
  two_way_context
↓
Future Level 4 calibration/model job
Learns which claims/features deserve stronger or softer language
↓
/game API reads prepared calibration outputs
services/game_service.py / services/model_trust_service.py
6. BigQuery Table

The main table is:

nfl-stream-406420.Analytics.gamelens_claim_training_examples

This table stores one row per GameLens claim.

A single game can create many rows, usually around 25–35 rows, because GameLens makes multiple claims:

core area claims
category summaries
metric highlights
team comparison metrics
game profile signals
7. Level 1 — Claim Row Builder
Worker
agg/gamelens_training/build_claim_training_examples.py
Purpose

Reads GameLens payloads and writes one row per pregame claim into BigQuery.

Command pattern:

python -m agg.gamelens_training.build_claim_training_examples \
  --payload-run qa/gamelens_payload_runs/<payload_run_name> \
  --run-id <run_id> \
  --write-bigquery \
  --replace-run
Baseline result

Run ID:

baseline_96_stage1_v2

Result:

Rows built: 2708
Game count with claim rows: 90
Unique claim keys: 2708
Errors: 0
Fresh random sample result

Run ID:

fresh_96_features_qa_v2

Result:

Rows built: 2737
Game count with claim rows: 92
Unique claim keys: 2737
Errors: 0

This was good because the fresh run was close in shape to baseline but not identical.

8. Level 2 — Postgame Claim Validation
Worker
agg/gamelens_training/update_claim_training_validation.py
Purpose

Adds validation labels to each claim row.

It checks the pregame claim against postgame actual metrics from:

Analytics.game_team_metric_facts_{season}
Main validation fields
validation_result
validated_flag
actual_team
actual_side
actual_gap
actual_gap_bucket
qa_read_v2
headline_claim_validation_rate
unique_claim_validation_rate
What validation_result means

Current major values:

validated
not_validated
actual_neutral_or_mixed
unavailable
validated

The postgame data supported the pregame claim.

Example:

Pregame:
KC has better Scoring Efficiency.

Postgame:
KC actually had the better scoring-efficiency result.

validation_result = validated
not_validated

The postgame data contradicted the claim.

Example:

Pregame:
MIN has better Defensive Control.

Postgame:
Opponent actually had the better defensive-control result.

validation_result = not_validated
actual_neutral_or_mixed

The postgame result was too close, tied, or mixed.

Example:

Pregame:
Team A has the edge.

Postgame:
The two teams were effectively even.

validation_result = actual_neutral_or_mixed
unavailable

The system could not find the correct actual metric to validate against.

We fixed this to zero in the main run by adding the metric alias:

turnover_margin_per_game → turnover_margin
9. What “Validation Rate” Means

This is important.

When we say:

validation_rate = 0.5275

that means:

52.75% of those claim rows were validated by postgame data.

Formula:

validated_count / total_row_count

Example:

supportive rows = 709
validated = 374

validation_rate = 374 / 709 = 52.75%

So in plain English:

For this group of claims, GameLens was directionally correct about 52.8% of the time.

But this is not winner accuracy.

It is claim accuracy.

Better plain-English wording

Instead of saying:

We guessed the winner correctly 52.8% of the time.

we should say:

Claims in this bucket were supported by postgame data 52.8% of the time.

or:

When GameLens produced this type of pregame claim, the postgame data agreed 52.8% of the time.

This is a cleaner and more honest interpretation.

10. Baseline Claim Validation Totals

For:

baseline_96_stage1_v2

We had:

validated: 1271
not_validated: 1060
actual_neutral_or_mixed: 377
unavailable: 0
total rows: 2708

Approximate overall claim validation rate:

1271 / 2708 = 46.9%

So across all claim rows, before feature filtering:

GameLens claims validated around 47% of the time.

That sounds modest, but remember:

This includes every claim row, not just headline claims.
Some claim families are inherently noisy.
Some rows are supporting context, not direct prediction claims.
We are validating against postgame metrics, which can be volatile game to game.

The value comes from finding which subsets validate better.

11. Fresh Sample Claim Validation Totals

For:

fresh_96_features_qa_v2

From the two-way context grouped totals:

validated: 1375
not_validated: 1020
actual_neutral_or_mixed: 342
total rows: 2737

Approximate overall claim validation rate:

1375 / 2737 = 50.2%

So the fresh sample overall was slightly stronger than baseline.

Again, this is not “winner prediction accuracy.”

It means:

About half of the claim rows were supported by postgame data.

The goal is to identify which conditions push that rate meaningfully higher.

12. Level 3 — Feature Engineering
Worker
agg/gamelens_training/update_claim_training_features.py
Active formula version
offense_finish_v2__defensive_suppression_v3__two_way_context_v1
Current engineered features
offense_finish_score
defensive_suppression_score
two_way_edge_score
two_way_context
13. Feature: offense_finish_score
What it asks
Does this team have pregame support for moving the ball and finishing drives?
Intended football meaning

This is about whether a team can create and finish offensive opportunities.

It is not simply:

Is the offense good?

It is closer to:

Can this team move the ball and convert that movement into points/touchdowns?
Scoped claim families

This is intentionally scoped to relevant offensive/finishing claims:

Scoring Efficiency
Drive Conversion
Red Zone Finish
Rushing Game
third_down_pct
red_zone_efficiency
yards_per_rush
Why scoping mattered

A first broad version applied the score to almost every row.

That made it noisy because it was being attached to claims like:

Turnovers
Pressure
Defensive Control

That was not appropriate.

We learned:

A good feature is not just a good formula. It also needs to apply to the correct claim family.

Counts

Baseline:

Rows with offense_finish_score: 754
Rows missing offense_finish_score: 1954

Fresh:

Rows with offense_finish_score: 748
Rows missing offense_finish_score: 1989
Baseline validation by bucket
Bucket	Row Count	Validation Rate
strong_positive	310	55.8%
positive	189	51.3%
mixed_near_even	118	53.4%
negative	90	42.2%
strong_negative	47	44.7%
Fresh validation by bucket
Bucket	Row Count	Validation Rate
strong_positive	300	53.7%
positive	203	51.2%
mixed_near_even	129	50.4%
negative	67	22.4%
strong_negative	49	46.9%
Interpretation

The useful repeatable signal:

strong_positive / positive offense_finish_score usually validates around 51–56%
negative offense_finish_score is weaker

Plain English:

When offensive finish support is positive, GameLens offensive/finishing claims tend to validate better. When it is negative, those claims deserve caution.

This is not strong enough to say:

Positive offense finish means the team wins.

But it is strong enough to say:

Positive offense finish is useful support for related claims.
14. Feature: defensive_suppression_score
What it asks
Does this team have pregame support for limiting opponent offense/scoring?
Intended football meaning

This is about defensive control and scoring suppression.

It tries to capture whether a team can:

limit points
limit efficiency
control opponent offensive output
Scoped claim families
Defensive Control
points_allowed_per_play
points_allowed_per_yard
yards_allowed
points_allowed
defensive_success_rate
Important lesson from QA

We tested a fallback where points_allowed_per_play could create a score by itself.

That failed.

The metric-only fallback validated at:

25.0%

That is bad.

So the final rule became:

Best:
  Defensive Control + points_allowed_per_play

Okay:
  Defensive Control only

Rejected:
  points_allowed_per_play only
Why this matters

This is a good example of the system working.

At first, it felt wrong to have missing values.

But QA showed those missing values were protecting us from weak isolated metrics.

So instead of filling the data just to fill it, we used validation results to decide:

No Defensive Control = no defensive_suppression_score
Counts

Baseline:

Rows with defensive_suppression_score: 364
Rows missing defensive_suppression_score: 2344

Fresh:

Rows with defensive_suppression_score: 371
Rows missing defensive_suppression_score: 2366
Baseline validation by bucket
Bucket	Row Count	Validation Rate
strong_positive	255	52.6%
positive	105	42.9%
mixed_near_even	4	75.0%

The mixed bucket only had 4 rows, so do not trust that much.

Fresh validation by bucket
Bucket	Row Count	Validation Rate
strong_positive	222	53.2%
positive	127	54.3%
mixed_near_even	22	40.9%
Interpretation

The repeatable signal:

Defensive suppression is useful when Defensive Control exists.

Plain English:

When GameLens has broader Defensive Control support, defensive suppression claims validate better than weak/mixed cases.

Again, this is not winner prediction.

It is claim support.

15. Feature: two_way_edge_score
What it asks
Does this team have both offensive finish support and defensive suppression support?
Formula
two_way_edge_score = min(offense_finish_score, defensive_suppression_score)
Why use min()

A two-way profile is only as strong as its weaker side.

If a team has:

offense_finish_score = 0.70
defensive_suppression_score = 0.10

then the team does not really have a strong two-way profile.

The two-way score should not be 0.70.

It should be limited by the weak side.

Important caution

two_way_edge_score is a numeric companion field.

The more meaningful and repeatable field was:

two_way_context

not the raw score.

16. Feature: two_way_context
What it asks
Does the claimed team have supportive two-way context?
Values
supportive
available_mixed
unavailable
Logic
supportive:
  offense_finish_score >= 0.15
  AND defensive_suppression_score >= 0.15

available_mixed:
  both scores exist
  but they are not both supportive

unavailable:
  one or both scores missing
Why we built this

We first tested a stricter idea:

two_way_strong

where both scores needed to be strong.

That did not repeat well across baseline and fresh samples.

Then we softened the idea into:

supportive two-way context

That repeated better.

Counts

Baseline:

supportive: 709
available_mixed: 1470
unavailable: 529

Fresh:

supportive: 771
available_mixed: 1435
unavailable: 531
Baseline validation
Context	Row Count	Validation Rate
supportive	709	52.8%
available_mixed	1470	44.8%
unavailable	529	45.2%
Fresh validation
Context	Row Count	Validation Rate
supportive	771	56.2%
available_mixed	1435	47.6%
unavailable	531	48.8%
Interpretation

This is the cleanest new repeatable feature.

Plain English:

When both offensive finish support and defensive suppression support are present, GameLens claims validate more often.

In rough terms:

Baseline supportive lift:
52.8% vs 44.8–45.2%

Fresh supportive lift:
56.2% vs 47.6–48.8%

That is about a 7–8 percentage point lift in both samples.

This is meaningful.

Important usage caution

This should be treated as:

side-level support context

not as:

automatic confidence booster

Good future use:

“This claim has supportive two-way context, so stronger explanation language may be allowed.”

Bad future use:

“two_way_context is supportive, so increase pick confidence automatically.”
17. What “How Often Are We Guessing Correctly?” Means Here

If someone asks:

How often are we right?

The honest answer depends on the scope.

Overall claim validation

Baseline:

1271 / 2708 = 46.9%

Fresh:

1375 / 2737 = 50.2%

So across all claim rows:

GameLens claims validated roughly 47–50% of the time.

But that includes:

headline claims
supporting claims
metric highlights
category summaries
core area comparisons
game profile claims
noisy claim families

So the overall number is less important than the conditional numbers.

Better question

Instead of:

How often are we right overall?

Ask:

When this type of claim has this kind of feature support, how often does it validate?

That is where the value appears.

Examples:

two_way_context = supportive:
  Baseline: 52.8%
  Fresh: 56.2%

offense_finish_score strong_positive:
  Baseline: 55.8%
  Fresh: 53.7%

defensive_suppression_score strong_positive:
  Baseline: 52.6%
  Fresh: 53.2%

Those are better than the total average.

That tells us:

The system is learning which kinds of pregame claims deserve more trust.

18. SQL Testing Process for New Features

This is one of the most important parts to preserve.

We should not code a feature first just because it sounds smart.

The process should be:

1. Think of football idea.
2. Test it with SQL against existing claim table.
3. Check baseline sample.
4. Check fresh sample.
5. Look for repeatable lift.
6. Only then code it into update_claim_training_features.py.
7. Dry-run locally.
8. Validate dry-run CSV.
9. Write to BigQuery.
10. Re-check feature signal in BigQuery.
11. Commit only if meaningful.
Why SQL first?

SQL lets us test an idea without changing production code.

Example:

Does two-way offensive + defensive support matter?

Before coding, we tested it in SQL using existing:

offense_finish_score
defensive_suppression_score
validation_result

We found:

two_way_strong was not reliable
two_way_supportive was better

That prevented us from hardcoding the wrong version.

What makes a feature worth coding?

A candidate feature should show:

repeatable lift across baseline and fresh sample
enough row count to matter
football logic that makes sense
no obvious leakage
clear interpretation for API language
Warning signs

Do not code features that show:

great result in one sample, weak result in another
tiny row count only
feature applies to unrelated claim types
postgame leakage
hard-to-explain meaning
SQL testing language to use

For future feature testing, describe the question like this:

Candidate feature:
rushing_control_score

Question:
When pregame rushing-control context is supportive, do related claims validate more often?

SQL test:
Bucket rows into supportive / mixed / unavailable.
Compare validation_rate across baseline and fresh samples.
Split by claim_type and claim_layer.
Only code if supportive beats unavailable in both samples.
19. QA Process We Used Today
Step A — Create a true fresh sample

We added:

--sample-mode random
--random-seed

to:

qa_collect_gamelens_payloads.py

Fresh run command:

python qa_collect_gamelens_payloads.py \
  --seasons 2023 2024 2025 \
  --games-per-season 32 \
  --sample-mode random \
  --random-seed 20260514 \
  --run-name fresh_96_features_qa_v2
Step B — Verify it was actually fresh

Result:

baseline games: 96
fresh games: 96
same order: False
same set: False
overlap: 17

This confirmed:

79 games were different
17 games overlapped

Good.

Step C — Run Level 1
python -m agg.gamelens_training.build_claim_training_examples \
  --payload-run qa/gamelens_payload_runs/fresh_96_features_qa_v2 \
  --run-id fresh_96_features_qa_v2 \
  --write-bigquery \
  --replace-run

Result:

Rows built: 2737
Errors: 0
Step D — Run Level 2
python -m agg.gamelens_training.update_claim_training_validation \
  --run-id fresh_96_features_qa_v2 \
  --write-bigquery

Result:

Training rows loaded: 2737
Actual metric rows loaded: 6562
Validation rows built: 2737
Step E — Run Level 3
python -m agg.gamelens_training.update_claim_training_features \
  --run-id fresh_96_features_qa_v2 \
  --write-bigquery

Result:

Rows with offense_finish_score: 748
Rows with defensive_suppression_score: 371
Rows with two_way_edge_score: 2206
Step F — Dry-run before writing new feature changes

Before writing two_way_context, we ran:

python -m agg.gamelens_training.update_claim_training_features \
  --run-id fresh_96_features_qa_v2 \
  --dry-run

Then inspected the CSV locally.

Important checks passed:

supportive_null_scores = 0
available_mixed_null_scores = 0
unavailable_non_null_scores = 0
supportive_with_bad_score = 0
unavailable_with_score = 0
side-level consistency violations = 0

This confirmed the feature was mechanically correct before BigQuery write.

20. Larger Sample Testing Plan

Current tested sample:

Baseline:
96 payload slots
2708 claim rows

Fresh:
96 payload slots
2737 claim rows

Combined:
192 payload slots
5445 claim rows

This is a good start, but not enough to finalize all feature logic forever.

Next suggested testing

Run more random 96-game samples:

fresh_96_features_qa_v3
fresh_96_features_qa_v4
fresh_96_features_qa_v5

Each with:

32 games per season
2023, 2024, 2025
different random seed

Example:

python qa_collect_gamelens_payloads.py \
  --seasons 2023 2024 2025 \
  --games-per-season 32 \
  --sample-mode random \
  --random-seed 20260515 \
  --run-name fresh_96_features_qa_v3

Then run:

Level 1 → Level 2 → Level 3

Goal:

Check if feature lift repeats across 3–5 samples.
Larger historical test

The collector showed:

Candidate games loaded: 855

At roughly 14 seconds per payload:

855 × 14 seconds = about 3.3 hours locally

That is too slow for casual local testing, but reasonable for a future batch/cloud process.

Expected row count:

~28 claim rows/game × 855 games ≈ 24,000 claim rows

That is very manageable in BigQuery.

21. Other Features Still Worth Testing
1. rushing_control_score
Question
Does pregame rushing-control support help identify claims that validate?
Why it matters

Rushing/control has repeatedly appeared as a blind spot.

It affects:

clock
drive stability
game script
late-game control
variance
physical matchup control
Possible inputs
yards_per_rush
rushing_yards
rushing_attempts
run_play_pct
opponent defensive control
points_allowed_per_play
How to SQL test first

Bucket teams into:

rushing_supportive
rushing_mixed
rushing_unavailable

Then compare:

validation_rate by bucket
split by rushing-related claim types
baseline vs fresh

Only code if support repeats.

2. disruption_upside_score
Question
Does disruption/turnover/pressure context identify upside or volatility?
Possible inputs
Disruption and Turnovers
Pressure
Turnovers
sacks
turnover_margin
defensive_interceptions
fumbles_recovered
sack_to_turnover_ratio
Important caution

This should probably not be a simple confidence booster.

It may be better as:

upside warning
volatility flag
confidence cap reason

Because disruption can flip games, but it is noisy.

3. passing_efficiency_support_score
Question
Are passing claims supported by direct passing efficiency?
Possible inputs
yards_per_pass
passing_yards
pass_td_share
pass_play_pct
sacks_taken
pressure allowed if available
Why

offense_finish_score was not clean enough for broad Passing Game claims.

Passing deserves its own support feature.

4. drive_sustainability_score
Question
Can this team stay on schedule and keep drives alive?
Possible inputs
third_down_pct
1st_down_rate
yards_per_play
total_drives
time_of_possession
Why

This is different from scoring explosiveness.

A team can sustain drives but struggle in the red zone.

5. red_zone_finish_score
Question
Can this team turn scoring chances into touchdowns?
Possible inputs
red_zone_efficiency
td_rate
points_per_play
Why

This may become a cleaner sub-feature than broad offense_finish_score.

6. confidence_cap_reason
Question
Why should confidence be held down?

Possible reasons:

split_core_areas
feature_conflict
turnover_volatility
missing_feature_support
offense_defense_mismatch
metric_only_defensive_support_rejected
early_season_low_data
two_way_unavailable
Why this matters

The API may need cap reasons more than more scores.

Example future API language:

“Confidence capped because the offensive profile is supportive, but defensive suppression is unavailable.”
7. hidden_lean_score
Question
In No Pick / Low Confidence games, was there still a soft structural lean?
Possible inputs
offense_finish_score
defensive_suppression_score
two_way_context
rushing_control_score
disruption_upside_score
core_area split
team comparison edge score
Caution

This should not turn every No Pick into a pick.

Better use:

watchlist lean
soft lean
upside warning
22. Production Adoption Plan
Current state

Right now this is still a local QA workflow.

It reads from:

qa/gamelens_payload_runs/...

That is fine for testing, but not final production.

Production goal

Eventually this should run after daily data loads.

Production sequence:

Internal payload / metrics hit BigQuery
↓
Boxscore / game facts loaded
↓
Windowed metrics / rankings updated if needed
↓
GameLens claim-training pipeline runs:
  Level 1
  Level 2
  Level 3
↓
Future Level 4 calibration summary updates
↓
/game API reads prepared calibration outputs
App.py role

app.py should not contain the training logic.

Better design:

app.py = trigger/orchestration endpoint
agg/gamelens_training/ = worker logic
BigQuery = storage and output
/game API = reads prepared calibration outputs
Future endpoint
POST /internal/gamelens-training/run

Possible body:

{
  "run_id": "daily_2026_05_14",
  "season": "2025",
  "mode": "daily"
}
Triggering

Use Cloud Scheduler to send a secure POST to Cloud Run.

Cloud Scheduler
↓
Authenticated POST
↓
Cloud Run / app.py internal endpoint
↓
Sequential worker execution
Security

Do not leave the endpoint public.

Use:

Cloud Scheduler service account
Cloud Run Invoker role
internal auth guard if needed
Daily worker order
Level 1 build claims
↓
Level 2 validate claims
↓
Level 3 update feature scores
Level 0 is not daily
create_claim_training_examples_table.py

is setup/migration only.

23. Future Level 4 Calibration

The next major architectural step is not necessarily another feature.

It may be a calibration summary table.

Possible table:

Analytics.gamelens_claim_calibration_summary

Possible grain:

feature_formula_version
claim_type
claim_layer
claim_name
core_area
category
metric
feature_bucket
sample_size
validation_rate
confidence_modifier
language_modifier

This would allow the API to ask:

Historically, how reliable is this kind of claim under this kind of feature context?

Then API language can become more disciplined.

Example:

Claim:
Team A has a metric highlight in third_down_pct.

Context:
two_way_context = supportive.

Historical result:
This combination validates at a better-than-average rate.

API language:
“Supported by two-way context.”
24. Current Known Good State
Files
agg/gamelens_training/create_claim_training_examples_table.py
agg/gamelens_training/build_claim_training_examples.py
agg/gamelens_training/update_claim_training_validation.py
agg/gamelens_training/update_claim_training_features.py
qa_collect_gamelens_payloads.py
BigQuery table
nfl-stream-406420.Analytics.gamelens_claim_training_examples
Active formula version
offense_finish_v2__defensive_suppression_v3__two_way_context_v1
Confirmed run IDs
baseline_96_stage1_v2
fresh_96_features_qa_v2
Combined claim rows tested
5445 claim rows
Strongest repeatable feature signal
two_way_context = supportive

Validation:

Baseline: 52.8%
Fresh:    56.2%

Compared to:

available_mixed:
  Baseline: 44.8%
  Fresh:    47.6%

unavailable:
  Baseline: 45.2%
  Fresh:    48.8%
25. Final Interpretation

Today’s work created a real feedback loop.

Before:

GameLens generated matchup reasoning.

Now:

GameLens can store its pregame claims,
validate those claims against postgame data,
test which pregame feature contexts improve validation,
and eventually use that evidence to improve confidence language.

The most important conceptual takeaway:

We are not just asking whether GameLens picked the winner.
We are asking whether GameLens correctly identified the matchup truths that were knowable before the game.

That is the right foundation for a smarter and more trustworthy GameLens API.