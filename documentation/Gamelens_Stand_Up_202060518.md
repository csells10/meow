# GameLens Tomorrow Work Plan — Level 3 QA Analysis

_Date prepared: May 17, 2026_  
_For work on: May 18, 2026_

## Purpose

Tomorrow's goal is to analyze the larger QA run collected today and decide whether the current Level 3 feature layer is reliable enough to support Level 4 calibration work.

The main question is **not**:

```text
Did GameLens pick the winner?
```

The better question is:

```text
When GameLens makes a pregame claim, do the Level 3 features help identify when that claim is more likely to validate after the game?
```

---

## Current Product Direction

Frontend expansion is paused.

Level 3 feature engineering exists as a first working version:

```text
offense_finish_score
defensive_suppression_score
two_way_edge_score
two_way_context
```

The next step is to test whether these features hold up across a larger sample before building Level 4 calibration.

---

## Today’s Target Setup

Today’s goal is to collect a larger QA sample so tomorrow can focus on analysis instead of setup.

### Recommended sample

```text
240 games total
80 games from 2023
80 games from 2024
80 games from 2025
```

Try to keep the sample stratified across:

```text
early regular season
mid regular season
late regular season
postseason
```

### Suggested run naming

Use names that clearly separate this from the old 96-game baseline.

```text
payload run: qa/gamelens_payload_runs/level3_240_stratified_20260517
claim run id: level3_240_stage1_20260517
validation run: claims_240_level3_20260517
feature run: features_240_level3_20260517
```

Exact names can differ, but avoid overwriting:

```text
qa/gamelens_payload_runs/baseline_32_v2
qa/gamelens_claim_validation_runs/claims_96_v2
baseline_96_stage1_v2
fresh_96_features_qa_v2
fresh_96_features_qa_v3
```

---

## Ideal State Before Starting Tomorrow

Tomorrow will go much faster if today produces:

- a completed payload collection folder
- a sample metadata file showing which games were selected
- Level 1 claim rows built
- Level 2 validation results attached
- Level 3 features attached
- no known fatal errors
- baseline 96-game outputs preserved

Minimum acceptable state:

```text
The 240-game payload collection exists and can be processed tomorrow.
```

Ideal state:

```text
The full Level 1 → Level 2 → Level 3 pipeline has already run on the 240-game sample.
```

---

# Tomorrow Workflow

## Step 1 — Confirm the QA run is usable

Before interpreting results, confirm the sample itself is clean.

Check:

- total games collected
- games per season
- early/mid/late/postseason distribution
- duplicate game count
- failed payload count
- claim row count
- validation unavailable count
- Level 3 feature-null counts
- tie / push / no-decision handling

Expected rough shape:

```text
~240 games
~7,000+ claim rows, depending on payload shape
unavailable validation should be low
no fatal pipeline errors
```

If the sample is badly imbalanced or has lots of failures, fix the QA run before drawing conclusions.

---

## Step 2 — Compare 96-game baseline vs 240-game sample

Use the old 96-game run as the baseline, not as final truth.

Compare:

| Area | Question |
|---|---|
| Overall claim validation | Did the larger sample stay near the 96-game result or move meaningfully? |
| Headline vs supporting claims | Do headline claims validate better, worse, or the same? |
| Claim type | Which claim types improved or weakened? |
| Core Area | Which football areas are more reliable? |
| Confidence tiers | Does High actually validate better than Medium/Low? |
| No Pick cases | Are No Picks mostly good restraint or too passive? |

---

## Step 3 — Test Level 3 features directly

This is the main work.

### `two_way_context`

Check validation rate by:

```text
supportive
available_mixed
unavailable
```

Then split by:

```text
claim_type
claim_layer
metric
season
bucket / phase
```

Key question:

```text
Does supportive still validate better than available_mixed and unavailable in the larger sample?
```

Important rule:

```text
two_way_context should remain claim-language support, not winner confidence.
```

---

### `offense_finish_score`

Check bucketed validation rates.

Suggested buckets:

```text
strong_positive
positive
mixed_near_even
negative
strong_negative
```

Key question:

```text
Do positive offense-finish buckets still validate better for offensive / finishing claim families?
```

Focus claim families:

```text
Scoring Efficiency
Drive Conversion
Red Zone Finish
Rushing Game
third_down_pct
red_zone_efficiency
yards_per_rush
```

---

### `defensive_suppression_score`

Check bucketed validation rates.

Key question:

```text
Does defensive suppression help identify when defensive claims deserve stronger language?
```

Focus claim families:

```text
Defensive Control
Scoring Suppression
points_allowed_per_play
points_allowed_per_yard
yards_allowed
points_allowed
defensive_success_rate
```

Important caution:

```text
Do not let isolated points_allowed_per_play become the whole defensive story by itself.
```

---

### `two_way_edge_score`

Use this as a numeric companion to `two_way_context`.

Check:

- distribution by side
- distribution by claim validation result
- whether high positive values correspond to higher validation
- whether it adds anything beyond the categorical `two_way_context`

Key question:

```text
Is two_way_edge_score useful as a future calibration input, or is two_way_context enough for now?
```

---

## Step 4 — Review Core Area reliability

Analyze validation by Core Area:

```text
Scoring Efficiency
Offensive Output
Defensive Control
Disruption and Turnovers
Field Control, if present, should remain ignored/audit only
```

Questions:

- Which Core Areas validate best?
- Which ones are too noisy alone?
- Does Core Area agreement improve validation?
- Does split Core Area context cap confidence correctly?
- Are pressure/turnover-heavy reads still weaker?

Special caution:

```text
Pressure and turnovers should probably be treated as volatility/upside context unless supported by stronger surrounding evidence.
```

---

## Step 5 — Check confidence calibration

Group by:

```text
profile_strength.label
outcome_confidence.label
matchup_label
matchup_cautions
model_trust.edge.strength
model_trust.signal_alignment.summary_label
```

Questions:

- Does High confidence validate better than Medium and Low?
- Are Strong Profile games actually cleaner?
- Are Low Confidence games mostly correct caution?
- Do caution codes explain why confidence was held down?
- Are there High Confidence failures with poor claim validation?

If High confidence does not clearly validate better, do not panic.

Interpretation:

```text
The model may have useful matchup structure, but confidence promotion rules still need calibration.
```

---

## Step 6 — Inspect the most valuable game buckets

Do not manually inspect all 240 games first.

Start with buckets that teach the most.

### Bucket A — Bad reasoning + bad outcome

Goal:

```text
Find where GameLens was wrong for the wrong reasons.
```

Questions:

- Was the miss pressure-heavy?
- Was the miss turnover-heavy?
- Did season-to-date data overrule recent form?
- Did Core Areas split but confidence still rose too high?
- Did the claim validator show poor headline validation?

---

### Bucket B — Correct outcome but mixed reasoning

Goal:

```text
Separate lucky correctness from good reasoning.
```

Questions:

- Did the model win the outcome but miss the football story?
- Did Level 3 features explain why the lean was still reasonable?
- Should confidence have been lower?

---

### Bucket C — No Pick with strong validated claims

Goal:

```text
Find possible hidden-lean games.
```

Questions:

- Was No Pick good restraint?
- Did one side have enough validated claim support for a cautious lean?
- Did Level 3 features identify the stronger side pregame?

Do not punish No Pick automatically.

---

### Bucket D — Supportive false positives

Goal:

```text
Find where two_way_context looked supportive but claims still failed.
```

Questions:

- Which metrics failed despite support?
- Were they outside the allowlist?
- Were they volatile metrics like turnovers, third downs, or pressure?
- Should any allowlist entries be downgraded?

---

## Step 7 — Decide what earns Level 4 calibration

Only after the larger QA review, decide what should feed Level 4.

Possible Level 4 table:

```text
Analytics.gamelens_claim_calibration_summary
```

Possible grain:

```text
feature_formula_version
claim_type
claim_layer
core_area
category
metric
feature_bucket
sample_size
validation_rate
language_modifier
confidence_modifier
```

Level 4 should answer:

```text
Historically, how reliable is this type of claim under this feature context?
```

It should not directly answer:

```text
Who will win?
```

---

# Decisions To Make Tomorrow

By the end of tomorrow, try to answer these:

| Decision | Answer |
|---|---|
| Does `two_way_context = supportive` still validate better in the larger sample? | TBD |
| Should `two_way_context` remain claim-language support only? | Likely yes |
| Does `offense_finish_score` deserve Level 4 calibration use? | TBD |
| Does `defensive_suppression_score` deserve Level 4 calibration use? | TBD |
| Is `two_way_edge_score` useful beyond `two_way_context`? | TBD |
| Are pressure/turnover claims still too noisy for stronger language? | TBD |
| Does High confidence need stricter promotion rules? | TBD |
| Should the next feature candidate be disruption upside, hidden lean, or confidence suppression? | TBD |

---

# Do Not Do Tomorrow

Do not:

- add new frontend fields
- create more feature scores before analyzing the 240-game run
- let `two_way_context` boost winner confidence
- turn rankings into automatic pick logic
- treat No Pick as failure by default
- rewrite the model based on one sample
- start a full frontend redesign
- build public accuracy UI

---

# Expected Outputs From Tomorrow

Aim to finish with:

```text
1. A short QA summary of the 240-game run
2. A comparison against the 96-game baseline
3. Level 3 feature reliability tables
4. A list of trusted, watch, and rejected feature uses
5. A proposed Level 4 calibration table/grain
6. A short next-step recommendation
```

Suggested output file name:

```text
GameLens_Level3_240_QA_Findings.md
```

---

# One-Sentence North Star

```text
Use the larger QA run to decide which Level 3 features reliably support claim language, then turn those repeatable patterns into Level 4 calibration rules.
```







GameLens Level 3 240-Game QA Findings

Date: May 18, 2026
_Run ID: larger_240_level3_qa_20260517

1. Purpose

This larger QA run tested whether the current Level 3 engineered feature layer helps identify when GameLens pregame claims are more likely to validate after the game.

The goal was not simply to ask:

Did GameLens pick the winner?

The better question was:

When GameLens makes a pregame claim, do Level 3 features help identify when that claim deserves stronger, softer, or capped language?

This matches the current GameLens direction: matchup intelligence and confidence calibration, not a forced pick machine. The stand-up doc framed this as testing whether Level 3 features help identify when claims validate, rather than only checking winner prediction.

2. Payload Collection Result

The larger payload collection was clean.

Item	Result
Games selected	240
Payloads collected	240
Failed games	0
2023 games	80
2024 games	80
2025 games	80
Early regular season	63
Mid regular season	63
Late regular season	75
Postseason	39

This was a strong sample setup: balanced by season and reasonably spread across season phases.

Week 1 note

Week 1 games showed missing ranking context, missing matchup breakdown, and missing visible turnover_margin_per_game.

Current interpretation:

This is expected under the current pregame-safe ranking setup because Week 1 has no prior current-season ranking/window context.

Do not treat this as a bug yet. It may need a future Week 1 handling rule, but it should not block this QA run.

3. Level 1 — Claim Rows Built

Level 1 successfully built claim rows from the 240 payloads.

Item	Result
Payload files found	240
Claim rows built	6,775
Errors	0
Claim type distribution
Claim type	Rows
metric_highlight	1,791
category_summary	1,670
team_comparison_metric	1,051
core_area_comparison	834
core_area_summary	816
game_profile	613
Claim layer distribution
Claim layer	Rows
headline	2,122
supporting	4,653
Season distribution
Season	Rows
2023	2,223
2024	2,243
2025	2,309

This is a healthy row count for a 240-game run.

4. Level 2 — Claim Validation Result

Level 2 successfully validated all claim rows against postgame metric facts.

Item	Result
Training rows loaded	6,775
Actual metric rows loaded	16,048
Validation rows built	6,775
Actual load errors	0
Validation result distribution
Validation result	Rows	Rate
validated	3,335	49.2%
not_validated	2,595	38.3%
actual_neutral_or_mixed	845	12.5%

This larger sample showed overall claim validation around 49.2%, which is slightly stronger than the older ~46–47% baseline discussed earlier.

QA read distribution
QA read	Rows
good_reasoning_correct_outcome	1,768
bad_reasoning_bad_outcome	1,274
mixed_review	1,094
no_pick_claim_review	2,047
outcome_correct_reasoning_mixed	371
good_reasoning_bad_outcome	221

Important interpretation:

Winner correctness and claim correctness are not the same thing.
A team can lose while a claim validates.
A team can win while the reasoning is mixed.

That distinction should remain central to GameLens QA.

5. Level 3 — Feature Update Result

Level 3 successfully attached engineered features to all 6,775 claim rows.

Formula version:

offense_finish_v2__defensive_suppression_v3__two_way_context_v1
Feature field	Rows present
offense_finish_score	1,883
defensive_suppression_score	878
two_way_edge_score	5,378
two_way_context distribution
Context	Rows
available_mixed	3,476
supportive	1,902
unavailable	1,397

This gave enough volume to evaluate two_way_context seriously.

6. Main Finding — two_way_context Passed the Bigger Test

The most important result:

two_way_context	Rows	Validated	Validation rate
supportive	1,902	1,142	60.04%
unavailable	1,397	689	49.32%
available_mixed	3,476	1,504	43.27%
Interpretation
two_way_context = supportive is a meaningful positive claim-support signal.
available_mixed is not neutral; it should likely act as a softener/caution bucket.
unavailable should behave more like a neutral fallback.

This supports using two_way_context in Level 4 calibration, but only as claim-language support, not as winner confidence or pick logic.

7. two_way_context by Claim Type / Layer

supportive generally beat the other buckets across claim type and layer.

Claim type	Layer	Supportive validation rate	Read
core_area_summary	supporting	62.55%	Strong
metric_highlight	headline	63.21%	Strong
core_area_comparison	headline	60.08%	Strong
metric_highlight	supporting	60.00%	Strong
team_comparison_metric	supporting	59.93%	Strong
category_summary	supporting	59.87%	Strong
game_profile	headline	52.14%	Weak / do not prioritize
Decision
two_way_context should be used for metric, category, team comparison, and core-area claim-language support.
Do not prioritize it for Game Profile claims yet.

Game Profile remains too broad/noisy to be a first Level 4 target.

8. Metric-Specific two_way_context Findings

The larger sample showed that two_way_context = supportive is useful, but metric-specific.

Strongest candidates for firmer claim language
Metric	Supportive validation rate	Decision
points_allowed_per_play	70.4%	Strong allowlist
points_per_play	68.9%	Strong allowlist
1st_down_rate	64.3%	Strong allowlist
Conditional / watch
Metric	Supportive validation rate	Decision
third_down_pct	62.6%	Conditional watch

third_down_pct performed better than expected, but it should not be broadly promoted yet because phase splits were less clean.

Downgrade / do not boost broadly
Metric	Supportive validation rate	Decision
red_zone_efficiency	46.1%	Downgrade / do not boost broadly
turnover_margin_per_game	51.9%	Keep blocked for stronger language
yards_per_pass	65.1%	Useful metric maybe, but two-way context did not clearly improve it
yards_per_play	65.0%	Useful metric maybe, but unavailable was also high
yards_per_rush	61.7%	Watch, but do not boost broadly

The surface-level results also showed strong support for points_allowed_per_play and points_per_play across several claim surfaces.

9. Phase / Bucket Findings

Phase split confirmed that some metrics are stable and others are not.

points_allowed_per_play
Bucket	Supportive validation rate
Early regular	82.9%
Mid regular	67.2%
Late regular	66.1%
Postseason	76.7%

Decision:

points_allowed_per_play is the cleanest current “Backed by the matchup” candidate.
points_per_play

Strong across buckets overall, though late regular season had an unavailable spike.

Decision:

points_per_play remains a strong allowlist candidate.
1st_down_rate

Strong in early, mid, and late regular season. Postseason supportive rows were limited in the output.

Decision:

1st_down_rate remains a strong allowlist candidate, with postseason watch.
third_down_pct

Supportive was good overall, but mid/late buckets were less clean because unavailable or mixed buckets also performed well in places.

Decision:

third_down_pct should remain conditional/watch, not broadly promoted.
red_zone_efficiency

Very unstable:

Bucket	Supportive validation rate
Early regular	81.3%
Mid regular	26.8%
Postseason	20.8%

Decision:

red_zone_efficiency should be downgraded and should not receive broad stronger-language support.
turnover_margin_per_game

Supportive usually beat mixed, but the absolute validation rate remained modest and turnovers remain volatile.

Decision:

turnover_margin_per_game should remain blocked from stronger language.
Use it as normal context / volatility context only.

These phase findings came from the bucket split query over the larger 240-game run.

10. Component Score Findings
offense_finish_score

Bucket results:

Bucket	Rows	Validation rate
strong_positive	898	52.9%
mixed_near_even	257	50.2%
missing	4,892	48.9%
strong_negative	121	48.8%
negative	174	47.1%
positive	433	45.7%

Decision:

offense_finish_score is weak/messy as a standalone calibration feature.
It should remain an ingredient in two_way_context, not a standalone Level 4 driver yet.
defensive_suppression_score

Bucket results:

Bucket	Rows	Validation rate
strong_positive	567	63.7%
missing	5,897	48.0%
mixed_near_even	23	47.8%
positive	288	45.5%

Decision:

defensive_suppression_score is useful only at strong_positive.
Use defensive_suppression_score >= 0.40 as a possible secondary support flag.
Do not use positive/mixed defensive buckets.
11. Defensive Suppression Design Finding

After reviewing update_claim_training_features.py, the defensive feature shape makes sense.

The current defensive formula:

requires Defensive Control
uses only limited confirming metrics:
points_allowed_per_play
points_allowed_per_yard
disables metric-only fallback
applies from the claimed side’s perspective

Because of this, defensive_suppression_score behaves more like:

a narrow strong-support detector

rather than:

a full symmetric positive / neutral / negative defensive scale

This is not necessarily a bug, but it is a design limitation. The code explicitly requires Defensive Control and uses confirming scoring-suppression metrics only as sharpening inputs.

Decision
For Level 4, treat only defensive_suppression_score >= 0.40 as meaningful.
Do not expect useful negative defensive buckets from the current formula.
If a full defensive scale is desired later, compute side-level defensive context for every game/team first, then let claims inherit it.
12. Current Level 4 Direction

The 240-game QA run supports building Level 4 calibration around:

two_way_context
claim_type
claim_layer
metric
feature bucket
sample_size
validation_rate
language_modifier
confidence_modifier

Possible future table:

Analytics.gamelens_claim_calibration_summary

Possible grain:

feature_formula_version
claim_type
claim_layer
core_area
category
metric
feature_bucket
sample_size
validation_rate
language_modifier
confidence_modifier

Level 4 should answer:

Historically, how reliable is this kind of claim under this feature context?

It should not directly answer:

Who will win?
13. First Proposed Level 4 Allowlist
Strong allowlist

Allow firmer claim language / “Backed by the matchup” for:

points_allowed_per_play
points_per_play
1st_down_rate

Only when:

language_support.language_boost_allowed = true

and the row meets the appropriate claim type / claim layer / metric requirements.

Conditional watch
third_down_pct

Use cautiously. Do not broadly promote yet.

Block / downgrade from stronger language
red_zone_efficiency
turnover_margin_per_game
td_rate
yards_per_play
yards_per_pass
yards_per_rush

These may still appear as normal claims, but should not automatically receive stronger “Backed by the matchup” language.

14. Product Language Decision

The correct frontend/product meaning remains:

Backed by the matchup

Good tooltip:

Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.

Do not use language like:

High confidence
Prediction boost
Win signal
Model lock
Best bet
Outcome support
Key rule
Do not show stronger language just because two_way_context = supportive.
Only show it when language_support.language_boost_allowed = true.
15. Current Do / Do Not
Do
Use two_way_context as claim-language support.
Use available_mixed as a caution/softener.
Use points_allowed_per_play, points_per_play, and 1st_down_rate as first strong allowlist candidates.
Use defensive_suppression_score >= 0.40 as a possible secondary support flag.
Keep frontend expansion paused until calibration is clearer.
Do not
Do not use two_way_context as winner confidence.
Do not use it as pick confidence.
Do not use it to override Model Trust.
Do not use it to automatically promote High Confidence.
Do not broadly boost red_zone_efficiency.
Do not broadly boost turnover_margin_per_game.
Do not treat offense_finish_score as standalone calibration yet.
Do not expect defensive_suppression_score to behave like a full positive/negative scale.
16. Recommended Next Tests

The next tests should be decision-focused, not random.

Priority	Test	Why
1	Allowlist by surface: metric + claim_type + claim_layer	Decide exactly where “Backed by the matchup” is safe
2	Season split for top metrics	Make sure 2023, 2024, 2025 all support the same conclusion
3	Supportive false positives	Learn when supportive still fails
4	Outcome confidence calibration	See whether High confidence actually has stronger claim validation
5	No Pick hidden-lean review	Find whether some No Picks deserve cautious lean language
6	Defensive suppression drilldown	Determine whether strong defensive suppression is more than just points_allowed_per_play
7	Available mixed softener test	Confirm whether available_mixed should actively suppress stronger language
8	Week 1 handling	Decide whether Week 1 needs a separate unavailable/insufficient-context label
17. Bottom Line

The 240-game QA run was a strong success.

Main conclusion:

Level 3 is useful, but not all Level 3 features are equally useful.

Best current finding:

two_way_context = supportive is a real claim-language support signal.

Best current metrics:

points_allowed_per_play
points_per_play
1st_down_rate

Most important product boundary:

Use these findings to calibrate claim language, not to force winner predictions.

One-sentence north star:

Use the 240-game QA findings to build Level 4 calibration that decides which claims deserve stronger, softer, or capped language.



| Priority | Test                                                        | Why it matters                                                                                                                             | Decision it answers                                         |
| -------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------- |
| **1**    | **Allowlist by surface**: metric + claim_type + claim_layer | You know which metrics look good overall, but the badge may behave differently in Team Comparison vs Metric Highlight vs Category Summary. | Where should `Backed by the matchup` be allowed?            |
| **2**    | **False positives for supportive**                          | Find cases where `supportive` existed but the claim failed.                                                                                | What should cap or block the badge?                         |
| **3**    | **Season split**                                            | Make sure 2023, 2024, and 2025 all support the same conclusion.                                                                            | Is one season carrying the signal?                          |
| **4**    | **Outcome confidence calibration**                          | High confidence still had many misses in the payload QA review.                                                                            | Does High actually deserve High?                            |
| **5**    | **No Pick hidden-lean review**                              | Some No Pick games may have strong claim support.                                                                                          | Should GameLens sometimes say “No Pick, but cautious lean”? |
| **6**    | **Defensive suppression strong-positive drilldown**         | `defensive_suppression_score >= .40` looked useful, but we need to know whether it’s just `points_allowed_per_play` in disguise.           | Is it a real feature or just a wrapper?                     |
| **7**    | **Available mixed as a softener**                           | `available_mixed` was consistently weakest.                                                                                                | Should it actively prevent stronger language?               |
| **8**    | **Week 1 / missing ranking exclusion**                      | Week 1 lacks ranking/matchup breakdown under your current pregame-safe setup.                                                              | Should Level 4 exclude or label Week 1 separately?          |

Season Test two-way-context
### Strong allowlist
- points_allowed_per_play
- 1st_down_rate

### Strong, but watch
- points_per_play

### Conditional allowlist candidate
- third_down_pct

### Block from automatic stronger language
- red_zone_efficiency
- turnover_margin_per_game
- td_rate
- yards_per_play
- yards_per_pass
- yards_per_rush

# GameLens Future Feature Candidates

## Purpose

These are future feature ideas to test after the 240-game Level 3 QA run.

Important rule:

Do not code all of these immediately.

Each feature should go through:

1. Football hypothesis
2. SQL prototype
3. QA against existing claim-validation data
4. Fresh random sample test
5. Calibration decision
6. Product-language decision

A feature is only useful if it earns a clear role:

- stronger claim language
- softer claim language
- confidence cap
- volatility warning
- No Pick refinement
- internal QA only

---

# 1. Drive Killer Index

## Plain-English Meaning

Can this defense stop drives before they become points?

This is about whether a defense can prevent opponents from sustaining possessions and finishing drives.

## Football Hypothesis

A defensive edge is stronger when the team is not just good in one defensive metric, but shows multiple signs of ending or suppressing drives.

## Candidate Inputs

Core Areas:

- Defensive Control
- Scoring Efficiency / Scoring Suppression

Possible metrics:

- points_allowed_per_play
- 1st_down_rate allowed / opponent 1st_down_rate
- third_down_pct defense
- red_zone_efficiency allowed
- yards_allowed
- yards_per_play allowed
- points_allowed_per_yard
- defensive_success_rate, if reliable

## Possible Internal Field Name

```text
drive_killer_score
Likely Product Role

Strong defensive claim support.

This could help support language like:

This defense has a cleaner drive-killing profile.

What It Should Not Do

Do not use directly as winner confidence.

First QA Question

When Drive Killer Index is strong, do defensive claims validate more often?

Especially:

points_allowed_per_play
Defensive Control
Scoring Suppression
opponent drive-sustain claims
2. Stay-on-Schedule Edge
Plain-English Meaning

Can this offense consistently keep drives manageable?

This is about first downs, efficiency, avoiding stalled drives, and staying ahead of the chains.

Football Hypothesis

An offense with strong stay-on-schedule traits is more reliable than an offense that only has isolated explosive or scoring metrics.

Candidate Inputs

Core Areas:

Offensive Output
Scoring Efficiency

Possible metrics:

1st_down_rate
third_down_pct
yards_per_play
yards_per_rush
pass_run_ratio, context only
total_plays
time_of_possession, context only
offensive_snap_load, context only
Possible Internal Field Name
stay_on_schedule_score
Likely Product Role

Offensive rhythm / drive sustainability support.

This could support language like:

This offense has a cleaner path to sustaining drives.

What It Should Not Do

Do not treat volume alone as quality.

High total plays or high snap load should not automatically mean the offense is better.

First QA Question

When Stay-on-Schedule Edge is strong, do claims around 1st_down_rate, third_down_pct, and Offensive Output validate more often?

3. Chaos Threat
Plain-English Meaning

Can this team create pressure, sacks, turnovers, or broken drives that swing the game?

This is the “things could get messy” feature.

Football Hypothesis

Pressure and turnovers are noisy alone, but they may become more meaningful when paired with opponent vulnerability.

Candidate Inputs

Core Areas:

Disruption and Turnovers
Defensive Control, as support only

Possible metrics:

sacks
pressure-related proxy, if available
sacks_taken by opponent
turnover_margin_per_game
defensive_interceptions
interceptions_thrown by opponent
fumbles_lost by opponent
pass_attempts / opponent pass volume
yards_per_pass allowed
opponent pass_play_pct
Possible Internal Field Name
chaos_threat_score
Likely Product Role

Volatility warning / upset potential / confidence cap.

This should create caution, not automatic confidence.

Good Product Language

This matchup has chaos potential.

The pressure and turnover profile could swing the game, but it should raise caution more than confidence.

What It Should Not Do

Do not use as a straight win signal.

Do not say:

This team should win because it has chaos upside.

First QA Question

When Chaos Threat is high, are high-confidence misses more common?

Does it explain variance games, upsets, or bad-reasoning/bad-outcome buckets?

4. Empty Yards Detector
Plain-English Meaning

Can this team move the ball but fail to turn that movement into points?

This feature looks for offenses that produce yardage or movement but do not finish drives well.

Football Hypothesis

Offensive Output without Scoring Efficiency can create misleading confidence.

A team may look good between the 20s but fail to finish.

Candidate Inputs

Core Areas:

Offensive Output
Scoring Efficiency

Possible metrics:

Positive movement inputs:

total_yards
yards_per_play
1st_down_rate
total_plays
yards_per_pass
yards_per_rush

Weak finishing inputs:

points_per_play
red_zone_efficiency
td_rate
points_per_yard, if available
scoring drives, if available
Possible Internal Field Name
empty_yards_flag

or

empty_yards_score
Likely Product Role

Confidence softener.

This helps avoid overrating teams that move the ball but do not score enough.

Good Product Language

The offense can move the ball, but the profile is less convincing because those drives have not reliably turned into points.

What It Should Not Do

Do not punish every offense with mediocre red-zone numbers automatically.

This should only matter when Offensive Output looks strong but finishing support is weak.

First QA Question

When Empty Yards Detector is active, do Offensive Output claims validate but Scoring Efficiency / outcome confidence disappoint?

5. Bend-Don’t-Break Profile
Plain-English Meaning

Does this defense allow some movement but still limit scoring?

This is a defensive nuance feature.

Football Hypothesis

Some defenses may give up yards or first downs but still prevent points. That should produce different language than a defense that dominates everywhere.

Candidate Inputs

Movement allowed:

yards_allowed
yards_per_play allowed
1st_down_rate allowed
third_down_pct allowed

Scoring suppression:

points_allowed_per_play
red_zone_efficiency allowed
points_allowed_per_yard
defensive red-zone suppression, if available
Possible Internal Field Name
bend_dont_break_score
Likely Product Role

Defensive nuance / language modifier.

This could explain why a defensive profile is useful but not dominant.

Good Product Language

This looks more like a bend-don’t-break defensive profile than a full shutdown edge.

What It Should Not Do

Do not treat it as the same thing as dominant defense.

First QA Question

Do Bend-Don’t-Break profiles validate defensive scoring claims better than yardage claims?

6. Finish the Drive Score
Plain-English Meaning

Can this offense turn possessions and movement into points?

This is the cleaner version of offensive finishing.

Football Hypothesis

Scoring-related offensive claims are stronger when movement metrics and finishing metrics both support the same team.

Candidate Inputs

Core Areas:

Offensive Output
Scoring Efficiency

Possible metrics:

points_per_play
red_zone_efficiency
td_rate
1st_down_rate
third_down_pct
yards_per_play
yards_per_rush
total_drives
points_per_drive, if available later
Possible Internal Field Name
finish_the_drive_score
Likely Product Role

Stronger offensive claim language.

Good Product Language

This offense has a cleaner finishing profile.

They are not just moving the ball; they are turning drives into points.

What It Should Not Do

Do not make this a duplicate of offense_finish_score unless the formula is redesigned and proven cleaner.

First QA Question

Does Finish the Drive Score outperform the current offense_finish_score, which was weak as a standalone feature in the 240-game QA run?

7. Trap Door Game Flag
Plain-English Meaning

Does the lean look tempting, but the football profile has hidden risk?

This is a warning flag for games where the model might be too confident.

Football Hypothesis

Some games have a visible lean, but the profile contains enough conflict or volatility that confidence should be capped.

Candidate Inputs

Possible warning inputs:

core_areas_are_split
core_area_gap_is_small
core_areas_do_not_fully_confirm_lean
available_mixed two_way_context
pressure-heavy support
turnover-heavy support
weak scoring efficiency support
one-stat concentration
poor headline claim reliability
Week 18 / postseason context
recent-form conflict, if available later
injury context, if available later
Possible Internal Field Name
trap_door_flag

or

trap_door_score
Likely Product Role

Confidence cap / caution language.

Good Product Language

The lean is there, but this is a trap-door profile: one or two volatile areas could flip the read.

What It Should Not Do

Do not reverse the lean automatically.

This is a caution feature, not a pick changer.

First QA Question

Do Trap Door games explain High Confidence Incorrect cases or bad_reasoning_bad_outcome cases?

8. Hidden Lean Detector
Plain-English Meaning

Did a No Pick game actually have enough structure to support a cautious lean?

This is for improving No Pick logic without forcing picks.

Football Hypothesis

Some No Pick games may contain strong validated pregame claims, but the current model stayed too passive.

Candidate Inputs

Possible positive inputs:

supportive two_way_context
multiple validated-looking top metrics
points_allowed_per_play edge
points_per_play edge
1st_down_rate edge
strong core_area agreement
low opposing signal count
strong profile but low confidence

Possible negative inputs:

core_area_gap_is_small
core_areas_are_split
available_mixed context
turnover-heavy signal
pressure-only signal
Possible Internal Field Name
hidden_lean_score
Likely Product Role

No Pick refinement.

Good Product Language

No clear pick, but one side has a cautious structural lean.

What It Should Not Do

Do not punish good No Picks.

Some No Picks are exactly the right answer.

First QA Question

Among No Pick games, do high hidden_lean_score games have stronger claim validation or larger final margins?

9. One-Stat Wonder Flag
Plain-English Meaning

Is the model leaning too hard on one metric?

This feature catches cases where one strong stat may be doing too much work.

Football Hypothesis

A claim is less trustworthy when the profile depends heavily on one metric and lacks broader support.

Candidate Inputs

Possible inputs:

one metric has a large gap
low core_area_agreement_rate
few supporting metrics
opposing_signal_count is high
two_way_context = available_mixed
only one Core Area favors the target team
metric edge exists but related category/core area does not agree
Possible Internal Field Name
one_stat_wonder_flag

or

edge_concentration_score
Likely Product Role

Language softener / confidence cap.

Good Product Language

This edge is real, but it is narrow.

The read leans on one standout stat rather than broad matchup support.

What It Should Not Do

Do not hide the metric.

The metric may still be useful. Just do not over-promote it.

First QA Question

Do one-stat-heavy claims validate less often than claims supported by multiple related metrics?

10. Complete Team Pressure
Plain-English Meaning

Does one team have multiple ways to control the matchup?

This is the more football-friendly version of a broad two-way profile.

Football Hypothesis

A team is more trustworthy when it has both an offensive path and a defensive path in the matchup.

Candidate Inputs

Current related feature:

two_way_context

Possible future inputs:

offense_finish_score
defensive_suppression_score
points_per_play
points_allowed_per_play
1st_down_rate
Defensive Control
Scoring Efficiency
Offensive Output
Possible Internal Field Name
complete_team_pressure_score

or keep internal:

two_way_context
Likely Product Role

Claim-language support, not winner prediction.

Good Product Language

This read is not standing alone; the broader profile gives the same team multiple paths.

What It Should Not Do

Do not expose this as “two-way context” to users.

Do not use it as automatic High Confidence.

First QA Question

Does this improve over the current two_way_context, or is two_way_context already enough?

Suggested Testing Order
First group: strongest practical candidates
Drive Killer Index
Trap Door Game Flag
Empty Yards Detector
Second group: useful but secondary
Stay-on-Schedule Edge
Finish the Drive Score
One-Stat Wonder Flag
Third group: later / more advanced
Chaos Threat
Hidden Lean Detector
Bend-Don’t-Break Profile
Complete Team Pressure
Feature Graduation Rule

A feature should not become product logic until it passes these gates:

Clear football hypothesis
Pregame-only inputs
No leakage from final score or postgame validation
SQL prototype shows meaningful lift
Sample size is large enough
Signal repeats across season or bucket
Product role is clear:
stronger language
softer language
confidence cap
volatility warning
No Pick refinement
internal QA only
Frontend wording can be explained without backend terms
Product Language Rule

The frontend should not show feature names like:

two_way_context
feature bucket
language support
formula version
calibration score

Frontend should translate features into football language:

cleaner scoring-suppression read
drive-sustain edge
empty-yards warning
trap-door profile
chaos potential
narrow edge
broader matchup support

The backend can stay technical. The frontend should tell the football story.


My top 3 for you to test next would be:

```text
1. Trap Door Game Flag
2. Drive Killer Index
3. Empty Yards Detector

Those are useful, colorful, and directly connected to the thing your 240-game QA exposed: GameLens needs smarter language control, not just more stats.


| Game ID            | Old QA behavior we remember                                                                                   | Why test it                                                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `20240114_GB@DAL`  | **DAL edge**, High confidence, `Strong Profile / High Outcome Confidence`, but actual was GB 48 - DAL 32      | High-confidence miss. Make sure Level 4 does not make this louder or change outcome logic.                                           |
| `20250113_MIN@LAR` | **MIN edge**, High confidence, `Strong Profile / High Outcome Confidence`, but actual was MIN 9 - LAR 27      | Another high-confidence miss. Good calibration caution case.                                                                         |
| `20231015_SF@CLE`  | **SF edge**, High confidence, `Strong Profile / High Outcome Confidence`, actual CLE 19 - SF 17               | Close high-confidence miss. Good for “don’t overreact to every miss.”                                                                |
| `20230907_DET@KC`  | **No Pick**, Low confidence, `No Clear Edge / Low Outcome Confidence`; Week 1 missing ranking/matchup context | Week 1 / missing-context test. Should not suddenly get unsupported badges.                                                           |
| `20240115_PIT@BUF` | **No Pick**, Low confidence, postseason                                                                       | No Pick should remain No Pick. Good restraint case.                                                                                  |
| `20251208_PHI@LAC` | **PHI edge**, Low confidence, `Thin Edge / Low Outcome Confidence`, caution: `core_area_gap_is_small`         | Low-confidence lean. Make sure language support does not accidentally imply high confidence.                                         |
| `20230917_KC@JAX`  | **JAX edge**, Low confidence, `Strong Profile / Low Outcome Confidence`                                       | Useful weird case: strong profile but low confidence. Great for checking separation between profile strength and outcome confidence. |
| `20240915_SF@MIN`  | **MIN edge**, Low confidence, `Clear Lean / Low Outcome Confidence`                                           | Low-confidence directional lean. Good clean runtime test.                    


[{
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "1st_down_rate",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "not_validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "not_validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "not_validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "not_validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "not_validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "JAX",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.093",
  "validation_result": "validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "KC",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.4349",
  "validation_result": "validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "KC",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "third_down_pct",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.4349",
  "validation_result": "validated"
}, {
  "game_id": "20230917_KC@JAX",
  "claimed_team": "KC",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.4349",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "1st_down_rate",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "points_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "turnover_margin_per_game",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20231015_SF@CLE",
  "claimed_team": "SF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.1915",
  "validation_result": "validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "1st_down_rate",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "1st_down_rate",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240114_GB@DAL",
  "claimed_team": "DAL",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "1st_down_rate",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "1st_down_rate",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "1st_down_rate",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "points_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "BUF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "PIT",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "PIT",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240115_PIT@BUF",
  "claimed_team": "PIT",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "MIN",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "MIN",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "MIN",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "MIN",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "MIN",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "MIN",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "MIN",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "MIN",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "MIN",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "SF",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "1st_down_rate",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "SF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "SF",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "SF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "not_validated"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "SF",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "SF",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "SF",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20240915_SF@MIN",
  "claimed_team": "SF",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "unavailable",
  "two_way_edge_score": null,
  "validation_result": "actual_neutral_or_mixed"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "points_allowed_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "third_down_pct",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20250113_MIN@LAR",
  "claimed_team": "MIN",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "supportive",
  "two_way_edge_score": "0.3412",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "LAC",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "1st_down_rate",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.1826",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "LAC",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "1st_down_rate",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.1826",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "LAC",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "1st_down_rate",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.1826",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "LAC",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.1826",
  "validation_result": "validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "LAC",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "third_down_pct",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.1826",
  "validation_result": "validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "LAC",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.1826",
  "validation_result": "validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_allowed_per_play",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "points_per_play",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "red_zone_efficiency",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "red_zone_efficiency",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "third_down_pct",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "category_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "core_area_summary",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "metric_highlight",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}, {
  "game_id": "20251208_PHI@LAC",
  "claimed_team": "PHI",
  "claim_type": "team_comparison_metric",
  "claim_layer": "supporting",
  "metric": "turnover_margin_per_game",
  "two_way_context": "available_mixed",
  "two_way_edge_score": "-0.0621",
  "validation_result": "not_validated"
}]                                                        |
