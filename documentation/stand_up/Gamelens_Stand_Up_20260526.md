
## Main goal

Tomorrow is not a big-build day.

The goal is to analyze the Admin Calibration + Claim Health dashboard we created and decide what deserves polish, what should be trimmed back, and what should wait.

Because Lovable credits are low, avoid large implementation requests. Use Lovable only for small, targeted fixes after we know exactly what is wrong.

## Current status

The backend and frontend are now talking to each other.

The Admin API is returning the expanded dashboard contract:

- `scope: admin_calibration_claim_health`
- `tabs`
- `default_tab`
- `formula_notes`
- `season_phase_groups`
- `section_metadata`
- populated new sections
- preserved legacy sections

The dashboard now has these tabs:

- Overview
- Game Calibration
- Core Area Alignment
- Pillar Health
- Feature Health
- Technical Debug

The Calibration Over Time chart is now rendering live data.

Lovable also fixed:
- chart spelling from “Calibrtion” to “Calibration”
- overlapping Overall Claim Validation / Selected Segment Validation behavior

## Tomorrow’s mindset

Do not ask: “What else can we add?”

Ask:

1. Does this page help me understand GameLens faster?
2. Which sections actually answer useful questions?
3. Which sections are noisy, redundant, or too technical?
4. Which pieces should be admin headline sections?
5. Which pieces should be hidden, collapsed, or moved lower?
6. What is the smallest next fix that improves clarity?

## Priority order

### 1. Visual scan first

Open the Admin page and go tab by tab.

For each tab, write one sentence:

- What is this tab trying to tell me?
- Is it obvious within 5 seconds?
- Is anything confusing, too dense, or visually broken?

Tabs to review:

- Overview
- Game Calibration
- Core Area Alignment
- Pillar Health
- Feature Health
- Technical Debug

Do not fix anything yet. Just observe.

## Tab-by-tab review notes

### Overview

Purpose:

This should answer:

“Is GameLens getting healthier over time?”

Check:

- Does the page open to Overview by default?
- Are coverage and baseline cards clear?
- Does the line chart explain enough?
- Are claim validation and game pick accuracy visually distinct?
- Does Week 1 missing data make sense?
- Is the page too crowded or just right?

Possible decisions:

- Keep as-is
- Add a short note under the chart
- Move some cards lower
- Add clearer labels for claim validation vs pick accuracy

### Game Calibration

Purpose:

This should answer:

“When GameLens gave a game-level read, did it align with the final result?”

Check:

- Can I quickly compare Low / Medium / High confidence?
- Are No Pick games clearly separated?
- Does “No Clear Edge” avoid looking like a failure?
- Are close misses and severe misses visible enough?

Important interpretation:

No Pick should not be treated as wrong. If a row has only no-picks, the UI should say that plainly.

Possible decisions:

- Keep matrix
- Add tooltip / explanation
- Make No Pick visually clearer
- Add small summary cards later

### Core Area Alignment

Purpose:

This should answer:

“When Matchup Lean said one thing, did Core Areas confirm it, split, or push back?”

Check:

- Is `confirmed_edge` easy to understand?
- Is `split_profile` easy to understand?
- Is `conflicting_profile` easy to understand?
- Are `avg_core_gap` and `avg_signal_gap` useful or too technical?
- Does this tab feel important enough?

Possible decisions:

- Add plain-English labels later
  - confirmed_edge → Confirmed Edge
  - split_profile → Split Profile
  - conflicting_profile → Conflicting Profile
  - coin_flip_profile → Coin-Flip Profile
  - no_clear_edge → No Clear Edge
- Keep raw codes for now if credits are low

### Pillar Health

Purpose:

This should answer:

“Which football areas are producing truthful claims?”

Check:

- Are Core Area and Category grouped clearly?
- Can I tell which areas are strongest?
- Can I tell which areas are weak?
- Does `missing_core_area` create confusion?
- Is the weekly table useful or overwhelming?

Possible decisions:

- Keep Pillar Health Matrix as primary
- Collapse Weekly Health by default if too dense
- Rename missing hierarchy labels later
- Add sorting later

### Feature Health

Purpose:

This should answer:

“Which engineered features are earning trust?”

Check:

- Are Football Calibration Features visually separated from Data Quality / Metadata Features?
- Is `offensive_efficiency_support_v1` understandable?
- Is `two_way_context` understandable?
- Are `offense_finish_score` and `defensive_suppression_score` too technical without helper text?
- Are low-sample warnings visible?

Possible decisions:

- Keep grouped table
- Add descriptions per feature family later
- Collapse legacy feature scorecard by default
- Move technical feature details lower

### Technical Debug

Purpose:

This should be useful for QA, not the headline dashboard.

Check:

- Is Surface Matrix only here?
- Does it feel clearly lower priority?
- Is it still useful for regression/debugging?

Possible decisions:

- Keep as-is
- Rename visible title to “Technical Claim Surface Debug”
- Collapse by default later

## Data interpretation checklist

Look for these patterns tomorrow.

### Game-level calibration

Questions:

- Is Medium Confidence meaningfully better than Low?
- Is High Confidence actually strong enough?
- Are Strong Profile games behaving better than Clear Lean games?
- Are No Clear Edge games mostly no-picks?
- Are severe misses concentrated in one confidence/profile bucket?

### Claim health

Questions:

- Which Core Area has the best validation rate?
- Which Core Area has the weakest validation rate?
- Are some categories surprisingly strong?
- Are some categories repeatedly weak?
- Is neutral/mixed high in specific areas?

### Feature health

Questions:

- Which feature buckets have positive lift?
- Which feature buckets have negative lift?
- Are low-sample buckets being over-interpreted?
- Are metadata features being visually separated from football features?
- Does offensive efficiency still look useful?
- Does two_way_context still deserve attention?

## What not to do tomorrow

Do not spend Lovable credits on:

- New backend sections
- Game drilldown
- Major redesign
- New filters
- New chart libraries
- New colors/fonts
- Sidebar/header changes
- Big refactors

Avoid asking Lovable to “make it better” generally.

Only ask for small fixes like:

- Rename this title
- Collapse this section
- Fix this label
- Improve this tooltip
- Hide duplicate line
- Show null as —
- Move this section lower
- Add one explanatory sentence

## Small Lovable prompts if needed

### Prompt 1 — tiny label cleanup

Use this if raw labels are confusing.

```text
Please make a tiny frontend-only label cleanup pass on the Admin Calibration dashboard.

Do not change backend, routing, auth, layout, fonts, colors, or data contracts.

Only add display-label helpers for known raw values:
confirmed_edge -> Confirmed Edge
split_profile -> Split Profile
conflicting_profile -> Conflicting Profile
coin_flip_profile -> Coin-Flip Profile
no_clear_edge -> No Clear Edge
missing_core_area -> Missing Core Area
missing_category -> Missing Category

Keep raw values available in tooltips if useful.
Build must pass.
Prompt 2 — collapse noisy legacy sections

Use this if the page feels too crowded.

Please make a small frontend-only polish pass.

Do not change backend, routing, auth, fonts, colors, or API fields.

Collapse older compatibility sections by default:
- Category Health
- Offensive Efficiency Feature Scorecard
- legacy Core Area / Confidence matrices if they appear below the newer matrix

Keep them accessible, but visually lower priority.

Do not remove sections.
Build must pass.
Prompt 3 — improve no-pick clarity

Use this if Game Calibration makes no-pick rows look bad.

Please improve no-pick clarity in the Game-Level Calibration matrix only.

Do not change backend or API fields.

If correct_count + incorrect_count === 0 and no_pick_count > 0:
- Display “No directional picks”
- Do not show 0% correct as the main value
- Keep no_pick_rate visible

No layout redesign. Build must pass.
Suggested tomorrow workflow
Open dashboard.
Take screenshots of each tab.
Write quick notes:
keep
confusing
trim/collapse
needs tiny fix
Do not touch code yet.
Pick one tiny Lovable fix only if needed.
Save observations for later backend/product work.
Success criteria for tomorrow

Tomorrow is successful if I can answer:

Which tab is most useful?
Which tab is least useful?
Which section should be the dashboard headline?
Which section should be collapsed or demoted?
Which labels need plain-English cleanup?
Which feature signals deserve more analysis later?
Whether the current dashboard is good enough to keep as the admin foundation
Likely next product direction

The dashboard should become less about “show every table” and more about a clear admin story:

Overall health over time
Game-level calibration
Core Area alignment
Pillar claim truth
Feature trust
Technical debug

That order still feels right.

Tomorrow should focus on whether the page actually communicates that story.


My honest advice for tomorrow: **do screenshots + notes first, Lovable second**. With low credits, your best move is to become the product reviewer, not the builder. 🧠

####Proposed Plan After Auditing and Analyzing the Admin Tab

GameLens Admin Calibration Review — Next-Step Plan
Main framing

The Admin Calibration + Claim Health dashboard is not production-ready yet, but it is already useful as a model review and production-monitoring prototype.

The goal is not to polish every visual. The goal is to learn what the 2025 model run is telling us:

Why is Game Pick Accuracy higher than Overall Claim Validation?
What makes Medium Confidence perform better than High Confidence in several places?
What claim types, pillars, and features are helping or hurting validation?
Which changes would improve claim truth without overfitting to one season?
What would this dashboard eventually monitor in production?

Important overfitting guardrail:

Treat 2025 results as calibration hypotheses, not permanent rules. Only change behavior when the data pattern also makes football/product sense.

1. Separate Game Pick Accuracy from Claim Validation
Key idea

Game Pick Accuracy and Overall Claim Validation are not the same thing.

Game Pick Accuracy asks:

Did GameLens’ directional game read match the final winner?

Overall Claim Validation asks:

Did GameLens’ individual football claims hold up against postgame evidence?

A game can be picked correctly while several supporting claims are wrong or mixed.

Example:

GameLens may correctly lean Team A, but its supporting claims about pressure, turnovers, red-zone finish, or scoring production may not validate cleanly.

Action items
Keep both metrics visible.
Do not judge the system only by winner accuracy.
Use Game Pick Accuracy for outcome calibration.
Use Claim Validation for explanation quality.
Improve claim validation by controlling which claims get stronger wording.
Do not improve claim validation by making GameLens silent or overly cautious.
Dashboard note to add later

Claim validation measures whether GameLens’ football explanations were supported by postgame data. Game pick accuracy measures whether directional game picks matched the final winner. These can diverge.

2. Learn what Medium Confidence is actually finding

This should be one of the next major analysis steps.

Medium Confidence appears to be the healthiest practical label right now, especially when paired with Strong Profile or Confirmed Edge.

Main question

What does Medium Confidence know that High Confidence is over-promising?

Analyze Medium Confidence by:
A. Profile strength

Compare:

No Clear Edge
Thin Edge
Mixed Profile
Clear Lean
Strong Profile

For each:

game count
correct %
no-pick rate
avg margin
severe miss rate
close miss rate
claim validation rate
neutral/mixed claim rate
B. Core Area profile type

Compare Medium Confidence across:

Confirmed Edge
Split Profile
Conflicting Profile
Coin Flip Profile
No Clear Edge

Questions:

Is Medium mostly living in Confirmed Edge?
Does Medium avoid Split/Conflicting profiles?
Are Medium Confidence picks supported by cleaner Core Area alignment?
Does Medium have less volatile pillar exposure?
C. Feature buckets

For Medium Confidence games, check exposure to:

Offensive Efficiency Support V1
Two Way Context
Defensive Suppression Score
Offense Finish Score
Clean Hierarchy Context

Questions:

Are Medium Confidence successful games more likely to have Repeat Positive Strong?
Are Medium Confidence successful games more likely to have Two Way Context = Supportive?
Are Medium misses more likely to show Available Mixed, Caution Only, or Opposing Efficiency Signal?
D. Pillars and categories

For Medium Confidence claims, inspect:

Offensive Output
Defensive Control
Scoring Efficiency
Disruption and Turnovers
Pressure
Turnovers
Turnover Risk
Passing Game
Scoring Suppression

Question:

Is Medium Confidence gravitating toward healthier pillars while High Confidence is accidentally allowing noisy pillars to speak too loudly?

3. Recalibrate High Confidence

Current pattern:

Medium Confidence × Strong Profile outperformed High Confidence × Strong Profile.

Core Area Alignment showed the same general warning:

Confirmed Edge × Medium Confidence outperformed Confirmed Edge × High Confidence.

Interpretation

High Confidence may be too easy to earn.

It may currently mean:

“The model sees a strong profile.”

But it should probably mean:

“The model sees a strong profile, the explanation quality is supported, and volatile signals are not driving the read.”

Next audit

Pull all High Confidence games and tag each miss:

Miss Type	Meaning
Close miss	Not a severe model failure
Severe miss	Real confidence problem
Turnover swing	Volatile event overwhelmed profile
Explosive offense miss	Model underrated upside
Rushing/control miss	Known possible blind spot
Late-season form miss	Recent form may need more weight
Weak claim support	Pick was strong but explanation was not
Noisy pillar driven	Pressure/turnovers drove too much confidence
Proposed future rule

High Confidence should require:

Strong Profile or Confirmed Edge
Clean Core Area support
Healthy feature support
No major volatile-pillar warning
No major opposing efficiency warning
Sufficient sample/data coverage

If those are not met, cap at Medium.

4. Improve Profile Strength × Confidence intersection
Current learning

The profile labels are useful, but confidence labels need stricter promotion rules.

Proposed interpretation
Profile / Confidence	Product meaning
No Clear Edge / Low	Model correctly avoided forcing a read
Thin Edge / Low	Lean exists, but should be soft
Mixed Profile / Low	Risky; consider more no-pick behavior
Clear Lean / Medium	Useful read, but not overwhelming
Strong Profile / Medium	Current best-performing practical bucket
Strong Profile / High	Needs stricter gating
Action items
Do not eliminate Medium Confidence.
Treat Medium as the default “usable read.”
Make High Confidence rarer.
Add reason codes when High Confidence is downgraded to Medium.
Track whether downgraded High-to-Medium games perform better over future runs.

Possible downgrade reasons:

feature_support_mixed
volatile_pillar_driver
core_area_gap_not_large_enough
opposing_efficiency_signal
claim_validation_support_missing
late_season_form_warning
small_sample_context
5. Core Area Alignment next steps
Confirmed Edge

Confirmed Edge is useful, but not enough by itself to justify High Confidence.

Action:

Confirmed Edge should support confidence, but High Confidence should require feature confirmation too.

Split Profile

Split Profile performed weakly.

Action:

Split Profile should cap confidence and use caution language.

Suggested copy:

The lean exists, but the matchup is split across key football areas.

Conflicting Profile

Conflicting Profile should be treated as a stronger caution.

Action:

Conflicting Profile should usually cap at Low or cautious Medium.

Suggested copy:

The model found a lean, but broader Core Area context pushes back.

Coin Flip Profile

Coin Flip Profile was more interesting than expected.

Action:

Do not automatically discard Coin Flip Profiles. Treat them as thin leans, not strong reads.

Suggested copy:

The overall matchup is close, but one or two signals create a slight lean.

No Clear Edge

This is behaving well.

Action:

Protect No Pick behavior. Do not force directional picks just to increase coverage.

6. Pillar Health improvements
Main learning

Healthiest areas:

Offensive Output
Passing Game
Defensive Control
Scoring Suppression

Weakest / caution areas:

Pressure
Turnover Risk
Turnovers
Disruption and Turnovers
Scoring Production
Action items
A. Fix missing hierarchy metadata

Rows like:

missing_core_area × Pressure
missing_core_area × Turnover Risk
missing_category

should be reviewed.

The first goal is not to “make the number better.”
The first goal is to ensure claims are being grouped correctly.

Potential mapping review:

Current	Possible destination
missing_core_area / Pressure	Disruption and Turnovers / Pressure
missing_core_area / Turnover Risk	Disruption and Turnovers / Turnover Risk
missing_core_area / Scoring Efficiency	Scoring Efficiency / proper category
missing_category rows	Recover category from metric registry when possible
B. Soften volatile pillar language

Pressure and turnovers should not speak loudly by themselves.

Possible rule:

Pressure, Turnover Risk, and Turnovers require confirming support before receiving strong language.

Confirming support could include:

Defensive Control support
Two Way Context supportive
Defensive Suppression strong positive
Opponent offensive weakness
Offensive Efficiency opposition warning
C. Treat weak pillars as context, not proof

These areas can still be useful, but they should usually say:

“could matter”
“adds volatility”
“creates risk”
“leans toward”
“is a caution signal”

Instead of:

“clear edge”
“strong advantage”
“should drive the matchup”
7. Pillar Weekly Health next steps

Use this section as a production-monitoring tool, not a tuning engine yet.

What to monitor
Claim validation by week
Game pick accuracy by week
No-pick rate by week
Missing metadata by week
Weakest core area by week
Avg confidence drift
Week 1 / early-season data gaps
Future production alerts

Create alert-style checks later:

Pattern	Possible concern
Claim validation drops below baseline for 2+ weeks	Language/model drift
No-pick rate spikes	Data missing or model too cautious
No-pick rate collapses	Model may be forcing reads
High Confidence underperforms Medium	Confidence rules too loose
Disruption/Turnovers repeatedly weakest	Volatile pillar over-speaking
Missing metadata increases	Pipeline or hierarchy issue

For now:

Keep Weekly Health, but do not overreact to one week.

8. Feature Health next steps

Feature Health is the clearest path to improving claim validation.

A. Offensive Efficiency Support V1

This is the strongest current feature.

Main learning:

Repeat Positive Strong is validating very well and has strong lift vs baseline.

Action:

Use Offensive Efficiency Support V1 as a claim-language gate.

Bucket	Action
Repeat Positive Strong	Eligible for stronger language
Repeat Positive Supportive	Measured support language
Not Relevant	Normal/default language
Context Only	Soft/context language
Mixed / Near Even	Soften
Caution Only	Do not boost
Negative Caution	Add caution
Opposing Efficiency Signal	Suppress or warn

This should help raise Claim Validation by making the strongest claims come from the strongest feature buckets.

B. Two Way Context

Main learning:

Supportive Two Way Context validates better than mixed or unavailable.

Action:

Use Two Way Context as a second language gate.

Bucket	Action
Supportive	Can support stronger claim language
Available Mixed	Soften
Unavailable	Neutral/default

Important:

Available Mixed should not be treated as harmless. It appears weaker than unavailable.

C. Defensive Suppression Score

Main learning:

Strong Positive is useful. Regular Positive is not.

Action:

Only allow Defensive Suppression to support stronger language when:

bucket = Strong Positive

Do not boost claims for regular Positive.

D. Offense Finish Score

Main learning:

Offense Finish is not strong enough as a standalone feature.

Action:

Keep it as an ingredient in Two Way Context.
Do not let it drive product language by itself yet.

E. Clean Hierarchy Context

Main learning:

This is mostly data-quality metadata, not football performance.

Action:

Move mentally into “Data Quality / Metadata Features.”

Use it to:

find missing mapping
clean hierarchy
improve category grouping
make future dashboards more reliable

Do not use it as a football feature.

9. Technical Debug low-hanging fruit

No deep dive yet, but current obvious learning:

Game Profile headline claims are below baseline.

Action items
Treat Game Profile as context, not proof.
Avoid strong validation language in Game Profile.
Keep Core Area Comparison headline claims prominent.
Gate Metric Highlight claims with feature support.
Make supporting metric highlights softer unless supported by strong feature buckets.

Potential future surface rules:

Surface	Action
core_area_comparison / headline	Keep prominent
metric_highlight / headline	Keep, but feature-gated
core_area_summary / supporting	Normal support
category_summary / supporting	Normal/soft support
team_comparison_metric / supporting	Avoid overclaiming near-even rows
metric_highlight / supporting	Require stronger support
game_profile / headline	Demote to context language
10. Recommended work order
Phase 1 — Analysis first

Do not spend Lovable credits here.

Create query/export views for:

Medium Confidence games
High Confidence games
Strong Profile × Medium Confidence
Strong Profile × High Confidence
Confirmed Edge × Medium Confidence
Confirmed Edge × High Confidence
High Confidence misses
Medium Confidence hits
Feature bucket distribution by confidence
Claim surface distribution by confidence

Goal:

Learn what Medium Confidence is selecting that High Confidence is not respecting.

Phase 2 — High Confidence miss audit

For each High Confidence miss, classify:

close miss
severe miss
noisy turnover game
pressure/disruption overstatement
opposing efficiency warning ignored
recent-form issue
rushing/control issue
explosive offense issue
weak claim validation support
data/metadata issue

Output:

A small table of miss reasons, not a giant rewrite.

Phase 3 — Claim-language gates

Implement backend language discipline before changing pick logic.

Suggested gate:

Stronger claim language requires strong feature support or clean two-way support.

Initial promotion candidates:

Offensive Efficiency Repeat Positive Strong
Offensive Efficiency Repeat Positive Supportive
Two Way Context Supportive
Defensive Suppression Strong Positive

Initial suppression/caution candidates:

Opposing Efficiency Signal
Caution Only
Available Mixed
Pressure without support
Turnover Risk without support
Game Profile headline claims
Phase 4 — Confidence recalibration

After the claim-language gates are understood, update confidence rules.

Potential rule:

High Confidence requires Strong Profile or Confirmed Edge plus trusted feature support.

Potential caps:

Condition	Cap
Split Profile	Low/Medium
Conflicting Profile	Low
Coin Flip Profile	Low/Medium
Strong Profile but weak feature support	Medium
Volatile pillar driver	Medium
Opposing Efficiency Signal	Medium or Low
Missing hierarchy/data quality issue	Medium
Phase 5 — Metadata cleanup

Clean hierarchy issues:

missing_core_area
missing_category
Pressure mapping
Turnover Risk mapping
Game Profile surface mapping
category recovery from registry

Goal:

Make the dashboard’s football groupings cleaner before trusting future comparisons.

Phase 6 — Dashboard polish later

Only after analysis and backend choices are clearer.

Small future polish candidates:

Add Claim Validation vs Game Pick Accuracy explanation
Add denominator note to Game Pick Accuracy
Rename missing metadata labels
Collapse dense weekly tables
Reorder Feature Health sections
Rename technical codes to readable labels
Add sample-size tooltips
11. Production vision

Eventually this Admin page should answer:

Is GameLens coverage healthy?
Is claim validation improving or degrading?
Is game pick accuracy calibrated by confidence?
Is High Confidence actually high quality?
Which pillars are trustworthy?
Which features are earning stronger language?
Which surfaces are over-speaking?
Are missing metadata/data-quality issues increasing?
Did a new model version improve or regress?
Are no-picks being used appropriately?
Current best conclusion

The 2025 run suggests GameLens is promising, but needs language and confidence discipline.

The main lessons:

Game Pick Accuracy can be higher than Claim Validation because picking the winner is easier than validating every supporting football claim.
Medium Confidence currently appears more trustworthy than High Confidence in key intersections.
High Confidence should become rarer and require feature-backed support.
Confirmed Edge matters, but should not automatically promote to High.
Split and Conflicting Profiles should cap confidence.
Offensive Output, Passing Game, Defensive Control, and Scoring Suppression are healthier claim areas.
Pressure, Turnovers, and Turnover Risk need softer language.
Offensive Efficiency Support V1 is the strongest feature-health win.
Two Way Context should remain central.
Defensive Suppression should only speak when Strong Positive.
Offense Finish should stay an ingredient, not a standalone driver.
Clean Hierarchy is a data-quality tool, not a football feature.
Game Profile should be contextual, not proof-heavy.
Best next move

Study what Medium Confidence is finding, then use that to make High Confidence stricter instead of louder.

### running the SQL

#findings
Yes — I can understand the outputs. Nothing looks unusable. A few things do jump out, and they are helpful, not bad. 🕵️‍♂️

Quick sanity check
1. One High Confidence game is unaccounted for

In your profile-strength result:

High / Strong Profile / confirmed_edge = 22 games
correct = 12
incorrect = 9
no-pick = 0

That totals 21, not 22.

The pick accuracy of 57.1% equals 12 / (12 + 9), so the missing game is probably something like Tie, Push, No Decision, or a differently formatted model_result.

Not a huge issue, but we should identify it later.

2. Feature bucket results are claim-level, not exclusive game buckets

In the Feature query, the same game can appear in multiple rows because a game has many claim rows and many feature buckets. So don’t add the games counts together.

That is expected. The schema confirms this table is claim-row based, with game fields, claim fields, validation fields, and feature fields all living together.

The big learning so far
Medium is not magically better because it is weaker

This is the important part:

Medium / Strong Profile / confirmed_edge is not “less confident junk.”

It has:

21 games
81.0% pick accuracy
66.3% avg claim validation
68.4% headline claim validation

High / Strong Profile / confirmed_edge has:

22 games
57.1% pick accuracy
53.5% avg claim validation
54.7% headline claim validation

So Medium Strong Profile is beating High on both:

winner accuracy
explanation truth

That means High Confidence is not just losing because football is random. It is also carrying weaker claim validation.

What Medium seems to be finding

The Feature bucket output is the strongest clue.

Medium / Strong Profile / confirmed_edge with:

two_way_context = supportive
offensive_efficiency_support_bucket = repeat_positive_strong

had:

10 games
83.8% claim validation
0.0% neutral/mixed

That is very clean.

Medium / Clear Lean / confirmed_edge with the same bucket had:

11 games
82.7% claim validation
1.0% neutral/mixed

Also very clean.

Translation

Medium seems healthiest when it finds:

confirmed edge + supportive two-way context + repeat-positive offensive efficiency

That should become your “gold profile” for claim language.

Not necessarily automatic High Confidence yet, but definitely:

“This is the kind of profile High should be looking for.”

What High is doing wrong

High has the same general profile label:

Strong Profile / confirmed_edge

But its misses are ugly on claim validation.

High Confidence incorrect games include:

BUF over MIA miss: 0.0% claim validation, severe miss
PIT over CLE miss: 25.0% claim validation
TB over NO miss: 18.5% claim validation
LAR over ATL miss: 3.1% claim validation
LAC over NYG miss: 12.5% claim validation
GB over CAR miss: 31.3% claim validation

These are not just “wrong winner, but good reasoning” cases. Several are wrong winner + bad explanation validation.

That is the strongest argument for recalibrating High.

The pattern I see
High Confidence is over-trusting signal strength

Look at the High misses:

core gaps are often large
signal gaps are often 8–11
team comparison edge score is often 1.0

So the system saw strong separation.

But the claims often failed postgame.

That suggests:

High Confidence is probably being promoted too much by signal/core/team-comparison strength, and not enough by claim-quality features.

In plain English:

High is saying “the matchup shape is strong,” but not always asking “are the actual claims trustworthy?”

Medium wins show the blueprint

Medium correct games often have strong claim validation and sometimes huge final margins. Several Medium winners had claim validation above 85–95%.

That tells me Medium is often finding real football mismatches, but because the signal gap is not quite high enough, it stays Medium.

So the fix may not be “High needs bigger signal gap.”

It may be:

High needs better feature support, not just bigger signal gap.

Pillar/category learning

Your pillar/category result is also useful.

Healthier claim areas

Medium and High both validate better in:

Offensive Output
Passing Game
Defensive Control
Scoring Suppression
Offensive Rhythm

For example, Medium / Offensive Output / missing_category shows 67.2%, and Medium / Passing Game shows 62.5%.

Caution areas

The weak spots remain:

Pressure
Turnover Risk
Turnovers
Drive Conversion
Scoring Production

Medium / missing_core_area / Pressure is especially weak:

35.6% validation
73 games

That is not a small-sample shrug. That deserves action.

My updated recommendation
High Confidence should require claim-quality gates

Do not promote to High from signal gap alone.

A future High rule should require something like:

Strong Profile or Confirmed Edge
AND signal/core/team comparison support
AND at least one trusted claim-quality feature:
  - two_way_context = supportive
  - offensive_efficiency_support = repeat_positive_strong
  - defensive_suppression = strong_positive
AND no major caution bucket:
  - opposing_efficiency_signal
  - available_mixed + caution_only
  - pressure/turnover driven without support
Medium should become the “trusted default”

Medium is currently behaving like:

“Usable matchup read with healthier restraint.”

That is a good product label.

High should become:

“Rare, feature-confirmed, explanation-backed read.”

### commit 1 after fixing admin health queries

####
Notes for today’s markdown
# Admin Calibration Dashboard — Graded Game Count Fix

## What changed

Updated `admin_claim_health_queries.py` to make game-pick calibration accounting clearer and more production-safe.

The issue found during review was that tied / no-decision games were included in total game counts but were not counted as correct, incorrect, or no-pick. This created confusing dashboard cells like:

- 22 games
- 12 correct
- 9 incorrect
- 0 no-pick

The missing game was later identified as a tie / no-decision game.

## Why this matters

Game Pick Accuracy should only grade games where GameLens made a directional pick and the game had a clear winner.

So the correct denominator is:

```text
graded_games = correct_games + incorrect_games

No-picks and no-decision/tie games should remain visible, but they should not be included in graded pick accuracy.

Functions updated

Updated game-result normalization and/or aggregation logic in:

get_game_level_calibration
get_core_area_alignment_matrix
get_calibration_over_time
get_pillar_weekly_health
Specific improvements

Added model result normalization using:

LOWER(TRIM(CAST(... AS STRING)))

Added or exposed fields such as:

graded_game_count / graded_games
no_decision_count / no_decision_games
correct_count / correct_picks
incorrect_count / incorrect_picks
no_pick_count / no_pick_games

Updated game-pick accuracy calculations to use graded games only:

correct / (correct + incorrect)

instead of total games.

Expected behavior

For a bucket like High Confidence / Strong Profile / Confirmed Edge:

22 total games
21 graded games
12 correct
9 incorrect
1 no decision
57.1% graded pick accuracy

This makes the dashboard more honest and easier to interpret.

Important boundary

This change does not alter:

model predictions
claim validation logic
matchup lean logic
confidence assignment
frontend layout
training data generation

It is an admin reporting / aggregation correctness fix only.

Follow-up note

One compatibility check remains useful: make sure any frontend code that previously expected games_with_pick still receives it, or has been updated to use graded_games.

Why this was a good fix

This supports the larger goal of turning the Admin dashboard into a reliable production-monitoring tool. It keeps total coverage, no-pick behavior, and tied/no-decision games visible without polluting graded pick accuracy.


I’d commit this as a **small correctness fix**, not a feature. Nice clean cleanup. 🧹

###Learning from Admin Dashboard High vs Medium issue:

My current answer

High Confidence should require four kinds of extra evidence:

1. Matchup shape
Strong Profile or Confirmed Edge
Meaningful signal gap
Meaningful core gap
Team Comparison not merely near-even

This is what High already mostly has.

2. Stable football support
Offensive Output support
Passing Game or Rushing Game support
Defensive Control support
Scoring Suppression support

This is the “real football pillar” layer.

3. Claim-quality support
Core Area Comparison headline claims validate historically well
Metric Highlight headline claims are from trusted metrics
Game Profile claims are not the main reason
Supporting claims are not dominated by volatile categories

This is what we are missing.

4. No unresolved warning path
No opposing efficiency signal
No negative caution
No volatile pressure/turnover dependency without stabilizer
No available_mixed two-way context dominating the read
No weak-surface dependency

This is the “don’t get cute” layer. 😄

##
High should require 4 layers
1. Shape strength

This is what High already has.

Strong Profile
confirmed_edge
large signal gap
large core gap
strong team comparison edge

But this alone is not enough.

2. Expected claim quality

This is the missing piece.

High should require something like:

expected_claim_validation_rate >= 60–65%

calculated from pregame-known features, such as:

claim type
claim layer
core area
category
metric
two_way_context
offensive_efficiency_support_bucket
claim_strength bucket
clean hierarchy status

This should be based on historical validation rates, not postgame validation from the same game.

3. Fragility screen

High should be capped if there are unresolved warning paths:

opposing_efficiency_signal present
negative_caution present
available_mixed dominating supportive
volatile category load too high
pressure/turnover claims acting as primary support
game_profile headline acting too loudly

Your Step 2 miss list showed every High miss had caution-only and volatile-category warnings, and several had weak claim validation or opposing/negative caution flags.

4. Outcome durability

High Correct games had much bigger margins:

High Correct avg margin: 18.3
High Miss avg margin: 4.8

That means High should not just identify a likely winner. It should identify a read likely to survive normal game noise.

Possible future durability features:

recent form
injury-adjusted opponent path
divisional/rivalry flag
explosive offense warning
rushing/control stability
late-season/week-17/week-18 context

Not today, but this is where the model probably grows.

##

follow up on High confidence rules:
My updated High Confidence framework
High should be rare and pass these gates
Gate 1 — Shape gate
profile_strength = Strong Profile
profile_type = confirmed_edge
signal_gap >= 7
core_gap >= 0.25

This is your current “the matchup looks strong” gate.

Gate 2 — Caution-heavy cap
clean_mix_bucket != caution_heavy_support

This is the first real improvement from today.

Gate 3 — Expected claim quality

This is the important future one:

expected_claim_validation_rate >= 60%

based on pregame claim signatures.

Example inputs:

claim_type
claim_layer
registry_core_area
registry_category
metric
two_way_context
offensive_efficiency_support_bucket
claim_strength_bucket
clean_hierarchy_status

This is the thing that can say:

“This profile looks strong, but these exact claim types usually do not validate well.”

Gate 4 — opponent path warning

Cap High if:

opposing_efficiency_signal > 0
OR negative_caution > 0
OR volatile categories are primary support
OR pressure/turnover signals carry too much of the story
Gate 5 — production later

Eventually add:

injury context
late-season / Week 18 flag
divisional/rivalry flag
recent-form shift
explosive-offense warning
rushing/control stability

Not today. But that’s where High probably gets smarter.

###

Updated answer: what should High require?

High should require more than “strong profile” and more than “clean-looking support.”

High should require this:
Strong matchup shape
+ not caution-heavy
+ high expected claim quality
+ no obvious opponent path
+ severity-aware confidence

The missing piece is:

expected claim quality

Not actual claim validation. We do not know that pregame.

But we can estimate it from historical pregame claim signatures.

What I would change conceptually
1. Cap caution-heavy High

This is the easiest rule.

If clean_mix_bucket = caution_heavy_support:
    High Confidence is not allowed

Use wording like:

Strong profile, but confidence kept measured due to volatile support mix.

2. Do not promote based on clean_support alone

This failed.

clean_support ≠ High Confidence

Clean support can stay as a supporting signal, but it cannot be the deciding factor.

3. Add a reasoning-quality tier

For High misses, we need to know whether the model was:

wrong but reasonable
wrong and unsupported

Possible buckets:

Bucket	Meaning
Clean outcome miss	wrong winner, claim validation decent
Bad reasoning miss	wrong winner, claim validation weak
Severe miss	wrong winner by 17+
Close variance miss	wrong winner by 1–3
No decision	tie/push/no graded outcome

This would make the Admin page way smarter.

4. Build expected_claim_quality_v0

This is the next real backend learning feature.

For each pregame claim row, estimate its expected validation from historical rows with the same signature:

claim_type
claim_layer
registry_core_area
registry_category
metric
two_way_context
offensive_efficiency_support_bucket
claim_strength_bucket
claim_strength_language_signal

Then roll that up to the game level:

game_expected_claim_quality = weighted average of expected claim quality

Headline claims should probably weigh more than supporting claims.

Then High requires something like:

expected_claim_quality >= 60% or 65%

This is better than counting “stable” and “caution” claims manually.

My current practical High Confidence rule

If I were writing the draft rule today, it would be:

High Confidence Candidate =
  Strong Profile
  AND confirmed_edge
  AND signal/core separation present
  AND clean_mix_bucket != caution_heavy_support
  AND expected_claim_quality_v0 >= threshold
  AND no major opponent-path warning

If it fails those final checks:

Downgrade to Medium Confidence

Not because the matchup is bad, but because the read is not durable enough to shout.

# Admin Dashboard Learning — High vs Medium Confidence

## Main direction

The Admin dashboard should be used as a model-learning and calibration tool, not as a source for hard-coded one-season rules.

The goal is not to build a homemade decision tree or manually gated random forest. The goal is to learn where GameLens is calibrated, where it overstates, and what kinds of failures repeat.

## Core learning

High Confidence currently appears too driven by strong matchup separation. It is not consistently better than Medium Confidence.

Medium Confidence, especially Medium + Strong Profile + Confirmed Edge, appears to be a healthier production default in the 2025 run.

This suggests High Confidence should become rarer and should require durability evidence, not just stronger signal/core/team-comparison separation.

## Claim Validation vs Game Pick Accuracy

Game Pick Accuracy measures whether the model picked the winner.

Claim Validation measures whether the model’s football explanation held up.

These can diverge. A model can pick the winner but explain it weakly, or miss the winner while still making mostly valid football claims.

This means future model review should separate:
- correct outcome + strong claims
- correct outcome + weak claims
- wrong outcome + strong claims
- wrong outcome + weak claims
- severe miss + weak claims

This is more useful than simple win/loss grading.

## What the High vs Medium work taught us

High misses were often not missing obvious support. Many still had strong-looking signal gap, core gap, team comparison edge, and supportive two-way context.

That means the model does not simply need “more strength” before assigning High.

It needs better evidence that the strength is durable.

## Important warning

The clean/mixed/caution-heavy support experiment was useful but too blunt.

It showed that caution-heavy profiles are risky, but it did not reliably separate all good High reads from bad High reads.

Therefore:
- Do not turn clean_mix_bucket into a hard rule yet.
- Do not promote High just because support looks clean.
- Do not cap everything from one 2025 pattern.
- Treat clean/caution mix as a diagnostic signal only.

## Strongest hypothesis from the Admin tab

High Confidence should eventually require an expected claim-quality layer.

This means estimating, before the game, whether the specific claims supporting a read come from historically reliable claim signatures.

Possible inputs:
- claim_type
- claim_layer
- core_area
- category
- metric
- two_way_context
- offensive_efficiency_support_bucket
- claim_strength_bucket
- clean_hierarchy_status

This should be tested as an offline calibration feature first, not immediately wired into production confidence.

## Model improvement themes

### 1. Confidence calibration

High should mean durable confidence, not just strong signal separation.

Medium may remain the default “usable strong read.”

High should become rarer and better justified.

### 2. Claim selection

The model may be producing too similar a claim recipe across strong-profile games.

Improve the model by making claim selection more game-specific and more selective.

### 3. Explanation quality

Track whether a game was:
- wrong but reasonable
- wrong and unsupported
- correct but weakly explained
- correct and strongly explained

This helps the model learn from misses without overreacting to normal football variance.

### 4. Volatility awareness

Pressure, turnovers, turnover risk, disruption, and scoring-production claims should be monitored carefully.

They are useful context, but they should not automatically help promote a read to High.

### 5. Expected claim quality

The next serious improvement is not another hand-built gate.

The next serious improvement is an expected claim-quality score, ideally tested with holdout or leave-one-game-out logic.

## Guardrail against overfitting

Only promote a change if it passes three tests:

1. It appears in the Admin data.
2. It makes football/product sense.
3. It survives validation beyond the exact slice where it was discovered.

Until then, keep it as a diagnostic warning, not a production rule.

## Best current conclusion

The Admin tab tells us GameLens is promising, but High Confidence is not calibrated enough yet.

The next model improvement should focus on making High Confidence more durable and explanation-backed, while preserving Medium Confidence as the safer useful read.

#### Next Work: Expected Claim Quality Calibration

Goal:
Build an offline calibration experiment that estimates how trustworthy a game’s explanation looked before the game.

Why:
High Confidence currently reflects strong matchup separation, but not always durable explanation quality. The Admin dashboard showed that High misses can still look strong pregame while producing poor postgame claim validation.

Approach:
Create a pregame-safe expected claim quality score using historical claim signatures such as:
- claim_type
- claim_layer
- registry_core_area
- registry_category
- metric
- two_way_context
- offensive_efficiency_support_bucket
- claim_strength_bucket
- clean_hierarchy_status

Important:
This should be tested offline first. Do not wire it into production confidence yet.

Success question:
Does expected claim quality separate:
- High correct vs High miss
- good reasoning miss vs bad reasoning miss
- Medium strong correct vs High fragile miss

Guardrail:
Avoid hard-coded one-season gates. Treat the score as a calibration feature to validate, not as a manual rule system.

Do not start by editing game_service.py.

The next backend work should be an offline expected-claim-quality calibration experiment, likely in `agg/gamelens_training/`, either by extending `build_claim_language_calibration.py` or creating a new worker such as `build_expected_claim_quality.py`.

Only after that feature proves useful should `game_service.py` be updated to expose the score as metadata, and only later should it influence runtime confidence.

##