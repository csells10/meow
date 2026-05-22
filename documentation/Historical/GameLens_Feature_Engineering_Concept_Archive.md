# GameLens Feature Engineering Playbook
## Turning Level 2 QA into Better Football Signals

**Purpose:**  
This document turns the `claims_96_v2` QA results into a practical feature-engineering plan for GameLens.

The goal is **not** to magically predict every NFL winner. The better goal is:

> Can GameLens tell when a pregame claim actually deserves stronger language?

Examples:

- “MIN generated more pressure” → Did MIN actually create more pass disruption postgame?
- “GB had better Offensive Output” → Did GB actually produce the stronger offensive performance?
- “SF has a Strong Profile / High Outcome Confidence” → Did the main football story actually show up?

You are not overthinking this. You are asking the correct data-science question.

---

# 1. What the 96-game Level 2 QA taught us

The `claims_96_v2` run is a useful baseline:

| Item | Result |
|---|---:|
| Games tested | 96 |
| Claims validated | 2,880 |
| Postgame metric rows loaded | 6,850 |
| Headline claim validation rate | 46.2% |
| Supporting claim validation rate | 45.8% |

That means GameLens has signal, but the current claim/confidence language is too loose.

## Claim type reliability

| Claim Type | Validation Rate | Football read |
|---|---:|---|
| Core Area Comparison | 48.3% | Best broad matchup layer, but not strong enough alone |
| Core Area Summary | 47.8% | Useful, but summaries may be too general |
| Team Comparison Metric | 46.9% | Helpful detail layer |
| Metric Highlight | 46.4% | Useful when paired with stronger context |
| Category Summary | 44.2% | Good for explanation, weaker as a driver |
| Game Profile | 42.6% | Too broad/noisy to drive confidence alone |

## Headline group reliability

| Headline Group | Validation Rate | Football read |
|---|---:|---|
| Scoring Efficiency | 56.7% | Strongest current headline signal |
| Offensive Output | 47.8% | Useful, but needs scoring/efficiency context |
| Disruption and Turnovers | 46.7% | Useful, but volatility risk |
| Defensive Control | 44.4% | Useful, but needs specific metric support |
| Turnovers | 36.7% | Volatile; should be caution/support only |
| Pressure | 32.2% | Too noisy alone; needs support |

## The big lesson

GameLens should not say:

> “Many signals point this way, therefore confidence is high.”

It should say:

> “The most meaningful, reliable, and context-supported signals point this way, therefore stronger language may be justified.”

---

# 2. Football translation for a novice data scientist

Think of an NFL game as four linked battles:

| Battle | Plain-English meaning | GameLens area |
|---|---|---|
| Can you move the ball? | Yards, first downs, explosive movement | Offensive Output |
| Can you turn movement into points? | Points/play, TD rate, red zone finish | Scoring Efficiency |
| Can you stop the other team from moving? | Yards allowed, third downs, drive control | Defensive Control |
| Can you create chaos? | Pressure, sacks, turnovers, short fields | Disruption / Turnovers |

A team can win one battle and lose the game.  
A team that wins multiple connected battles is much more interesting.

Example:

- **Offensive Output only:** Team moves the ball, but may stall.
- **Scoring Efficiency only:** Team scores efficiently, but maybe on limited chances.
- **Offensive Output + Scoring Efficiency:** Team both moves and finishes. This is much more powerful.
- **Defensive Control + Scoring Efficiency:** Team limits the opponent and finishes its own drives. Also powerful.
- **Pressure only:** Can be exciting but noisy. Pressure does not always become sacks, turnovers, or a win.
- **Turnovers only:** Extremely noisy. Treat as volatility, not certainty.

---

# 3. Core principle: split “better” from “elevated”

This is the most important language cleanup.

## Better

A team is **better** when it has a directional edge over the opponent.

Example:

> MIN has a better pressure profile than CHI.

That only means MIN is ahead in this matchup.

## Elevated

A team is **elevated** when it is not just better than the opponent, but meaningfully strong by league/rank/percentile context.

Example:

> MIN has an elevated pressure profile.

That should require:

- MIN beats the opponent in pressure-related metrics
- MIN is high in league percentile/rank
- The gap is meaningful
- Related support exists, such as opponent pass protection weakness or turnover risk

## Why this matters

A team can be “less bad” than the opponent without being good.

That should not earn strong language.

---

# 4. Thirty feature-engineering steps specific to GameLens

## A. Build better targets first

### 1. Create a claim-level training table

Build a table like:

```text
Analytics.gamelens_claim_training_examples
```

One row per pregame claim.

Suggested fields:

```text
game_id
season
bucket
game_week
claim_type
claim_layer
group_name
claim_name
claimed_team
opponent_team
actual_team
validation_result
validated_flag
headline_flag
supporting_flag
model_result
outcome_confidence_label
profile_type
final_margin_abs
```

Why this matters: winner prediction and claim validation are not the same problem.

---

### 2. Create a clean binary target: `claim_validated_flag`

Use:

```text
1 = validated
0 = not_validated
NULL = unavailable / actual_neutral_or_mixed / neutral_claim_but_actual_edge
```

Do not force neutral/mixed outcomes into right/wrong too early.

---

### 3. Create an “elevated_deserved_flag”

This is different from “validated.”

Example target:

```text
elevated_deserved_flag = 1
when claimed_team led postgame AND actual_percentile_gap >= threshold
```

This helps answer:

> Did the strong/elevated language deserve to exist?

---

### 4. Create a `claim_actual_gap_strength`

For each postgame claim, store the actual gap:

```text
actual_gap
actual_rank_gap
actual_percentile_gap
actual_gap_bucket
```

Suggested gap buckets:

```text
near_even
small_edge
clear_edge
dominant_edge
```

This turns validation from yes/no into strength.

---

### 5. Keep outcome target separate

A claim can validate while the team loses.

Example:

> PIT edge lost to CLE, but v2 found one “good reasoning + bad outcome” game: `20241121_PIT@CLE`.

That is a variance/missing-context bucket, not automatically bad reasoning.

---

## B. Build stronger pregame gap features

### 6. Use percentile gap as the default metric comparison

Raw gaps are hard to compare across metrics.

A `0.03` gap in one metric might be huge.  
A `0.03` gap in another might be noise.

Use:

```text
pregame_percentile_gap = claimed_team_percentile - opponent_percentile
```

---

### 7. Add rank gap

Rank gap is easy to understand.

Example:

```text
claimed_team_rank = 4
opponent_rank = 23
rank_gap = 19
```

That is more meaningful than rank 11 vs rank 14.

---

### 8. Add league-quality tier

For each team/metric/window:

```text
elite: >= 85th percentile
strong: >= 70th
average: 40th-70th
weak: <= 30th
poor: <= 15th
```

This helps prevent “less bad” teams from getting strong labels.

---

### 9. Add opponent weakness tier

A strong team edge matters more if the opponent is weak in the matching area.

Example:

> Pressure edge is more meaningful when the opponent has weak pass protection / sacks taken / poor yards per pass.

Suggested feature:

```text
opponent_vulnerability_tier
```

---

### 10. Add direction agreement inside each Core Area

For a Core Area, count how many metrics point to the same team.

Example:

```text
defensive_control_metrics_total = 5
defensive_control_metrics_for_MIN = 4
defensive_control_agreement_rate = 0.80
```

A Core Area edge with 80% internal agreement is more meaningful than one with 50%.

---

### 11. Add opposing signal count

A team can have a headline edge but still face contradictions.

Example:

```text
core_area_edges_for_team = 3
core_area_edges_against_team = 2
opposing_signal_count = 2
```

Use this to soften language.

---

### 12. Add “edge concentration”

Ask:

> Is the edge spread across many football areas, or only one noisy metric?

Feature:

```text
edge_concentration_score
```

High concentration means one metric may be doing too much work.

---

## C. Improve “Elevated” language

### 13. Require matchup gap plus league quality

For elevated language, require both:

```text
claimed_team beats opponent
claimed_team is strong/elite league-relative
```

This prevents weak statements like:

> “Team A is elevated because Team B is worse.”

---

### 14. Add “fragile elevated” label

A team may be strong, but the opponent may also be strong.

Example:

```text
claimed_team_percentile = 82
opponent_percentile = 78
```

That is not a dominant edge. Use:

```text
fragile_elevated
```

Plain English:

> “Both teams grade well here, so this is not a clean separation.”

---

### 15. Add “unsupported elevated” label

If a claim is elevated but related areas disagree, flag it.

Example:

```text
Pressure = elevated
Turnover Risk = not supportive
Defensive Control = not supportive
```

Use:

```text
unsupported_elevated_flag = true
```

---

### 16. Add claim language tiers

Suggested language mapping:

| Feature strength | Language |
|---|---|
| Tiny / split | “near even” |
| Small directional edge | “slight edge” |
| Clear gap, decent support | “clear edge” |
| Strong gap + league quality | “elevated edge” |
| Strong gap + multiple confirming areas | “major advantage” |

Avoid jumping from “better” to “strong.”

---

## D. Build football combo features

This is where your idea is strongest.

### 17. Offensive Output + Scoring Efficiency

This is one of the best combo candidates.

Football meaning:

> The team can move the ball and finish drives.

Feature:

```text
offense_plus_scoring_combo_score
```

Why it matters: Offensive Output alone can be empty yards. Scoring Efficiency tells you if the yards become points.

Examples from QA where the main story strongly matched the outcome:

- `20231203_MIA@WSH`: MIA won 45-15 with very strong headline validation.
- `20231217_SF@ARI`: SF won 45-29 with very strong headline validation.
- `20231019_JAX@NO`: JAX won 31-24 with validated headline reasoning.

---

### 18. Defensive Control + Scoring Suppression

Football meaning:

> The team limits movement and prevents points.

Feature:

```text
defensive_control_plus_scoring_suppression_score
```

This should be stronger than Defensive Control alone.

---

### 19. Pressure + Opponent Passing Weakness

Pressure alone validated poorly in the QA baseline. Headline Pressure validated around 32.2%.

Do not throw it away. Reframe it.

Feature:

```text
pressure_plus_opponent_passing_weakness
```

Possible inputs:

```text
pressure percentile
sacks percentile
opponent sacks_taken percentile
opponent yards_per_pass weakness
opponent pass_attempt_rate
```

Football meaning:

> Pressure matters more when the opponent is likely to pass and vulnerable when passing.

---

### 20. Pressure + Turnover Risk

Pressure can force bad throws. Turnovers alone are noisy. Together, they may be more meaningful.

Feature:

```text
pressure_turnover_combo_score
```

But keep this as a volatility feature, not a high-confidence feature.

Suggested language:

> “This matchup has disruption upside.”

Not:

> “This team should win.”

---

### 21. Rushing Game + Defensive Control

Football meaning:

> A team can shorten the game, control tempo, and keep the opponent from getting extra chances.

Feature:

```text
rushing_control_combo_score
```

This may help with cases where a team wins without flashy passing output.

---

### 22. Scoring Efficiency + Low Turnover Risk

Football meaning:

> The team finishes drives and avoids giving away possessions.

Feature:

```text
clean_finish_combo_score
```

This may be better than raw scoring efficiency alone.

---

### 23. Offensive Output + Opponent Defensive Weakness

Football meaning:

> A good offense is facing a defense likely to let that strength show up.

Feature:

```text
offensive_output_vs_defensive_vulnerability
```

This is matchup-specific, not just team-quality-specific.

---

### 24. Defensive Control + Opponent Offensive Weakness

Football meaning:

> A strong/solid defense faces an offense that may struggle to sustain drives.

Feature:

```text
defensive_control_vs_offensive_weakness
```

This can support stronger defensive language.

---

### 25. Two-way team edge

Football meaning:

> The same team has both offensive and defensive paths to win.

Feature:

```text
two_way_edge_score
```

Example:

```text
Offensive Output favors team
Scoring Efficiency favors team
Defensive Control favors team
```

This should be more valuable than any one Core Area.

---

### 26. Split-profile danger flag

Football meaning:

> The model sees strengths, but they are split between teams.

Feature:

```text
split_profile_danger_flag
```

Example:

```text
Team A: Offensive Output + Scoring Efficiency
Team B: Defensive Control + Disruption
```

This should soften confidence.

---

### 27. Volatility warning feature

Football meaning:

> A game may swing on turnovers, pressure, short fields, or late-game variance.

Feature:

```text
volatility_warning_score
```

Inputs:

```text
turnover reliance
pressure reliance
low offensive agreement
near-even core areas
high opposing signal count
```

Use this to reduce confidence, not necessarily change the lean.

---

### 28. No Pick “hidden lean” feature

The 96-game v2 run found No Pick games where claims validated strongly.

Examples:

- `20231008_CAR@DET`: No Pick, DET won 42-24, headline validation 0.778.
- `20231124_MIA@NYJ`: No Pick, MIA won 34-13, headline validation 0.667.
- `20241225_BAL@HOU`: No Pick, BAL won by 29, unique validation 0.696.
- `20251019_WSH@DAL`: No Pick, final margin 22, headline validation 0.667.
- `20251228_NYG@LV`: No Pick, final margin 24, unique validation 0.636.

Feature:

```text
hidden_lean_score
```

This should not force a pick yet. It should create:

> “No Pick, but one side has a cautious structural lean.”

---

### 29. High-confidence failure warning feature

Some High Outcome Confidence games had bad reasoning and bad outcomes.

Examples:

- `20240114_GB@DAL`: DAL edge, GB won 48-32.
- `20241229_LV@NO`: NO edge, LV won 25-10.
- `20250113_MIN@LAR`: MIN edge, LAR won 27-9.
- `20231023_SF@MIN`: SF edge, MIN won 22-17.

Feature:

```text
high_confidence_failure_risk
```

Inputs:

```text
pressure-heavy explanation
turnover-heavy explanation
low headline agreement
weak scoring efficiency support
split or conflicting supporting areas
late-season/playoff context
```

This feature should cap confidence.

---

### 30. Tie / push handling feature

`20250928_GB@DAL` ended 40-40 but currently shows up like a normal bad outcome.

Fix this.

Feature:

```text
outcome_result_type = win/loss/tie/no_decision
```

Do not train strict win/loss accuracy on ties.

---

# 5. The football combinations worth testing first

## Tier 1: Best first combo candidates

These are the ones I would test first.

### 1. Offensive Output + Scoring Efficiency

Why:

- Moving the ball plus finishing drives is a real football recipe.
- Scoring Efficiency was the strongest headline group in the QA baseline.

Feature:

```text
offense_finish_score =
  offensive_output_percentile_gap
+ scoring_efficiency_percentile_gap
+ offensive_internal_agreement
+ scoring_internal_agreement
```

Potential label:

```text
sustained_scoring_edge
```

---

### 2. Defensive Control + Scoring Suppression

Why:

- Limiting yards matters more if it also limits points.
- Defensive Control alone may be too broad.

Feature:

```text
defensive_suppression_score =
  defensive_control_percentile_gap
+ points_allowed_per_play_gap
+ third_down_defense_gap
+ red_zone_defense_gap
```

Potential label:

```text
suppression_edge
```

---

### 3. Two-way Edge

Why:

- Teams that can both score and suppress have multiple paths to a win.

Feature:

```text
two_way_edge_score =
  offense_finish_score
+ defensive_suppression_score
```

Potential label:

```text
two_way_profile
```

---

### 4. Disruption Upside

Why:

- Pressure and turnovers are noisy alone, but they can explain upset potential or game volatility.

Feature:

```text
disruption_upside_score =
  pressure_gap
+ sacks_gap
+ opponent_sacks_taken_gap
+ turnover_margin_gap
```

Potential label:

```text
disruption_upside
```

Use as a caution or upside label, not a primary pick driver.

---

### 5. Hidden Lean

Why:

- Some No Pick games had strong validated claims postgame.

Feature:

```text
hidden_lean_score =
  headline_claim_strength
+ core_area_agreement
+ top_metric_gap_strength
- opposing_signal_count
```

Potential label:

```text
no_pick_with_structural_lean
```

---

# 6. How examples from QA should guide the next model

## Good examples: strong story that showed up

### `20231217_SF@ARI`

- GameLens: SF edge
- Result: SF won 45-29
- Matchup label: Strong Profile / High Outcome Confidence
- Headline validation: 0.889

Takeaway:

> This is what a real high-confidence profile should look like.

Look for what was aligned here:

```text
Scoring support
Offensive output support
Low contradiction
Strong headline validation
```

---

### `20231203_MIA@WSH`

- GameLens: MIA edge
- Result: MIA won 45-15
- Matchup label: Strong Profile / Medium Outcome Confidence
- Headline validation: 0.889

Takeaway:

> Strong profile with strong claim validation may deserve higher confidence if pregame features showed broad agreement.

---

### `20231019_JAX@NO`

- GameLens: JAX edge
- Result: JAX won 31-24
- Matchup label: Clear Lean / Medium Outcome Confidence
- Headline validation: 0.625

Takeaway:

> Medium confidence can be correct and well-reasoned. Not every validated lean needs to be High.

---

## Bad examples: strong language that failed

### `20240114_GB@DAL`

- GameLens: DAL edge
- Result: GB won 48-32
- Matchup label: Strong Profile / High Outcome Confidence
- Headline validation: 0.125

Takeaway:

> This is a major calibration failure. The model likely overtrusted season/profile strength and missed matchup volatility, recent form, or playoff context.

---

### `20241229_LV@NO`

- GameLens: NO edge
- Result: LV won 25-10
- Matchup label: Strong Profile / High Outcome Confidence
- Headline validation: 0.111

Takeaway:

> Very low claim validation plus High confidence means High was too easy to earn.

---

### `20250111_LAC@HOU`

- GameLens: LAC edge
- Result: HOU won 32-12
- Matchup label: Clear Lean / Medium Outcome Confidence
- Headline validation: 0.000

Takeaway:

> Even Medium can be too strong when headline claims completely fail. This is a strong model-learning case.

---

# 7. What not to do

## Do not hand-code football opinions too aggressively

Avoid:

```text
Pressure is always bad
Turnovers are always useless
Core Areas are always best
```

Better:

```text
Pressure alone is noisy, but pressure + opponent vulnerability may matter.
Turnovers are volatile, but turnover risk + pressure + weak offense may matter.
Core Areas are useful, but only when internally aligned.
```

## Do not chase winner prediction first

Better order:

1. Improve claim validation
2. Improve elevated/strong language
3. Improve confidence calibration
4. Then build outcome model on top

## Do not overfit to 96 games

This is a baseline, not final truth.

Use it to design features, then test on more seasons/games.

---

# 8. Tomorrow’s practical work plan

## Step 1: Fix tie handling

File likely involved:

```text
qa_validate_gamelens_claims_v2.py
qa_collect_gamelens_payloads_bucket_v2.py
services/model_trust_service.py or services/game_service.py
```

Goal:

```text
tie -> Tie / Push / No Decision
```

Not Incorrect.

---

## Step 2: Create the training table design

Draft schema:

```text
Analytics.gamelens_claim_training_examples
```

Required fields:

```text
game_id
season
bucket
claim_type
claim_layer
group_name
claim_name
claimed_team
opponent_team
validated_flag
elevated_deserved_flag
pregame_percentile_gap
pregame_rank_gap
actual_percentile_gap
core_area_agreement_rate
opposing_signal_count
profile_type
outcome_confidence_label
model_result
final_margin_abs
```

---

## Step 3: Add pregame feature extraction

For each claim, attach:

```text
pregame_raw_gap
pregame_percentile_gap
pregame_rank_gap
claimed_team_league_percentile
opponent_league_percentile
claimed_team_tier
opponent_tier
```

---

## Step 4: Add combo features

Start with only 5:

```text
offense_finish_score
defensive_suppression_score
two_way_edge_score
disruption_upside_score
hidden_lean_score
```

Do not create 40 features on day one.

---

## Step 5: Re-run 96-game QA

Compare before/after:

```text
headline validation by claim type
bad_reasoning_bad_outcome count
good_reasoning_correct_outcome count
no_pick_hidden_lean count
high_confidence_failure count
```

---

# 9. Suggested model layer later

Once the training table is built, train the first model to predict:

```text
claim_validated_flag
```

Not winner.

Potential models:

```text
Logistic regression
Random forest
XGBoost / LightGBM
```

Start simple.

Best first target:

```text
Will this claim validate postgame?
```

Second target:

```text
Did this claim deserve elevated language?
```

Third target later:

```text
Should this matchup receive High/Medium/Low Outcome Confidence?
```

Winner prediction comes last.

---

# 10. Final philosophy

You are not trying to become a football expert overnight.

You are building a system that asks better questions than a casual fan would ask:

- Did the claimed strength actually show up?
- Was the edge just directional, or truly elevated?
- Did multiple football areas agree?
- Did the model overtrust noisy areas?
- Did the model miss a hidden lean?
- Did the final result match the actual matchup story?

That is real data science.

And honestly, this is the right path:

> Build better football claims first.  
> Calibrate language second.  
> Predict outcomes third.



Summary so far — Feature Testing Day 🏈🧪

We did exactly what your GameLens notes say to do: test feature ideas in SQL before coding anything. The goal stayed clean:

Does this pregame feature help related claims validate more often?

Not:

Does this feature pick winners?

That matters because GameLens is trying to become a matchup intelligence / claim truth tool, not a forced pick machine.

1. We tested rushing_control_score
First pass

The first rushing SQL looked broken because every row landed in:

rushing_unavailable

That happened because the bucket rule required:

score_weight_available >= 0.60

But most rows only had:

yards_per_rush + control_resistance = 0.55 available weight

So the feature was blocked before it could actually separate into buckets.

After the fix

Once we lowered the availability gate, the buckets started working.

Best finding

For narrow rushing claims, rushing_supportive looked useful:

narrow rushing supportive:
547 rows
324 validated
~59.2% validation

Compared to:

narrow rushing mixed:
238 rows
98 validated
~41.2% validation

That is a real signal.

Rushing conclusion

Good candidate, but only narrowly.

Keep testing as:
rushing_efficiency_support_score

or:

rush_claim_support_score
Use only for:
yards_per_rush
Rushing Game
narrow rushing claims
Do not use yet for:
winner confidence
matchup_lean confidence
Model Trust
broad drive-control warnings
automatic confidence caps

The drive-control side was messy. rushing_conflict did not behave reliably like a warning bucket.

2. We tested passing_efficiency_support_score

This one was more interesting.

Coverage check

The coverage result showed this was mostly a yards_per_pass test, not a full passing bundle yet.

The important coverage result:

larger_240 yards_per_pass / Passing Game / Offensive Output:
174 rows
103 validated
~59.2% validation

And the broader yards_per_pass rows in the larger sample were also solid.

Bucket test

This one produced a cleaner ladder.

Broad offensive claims
passing_conflict:   ~37.3%
passing_mixed:      ~48.9%
passing_supportive: ~58.8%

That is exactly the kind of shape we want:

conflict < mixed < supportive
Narrow passing claims
passing_mixed:      ~44.7%
passing_supportive: ~58.6%

Also good.

Best finding

The strongest combo was:

two_way_context = supportive
+
passing_support_bucket = passing_supportive

That validated around:

~64%+ for narrow passing claims
~69% for broad offensive claims

That is a better signal than the rushing test.

Passing conclusion

This is currently the stronger candidate.

Better name:
passing_efficiency_claim_support_v0

or, more honestly:

yards_per_pass_support_score
Use for:
yards_per_pass
Passing Game
narrow passing claims
broad offensive claims only when two_way_context is supportive
Do not use for:
winner prediction
automatic matchup confidence
Model Trust
passing volume claims
Current feature ranking
Feature	Status	Best Use	Confidence
passing_efficiency_claim_support_v0	Strongest candidate so far	Claim-language support	🟢 Good
rush_claim_support_score	Worth keeping	Narrow rushing claim support	🟡 Promising but scoped
rushing_control_score as broad feature	Not ready	Too messy outside rushing claims	🔴 Hold
rushing_conflict as confidence cap	Not ready	Did not behave cleanly	🔴 Hold
Big takeaway

Today we proved the workflow works:

Define a football idea.
Build a temporary SQL feature.
Bucket it.
Compare validation rates.
Decide whether it deserves code.

And the answer so far is:

Passing efficiency support looks like the better next feature. Rushing support is useful, but only in a narrow claim-support role.

That is a very productive little lab day. 🧠🏈

1. drive_sustainability_support_score

This is my top next test.

Question

When a team has pregame drive-sustainability support, do drive/control/offensive claims validate more often?

Possible inputs
1st_down_rate
third_down_pct
total_drives
time_of_possession
total_plays
points_per_play
Why I like it

This sits between rushing and passing. It asks:

Can the team stay on schedule and keep drives alive?

That matters for:

Drive Conversion
Offensive Rhythm
Offensive Output
Scoring Efficiency
rushing/passing support context
Best use if it works
claim support
offensive stability signal
confidence restraint helper

Not winner confidence.

2. scoring_conversion_support_score
Question

When a team has pregame scoring-conversion support, do scoring-related claims validate more often?

Possible inputs
points_per_play
td_rate
red_zone_efficiency
passing_tds_rushing_tds_sum
1st_down_rate
Caution

red_zone_efficiency has already looked unstable in earlier QA, so I would not let it drive the score alone.

Better version:

points_per_play is the anchor
td_rate is support
red_zone_efficiency is watch/caution only
Best use
Scoring Efficiency claim support
finish-drive language
stronger/softer scoring copy
3. explosive_offense_warning

This one is not a support score. It is a warning flag.

Question

Are there teams with explosive offensive upside that make a model lean more fragile?

Possible inputs
yards_per_play
yards_per_pass
points_per_play
passing_tds
yards_per_rush
Why this matters

This ties directly to your high-confidence miss lessons: sometimes a team can lose the “broad profile” but still have enough explosiveness to wreck the read.

Best use
upside warning
confidence cap reason
high-confidence failure-risk reason

Example future language:

Confidence held down because the opponent carries explosive offensive upside despite weaker broad profile.

This is a very GameLens feature.

4. defensive_resistance_support_score
Question

When a team has pregame defensive resistance support, do defensive claims validate more often?

Possible inputs
points_allowed_per_play
points_allowed_per_yard
defensive_success_rate
yards_allowed
points_allowed
Why test it

You already have defensive_suppression_score, but this could be a cleaner follow-up focused specifically on:

Defensive Control
Scoring Suppression
Defensive Efficiency
Best use
defensive claim support
defensive confidence restraint
better “Defensive Control” language
5. turnover_volatility_warning
Question

Do turnover-heavy profiles create more claim instability?

Possible inputs
turnover_margin_per_game
interceptions_thrown
defensive_interceptions
fumbles_lost
fumbles_recovered
sack_to_turnover_ratio
Important

This should not be a boost feature.

Turnovers are noisy. This should be tested as:

volatility warning
confidence cap reason
do-not-overstate flag
Best use
“This matchup has turnover volatility, so confidence should stay measured.”
6. pressure_disruption_warning
Question

Does pressure/disruption support explain volatility, upsets, or fragile claims?

Possible inputs
pressure_rate
sacks
sacks_taken
sack_to_turnover_ratio
interceptions_thrown
Best use
disruption upside
chaos warning
confidence cap
not automatic support

This could help explain games where the broader model was right-ish, but pressure flipped possessions or created short fields.

My recommended order
Priority	Feature	Why
1	drive_sustainability_support_score	Most likely to be clean and useful
2	scoring_conversion_support_score	Directly improves scoring language
3	explosive_offense_warning	Helps with high-confidence miss risk
4	defensive_resistance_support_score	Good defensive-language refinement
5	turnover_volatility_warning	Useful, but noisy
6	pressure_disruption_warning	Useful, but probably chaotic
My pick for the next SQL lab

Start with:

drive_sustainability_support_score

It is the best “next brick” because it can connect passing, rushing, scoring, and offensive output without immediately becoming a noisy chaos feature. Nice middle lane. 🧱🏈