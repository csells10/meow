# GameLens Next Model Phase — Feature Expansion Guardrails

## Current Product Position

Frontend and backend are now speaking the same language better. The next phase should not be “add a bunch of features.” It should be:

> Add one carefully tested model feature family that improves claim truth, confidence restraint, or explanation quality.

GameLens should continue to behave as a matchup intelligence and confidence-calibration tool, not a forced pick machine.

The main target is still:

> Did GameLens correctly identify a pregame matchup truth that later held up in postgame data?

Not:

> Did GameLens pick the winner?

A team can lose the game and still validate a GameLens claim. For example, a team may lose but still have had the stronger Defensive Control profile, better pressure profile, or better rushing efficiency in the actual game.

That distinction should stay protected.

---

## 1. Keep Claim Validation as the Main Target

The current claim-training system is valuable because it asks a better question than simple winner accuracy.

Instead of only grading:

```text
Predicted winner correct / incorrect

GameLens should continue grading:

Did this pregame claim validate postgame?

Examples of claims:

Team A had the better Offensive Output profile.
Team B had the better Defensive Control profile.
Team A had a Scoring Efficiency edge.
Team B had a Disruption and Turnovers lean.
Team A had a metric highlight in third_down_pct.

Each claim should be treated as a row that can be validated independently.

This allows GameLens to learn which types of football explanations deserve stronger wording, softer wording, or caution.

2. Use SQL First Before Coding Any New Feature

Do not code a new feature just because it sounds smart.

The workflow should be:

1. Define the football idea.
2. Test it in SQL against the existing claim-training table.
3. Compare baseline sample and fresh sample.
4. Look for repeatable lift.
5. Check row counts so the signal is not tiny/noisy.
6. Confirm the football interpretation makes sense.
7. Only then code it into update_claim_training_features.py.
8. Dry-run locally.
9. Inspect the dry-run CSV.
10. Write to BigQuery.
11. Re-check the feature in BigQuery.
12. Commit only if the signal is meaningful.

The standard question should be:

When this feature context exists, do related GameLens claims validate more often?

Not:

Did this feature pick more winners?
3. Keep two_way_context as the Current Known-Good Feature

The strongest proven feature so far is:

two_way_context = supportive

This means the claimed team has both:

offense_finish_score >= 0.15
defensive_suppression_score >= 0.15

The important lesson:

two_way_context = supportive

should be used as claim-language support, not as an automatic winner-confidence boost.

Good use:

This claim has supportive two-way context, so stronger explanation language may be allowed.

Bad use:

two_way_context is supportive, so increase the pick confidence automatically.

This feature should remain a language-calibration and claim-support feature first.

4. Best Next Feature Track: Confidence Cap Reasons

The most useful next feature may not be another score.

It may be a structured reason for why confidence should be held down.

Possible field:

confidence_cap_reason

Possible values:

split_core_areas
feature_conflict
turnover_volatility
missing_feature_support
offense_defense_mismatch
metric_only_defensive_support_rejected
early_season_low_data
two_way_unavailable
recent_form_conflict
explosive_offense_warning

Why this matters:

GameLens already has a better frontend language system now. The backend should get better at explaining why a matchup read is capped at Low or Medium confidence.

Example future API language:

Confidence capped because the offensive profile is supportive, but defensive suppression is unavailable.

or:

Confidence capped because Core Areas were split and the turnover/disruption profile adds volatility.

This feels more immediately useful than adding another raw model score.

5. Feature Ideas Worth Testing Next

Do not build these all at once. Treat them as candidates.

A. rushing_control_score

Question:

Does pregame rushing-control support help related claims validate more often?

Why it matters:

Rushing/control has repeatedly appeared as a blind spot. It affects clock, drive stability, late-game control, game script, variance, and physical matchup shape.

Possible inputs:

yards_per_rush
rushing_yards
rushing_attempts
run_play_pct
opponent defensive control
points_allowed_per_play

Possible use:

claim support
confidence cap reason
rushing-control warning

Do not use it as an automatic winner override until tested.

B. disruption_upside_score

Question:

Does disruption / pressure / turnover context identify volatility or upset potential?

Why it matters:

Pressure and turnovers are noisy alone, but they can still explain game volatility, short fields, defensive scores, and sudden script changes.

Possible inputs:

Disruption and Turnovers core area
Pressure
sacks
turnover_margin
defensive_interceptions
fumbles_recovered
sack_to_turnover_ratio
opponent sacks_taken

Best use:

upside warning
volatility flag
confidence cap reason

Bad use:

automatic confidence boost
C. hidden_lean_score

Question:

In No Pick / Low Confidence games, was there still a cautious structural lean?

Why it matters:

Some No Pick games may contain useful structure without enough support to become a full pick.

Possible inputs:

offense_finish_score
defensive_suppression_score
two_way_context
rushing_control_score
disruption_upside_score
core_area split
team comparison edge score
opposing_signal_count

Important caution:

This should not turn every No Pick into a pick.

Better product use:

No Pick, but one side has a cautious structural lean.

or:

No Pick, but one side has upside warning signs.
D. high_confidence_failure_risk

Question:

Are there signs that a High or Medium confidence read should be capped because the support is fragile?

Possible inputs:

pressure-heavy explanation
turnover-heavy explanation
low headline agreement
weak scoring efficiency support
split or conflicting Core Areas
late-season / playoff context
recent form conflict

Best use:

confidence cap
miss-risk warning
model trust reason code

This would help separate a clean strong profile from a profile that looks strong but is built on fragile or noisy ingredients.

E. recent_form_support

Question:

Does recent form support, conflict with, or sharpen the season-to-date profile?

Possible inputs:

last_3_games
last_7_games
regular_season_to_date
regular_plus_postseason_to_date

Important caution:

Recent form should not become the primary Matchup Lean source yet.

Best use:

recent form supports the season profile
recent form conflicts with the season profile
recent form warning
late-season form adjustment candidate

This could help explain severe misses where season-to-date profile looked good but recent team shape had changed.

6. What Not To Do Yet

Do not jump straight into:

winner prediction model
spread model
betting market layer
injury override layer
public model accuracy dashboard
dynamic frontend driver UI
large new frontend sections

Those are bigger projects.

The better order is:

1. Improve claim validation.
2. Improve language calibration.
3. Improve confidence caps.
4. Add carefully tested support features.
5. Then consider outcome prediction improvements.

Winner prediction should come later, on top of a more trustworthy claim-quality system.

Recommended Next Work Session
Step 1 — Choose one feature family

Recommended first choice:

confidence_cap_reason

Secondary choice:

rushing_control_score
Step 2 — SQL-only exploration

Before coding, write SQL to test whether the candidate feature has signal.

For confidence caps, test whether games/claims with known caution patterns validate worse or produce more severe misses.

Possible SQL buckets:

split_core_areas
two_way_unavailable
available_mixed
turnover_volatility
offense_defense_mismatch
near_even_team_comparison
high_confidence_with_low_claim_support
Step 3 — Compare across runs

Use at least:

baseline_96_stage1_v2
fresh_96_features_qa_v2
larger_240_level3_qa_20260517

The feature should not be trusted if it only works in one sample.

Step 4 — Decide whether to code

Only code if the feature shows:

repeatable lift or repeatable risk
enough row count
clear football interpretation
no postgame leakage
clear product use
Step 5 — Add as metadata first

New features should first appear as backend metadata.

They should not immediately change:

winner logic
matchup lean
outcome confidence
frontend copy

Let the data prove itself before it drives user-facing behavior.

Current Recommended Priority

My recommended next model work is:

Priority 1: confidence_cap_reason
Priority 2: rushing_control_score
Priority 3: disruption_upside_score
Priority 4: hidden_lean_score
Priority 5: recent_form_support

The theme is:

Help GameLens know when to speak carefully before teaching it to speak louder.

That matches the current product direction and protects the frontend improvements we just made.

## Tomorrow Focus — Metric Opportunity Discovery Before More Feature Building

Before adding more advanced hand-built features, pause and do an exploratory data pass first.

The recent feature labs were useful, but many of the best ideas were advanced combo features. Next step should be understanding the data more directly:

> What metrics show up most often, validate most often, and naturally group together?

### Goal

Build a Metric Opportunity Board that helps identify feature candidates from the data first, instead of inventing football theories first.

### Phase 1 — Metric Opportunity Map

Use SQL to answer:

- Which metrics appear most often?
- Which metrics have enough row count to trust?
- Which metrics validate above/below baseline?
- Which metrics behave differently by:
  - `claim_type`
  - `claim_layer`
  - `metric`
  - `category`
  - `core_area`
  - `two_way_context`

Output should help separate:

- high-volume useful metrics
- low-volume noisy metrics
- unstable metrics
- metrics that only work in certain claim types/layers

### Phase 2 — Group Discovery

Use SQL to find which metrics naturally travel together.

Questions:

- Which metrics repeatedly appear in the same claim families?
- Which metrics validate together?
- Which categories/core areas share useful behavior?
- Are there natural groups like:
  - passing efficiency
  - offensive efficiency
  - scoring support
  - defensive suppression
  - rushing path
  - volatility / turnover risk

### Phase 3 — Backward Engineer Candidate Features

Instead of starting with a feature idea, start with the strongest recurring metric groups.

Pattern:

1. Find metrics/groups with volume.
2. Check validation lift.
3. Check consistency across runs.
4. Check behavior by claim type/layer.
5. Only then create candidate feature names and bucket logic.

### Phase 4 — Python / Automated Feature Engineering Later

SQL should come first.

Python can help after the SQL discovery pass by doing:

- automated feature ranking
- correlation checks
- decision tree splits
- random forest feature importance
- SHAP-style explanations
- association rules
- automated bucket testing

K-means is possible later, but not the first tool. It is better for finding matchup/game profile clusters, not necessarily for deciding which claim-language features to build.

### Current Direction

Do not build more advanced combo features yet.

First create a clearer data map:

| Group | Metrics | Row Count | Validation Rate | Lift | Candidate Feature? |
|---|---|---:|---:|---:|---|
| Passing Efficiency | TBD | TBD | TBD | TBD | TBD |
| Defensive Suppression | TBD | TBD | TBD | TBD | TBD |
| Scoring Support | TBD | TBD | TBD | TBD | TBD |
| Rushing Support | TBD | TBD | TBD | TBD | TBD |
| Volatility / Turnovers | TBD | TBD | TBD | TBD | TBD |

### Key Principle

The next GameLens feature pass should be:

> Data discovery first, feature design second.

This should make the model cleaner, less hand-wavy, and easier to defend.

--direction of todays work--

My take: today should have 2 tracks
Track 1 — “Poke Holes” Audit

Before discovering anything new, I’d make yesterday’s 10 tested features prove themselves again.

For each feature, ask:

Question	Why it matters
Did it work across baseline, fresh, and larger_240?	Avoids one-sample magic
Did it have enough row count?	Avoids tiny noisy wins
Did it only work because of one metric?	Example: “passing support” may really be yards_per_pass
Did it work only in narrow claim families?	Good feature, wrong scope = bad product logic
Did conflict buckets actually behave like warnings?	If not, don’t call it a warning
Is the feature useful for language support, confidence cap, or just exploration?	Prevents it from steering the car too early

This is where I’d be strict. Some features should get renamed, narrowed, or benched.

Track 2 — Metric Opportunity Discovery

Then, yes, look for patterns in the existing data.

Your May 22 note already frames this well: build a Metric Opportunity Board that asks which metrics appear most often, which have enough volume, which validate above/below baseline, and how they behave by claim_type, claim_layer, metric, category, core_area, and two_way_context.

That should come before more feature engineering.

What I would add

I’d add one step before “find patterns”:

Build a Feature Claim Ledger

For each of the 10 tested features, make a simple table:

Feature	Best bucket	Best scope	Row count	Validation rate	Lift vs baseline	Repeatable?	Decision
passing_efficiency_claim_support_v0	supportive	yards_per_pass / Passing Game	TBD	TBD	TBD	TBD	keep / narrow
rush_claim_support_score	supportive	yards_per_rush / Rushing Game	TBD	TBD	TBD	TBD	keep scoped
rushing_control_score	broad	broad offensive/control	TBD	TBD	TBD	TBD	hold
turnover_volatility_warning	TBD	volatility/caution	TBD	TBD	TBD	TBD	test only

This gives you a “courtroom evidence board.” No vibes. No goblin math. 🧌

What I would take away

I would not do these today:

Do not code a new feature into update_claim_training_features.py.
Do not start K-means yet.
Do not start Python ML yet.
Do not decide “best feature” from one validation-rate table.
Do not use winner accuracy as the main target.

K-means may be useful later for game-profile clustering, but your own note is right: SQL comes first because you are still trying to understand claim-level metric behavior.

My recommended work order today
Inventory yesterday’s 10 feature claims
Re-test each across baseline / fresh / larger_240
Label each feature: keep, narrow, rename, hold, discard
Build Metric Opportunity Board
Run metric/category/core-area frequency tables
Look for metric groups that validate together
Only then propose the next feature family
Definition of done

Today is successful if you end with:

“Here are the features that survived re-validation, here are the ones that were overfit/noisy, and here are the metric groups the data itself suggests we should explore next.”

That’s the clean path. Yesterday was the lab bench. Today is peer review. 🔬✅


| Rank | Feature                               | What It Was Testing                                                                              | Yesterday’s Status                            | Best Use                                                                       | Main Concern / Hole to Poke Today                                                                         | Today’s Revalidation Decision |
| ---: | ------------------------------------- | ------------------------------------------------------------------------------------------------ | --------------------------------------------- | ------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------- | ----------------------------- |
|    1 | `offense_vs_defense_collision_v0`     | Whether a team’s offensive strength has a clean path against the opponent’s defensive resistance | Best core matchup-path feature                | Offensive, rushing, scoring claim language                                     | Confirm it is not just restating `two_way_context` or overfitting broad offensive claims                  | Re-test first                 |
|    2 | `matchup_fragility_warning_v0`        | Whether a strong-looking matchup profile is actually fragile because of conflict/resistance      | Best combo language/restraint feature         | Confidence restraint, caution language, clean path vs strength-on-strength     | Confirm warning buckets actually validate worse or create more severe misses                              | Re-test first                 |
|    3 | `passing_efficiency_claim_support_v0` | Whether passing efficiency support helps passing/broad offensive claims validate                 | Strong clean single-area support              | Passing claims, broad offensive claim support when scoped                      | Make sure it is not only `yards_per_pass` wearing a fancy costume 🕵️                                     | Keep, but scope tightly       |
|    4 | `defensive_resistance_support_v0`     | Whether defensive resistance context helps defensive claims validate                             | Strong defensive-language candidate           | Defensive Control / suppression language                                       | Confirm Defensive Control is required and isolated metrics do not create fake support                     | Keep, but validate scope      |
|    5 | `defense_vs_dynamite_v0`              | Whether defensive strength still holds up against explosive opponent upside                      | Good narrow defensive caution/support feature | Defensive caution, opponent upside warnings                                    | Check if it works broadly or only in narrow defensive claim families                                      | Re-test as caution feature    |
|    6 | `explosive_offense_warning_v0`        | Whether opponent explosiveness makes otherwise strong reads more fragile                         | Good opponent-upside warning                  | Fragility warning, high-confidence failure risk, confidence cap reason         | Make sure it predicts caution, not just “good offenses are good”                                          | Re-test as warning only       |
|    7 | `scoring_claim_support_v0`            | Whether scoring/conversion support improves scoring-related claims                               | Useful scoring support                        | Scoring Efficiency and finish-drive language                                   | Check if red-zone or TD-rate noise is sneaking into the signal                                            | Keep as scoped support        |
|    8 | `drive_conversion_scoring_support_v0` | Whether drive/conversion/scoring support helps offensive claims validate                         | Useful but conditional                        | Broad offensive/scoring claims, especially with `two_way_context = supportive` | It was misnamed as drive sustainability; confirm it is conversion/scoring support, not true drive control | Keep but rename/limit         |
|    9 | `rush_claim_support_score`            | Whether rushing support helps rushing-related claims validate                                    | Real but likely superseded by matchup path    | `yards_per_rush`, Rushing Game, narrow rushing claims                          | Confirm it still adds value after `offense_vs_defense_collision_v0`                                       | Backlog / narrow use          |
|   10 | Broad `rushing_control_score`         | Whether rushing/control explains overall matchup shape                                           | Not ready                                     | Research only                                                                  | Broad drive-control behavior was messy; do not let this become a fake confidence cap                      | Do not promote                |

-- ============================================================
-- Feature 1: offense_vs_defense_collision_v0
--
-- Fixed version:
-- Uses existing row-level defensive_suppression_score
-- instead of missing home_defensive_suppression_score /
-- away_defensive_suppression_score fields.
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(offense_finish_score AS FLOAT64) AS offense_finish_score,
    SAFE_CAST(defensive_suppression_score AS FLOAT64) AS defensive_suppression_score

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- Build one defensive suppression score per side per game.
-- This lets offensive rows borrow the opponent's defensive score.
side_defense_scores AS (
  SELECT
    run_id,
    game_id,
    claimed_side AS side,
    AVG(defensive_suppression_score) AS side_defensive_suppression_score,
    COUNTIF(defensive_suppression_score IS NOT NULL) AS defensive_score_rows
  FROM base
  WHERE defensive_suppression_score IS NOT NULL
    AND claimed_side IN ('away', 'home')
  GROUP BY
    run_id,
    game_id,
    claimed_side
),

scoped AS (
  SELECT
    b.*,

    CASE
      WHEN b.core_area = 'Offensive Output' THEN 'broad_offensive'
      WHEN b.core_area = 'Scoring Efficiency' THEN 'scoring'
      WHEN b.category = 'Passing Game' THEN 'passing'
      WHEN b.category = 'Rushing Game' THEN 'rushing'
      WHEN b.category IN ('Drive Conversion', 'Red Zone Finish', 'Scoring Production') THEN 'scoring'
      WHEN b.metric IN (
        'yards_per_play',
        'yards_per_pass',
        'yards_per_rush',
        'points_per_play',
        'third_down_pct',
        'red_zone_efficiency',
        '1st_down_rate'
      ) THEN 'metric_offensive'
      ELSE NULL
    END AS scope_checked,

    b.offense_finish_score AS claimed_offense_finish_score,

    ods.side_defensive_suppression_score AS opponent_defensive_suppression_score,
    ods.defensive_score_rows AS opponent_defensive_score_rows

  FROM base b
  LEFT JOIN side_defense_scores ods
    ON b.run_id = ods.run_id
   AND b.game_id = ods.game_id
   AND b.opponent_side = ods.side
),

bucketed AS (
  SELECT
    *,

    CASE
      WHEN scope_checked IS NULL THEN NULL

      WHEN claimed_offense_finish_score IS NULL
        OR opponent_defensive_suppression_score IS NULL
        THEN 'unavailable'

      WHEN claimed_offense_finish_score >= 0.15
        AND opponent_defensive_suppression_score <= 0.00
        THEN 'offense_clear_path'

      WHEN claimed_offense_finish_score >= 0.15
        AND opponent_defensive_suppression_score >= 0.15
        THEN 'strength_on_strength'

      WHEN claimed_offense_finish_score >= 0.15
        AND opponent_defensive_suppression_score > 0.00
        AND opponent_defensive_suppression_score < 0.15
        THEN 'offense_supported'

      WHEN claimed_offense_finish_score < 0.00
        AND opponent_defensive_suppression_score >= 0.15
        THEN 'offense_stressed'

      WHEN opponent_defensive_suppression_score >= 0.15
        THEN 'opponent_defense_warning'

      ELSE 'mixed_or_neutral'
    END AS feature_bucket

  FROM scoped
  WHERE scope_checked IS NOT NULL
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run AS (
  SELECT
    'offense_vs_defense_collision_v0' AS feature_name,
    b.run_id AS test_run,
    b.scope_checked,
    b.feature_bucket,

    COUNT(*) AS row_count,
    SUM(validated_flag_int) AS validated_count,
    COUNTIF(validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(s.baseline_rate, 4) AS baseline_rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS lift_vs_baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs AS (
  SELECT
    'offense_vs_defense_collision_v0' AS feature_name,
    'ALL_SELECTED_RUNS' AS test_run,
    b.scope_checked,
    b.feature_bucket,

    COUNT(*) AS row_count,
    SUM(validated_flag_int) AS validated_count,
    COUNTIF(validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(s.baseline_rate, 4) AS baseline_rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS lift_vs_baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
)

SELECT *
FROM bucket_summary_by_run

UNION ALL

SELECT *
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN test_run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  test_run,
  scope_checked,
  feature_bucket;

-summary

  For Feature 1, offense_vs_defense_collision_v0, the first audit showed that the idea is not strong enough as a broad offensive feature, but it does have value when scoped to scoring/offensive-finish claims. The cleanest pattern was that scoring claims performed better when the claimed team had an offensive path and the opponent defense did not create strong resistance, while strength_on_strength situations consistently reduced or softened the signal. The warning buckets were less reliable: opponent_defense_warning and offense_stressed did not repeat cleanly enough across runs to become firm rules yet. My decision is to keep this feature scoped, not promote it broadly, and mentally rename it to something like scoring_path_vs_defensive_resistance_v0. Best product use for now is language calibration: boost or support scoring claims only when the path is clean, and soften scoring language when the matchup is strength-on-strength.

--simply put

Does this offense have a real path to finish drives against this specific defense, or is it running straight into the opponent’s strength?

--feature 2--

--query
-- ============================================================
-- Feature 2: matchup_fragility_warning_v0
--
-- Football question:
-- Is the claim backed by stable two-way support,
-- or is it fragile because the support is mixed, one-sided,
-- unavailable, or not cleanly aligned?
--
-- Important table limitations:
-- 1) This table is one row per claim, not one row per game.
-- 2) offense_finish_score and defensive_suppression_score are row-perspective fields.
-- 3) two_way_context = unavailable often means scoped feature context is missing,
--    not that the football claim is bad.
-- 4) This tests claim validation, not winner prediction.
--
-- Standard output:
-- Feature_Name
-- Test_Run
-- Scope_Checked
-- Feature_Bucket
-- Row_Count
-- Validation_Rate
-- Lift_vs_Baseline
-- Repeatable_Across_Runs
-- Decision
-- Notes
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(offense_finish_score AS FLOAT64) AS offense_finish_score,
    SAFE_CAST(defensive_suppression_score AS FLOAT64) AS defensive_suppression_score,
    SAFE_CAST(two_way_edge_score AS FLOAT64) AS two_way_edge_score,
    two_way_context,

    SAFE_CAST(pregame_abs_percentile_gap AS FLOAT64) AS pregame_abs_percentile_gap,
    SAFE_CAST(core_area_agreement_rate AS FLOAT64) AS core_area_agreement_rate

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

scoped AS (
  SELECT
    *,

    CASE
      WHEN claim_type IN ('core_area_comparison', 'core_area_summary')
        THEN 'core_area_claims'

      WHEN claim_type = 'category_summary'
        THEN 'category_claims'

      WHEN claim_type IN ('metric_highlight', 'team_comparison_metric')
        THEN 'metric_claims'

      WHEN claim_type = 'game_profile'
        THEN 'game_profile_claims'

      WHEN claim_layer = 'headline'
        THEN 'headline_claims'

      WHEN claim_layer = 'supporting'
        THEN 'supporting_claims'

      ELSE 'other_claims'
    END AS scope_checked

  FROM base
),

bucketed AS (
  SELECT
    *,

    CASE
      -- Very small pregame separation means the claim may be thin.
      -- This bucket only applies when the field exists.
      WHEN pregame_abs_percentile_gap IS NOT NULL
        AND pregame_abs_percentile_gap <= 3
        THEN 'tiny_pregame_gap'

      -- Low internal Core Area agreement means the claim is not well supported.
      WHEN core_area_agreement_rate IS NOT NULL
        AND core_area_agreement_rate < 0.50
        THEN 'low_core_agreement'

      -- Cleanest positive support bucket.
      WHEN two_way_context = 'supportive'
        AND two_way_edge_score >= 0.15
        THEN 'stable_two_way_support'

      -- Feature context exists, but is not cleanly supportive.
      WHEN two_way_context = 'available_mixed'
        THEN 'available_mixed_fragile'

      -- One-sided offensive support.
      -- Interesting, but not enough to speak loudly.
      WHEN offense_finish_score >= 0.15
        AND (
          defensive_suppression_score IS NULL
          OR defensive_suppression_score < 0.15
        )
        THEN 'offense_only_support'

      -- One-sided defensive support.
      -- Interesting, but not enough to speak loudly.
      WHEN defensive_suppression_score >= 0.15
        AND (
          offense_finish_score IS NULL
          OR offense_finish_score < 0.15
        )
        THEN 'defense_only_support'

      -- Direct conflict buckets.
      WHEN offense_finish_score >= 0.15
        AND defensive_suppression_score < 0
        THEN 'offense_support_defense_conflict'

      WHEN defensive_suppression_score >= 0.15
        AND offense_finish_score < 0
        THEN 'defense_support_offense_conflict'

      -- Missing scoped context.
      -- Important: do not treat this as automatically bad football.
      WHEN two_way_context = 'unavailable'
        OR two_way_context IS NULL
        THEN 'two_way_unavailable'

      ELSE 'mixed_or_neutral'
    END AS feature_bucket

  FROM scoped
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run_raw AS (
  SELECT
    'matchup_fragility_warning_v0' AS Feature_Name,
    b.run_id AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

repeatability AS (
  SELECT
    Scope_Checked,
    Feature_Bucket,

    COUNT(DISTINCT Test_Run) AS runs_present,

    COUNTIF(Row_Count >= 30) AS runs_with_enough_rows,

    COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) AS positive_lift_runs,
    COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(Row_Count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_positive'

      WHEN COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_negative'

      ELSE 'unclear'
    END AS Repeatable_Across_Runs

  FROM bucket_summary_by_run_raw
  GROUP BY
    Scope_Checked,
    Feature_Bucket
),

bucket_summary_by_run AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket = 'stable_two_way_support'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'available_mixed_fragile'
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket IN (
        'offense_only_support',
        'defense_only_support',
        'two_way_unavailable'
      )
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket IN (
        'tiny_pregame_gap',
        'low_core_agreement',
        'offense_support_defense_conflict',
        'defense_support_offense_conflict'
      )
        AND rep.Repeatable_Across_Runs IN ('yes_negative', 'unclear')
        THEN 'retest_warning'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'stable_two_way_support'
        THEN 'Stable two-way support should validate above baseline if this feature is useful.'

      WHEN r.Feature_Bucket = 'available_mixed_fragile'
        THEN 'Mixed two-way context should validate below baseline and act as language restraint.'

      WHEN r.Feature_Bucket = 'offense_only_support'
        THEN 'One-sided offensive support is interesting but incomplete; do not auto-boost.'

      WHEN r.Feature_Bucket = 'defense_only_support'
        THEN 'One-sided defensive support is interesting but incomplete; do not auto-boost.'

      WHEN r.Feature_Bucket = 'two_way_unavailable'
        THEN 'Unavailable means scoped feature context is missing; do not treat as automatically bad.'

      WHEN r.Feature_Bucket = 'tiny_pregame_gap'
        THEN 'Tiny pregame separation should usually soften language.'

      WHEN r.Feature_Bucket = 'low_core_agreement'
        THEN 'Low internal agreement suggests the claim may be thin.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_by_run_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs_raw AS (
  SELECT
    'matchup_fragility_warning_v0' AS Feature_Name,
    'ALL_SELECTED_RUNS' AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

bucket_summary_all_runs AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket = 'stable_two_way_support'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'available_mixed_fragile'
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket IN (
        'offense_only_support',
        'defense_only_support',
        'two_way_unavailable'
      )
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket IN (
        'tiny_pregame_gap',
        'low_core_agreement',
        'offense_support_defense_conflict',
        'defense_support_offense_conflict'
      )
        AND rep.Repeatable_Across_Runs IN ('yes_negative', 'unclear')
        THEN 'retest_warning'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'stable_two_way_support'
        THEN 'Stable two-way support should validate above baseline if this feature is useful.'

      WHEN r.Feature_Bucket = 'available_mixed_fragile'
        THEN 'Mixed two-way context should validate below baseline and act as language restraint.'

      WHEN r.Feature_Bucket = 'offense_only_support'
        THEN 'One-sided offensive support is interesting but incomplete; do not auto-boost.'

      WHEN r.Feature_Bucket = 'defense_only_support'
        THEN 'One-sided defensive support is interesting but incomplete; do not auto-boost.'

      WHEN r.Feature_Bucket = 'two_way_unavailable'
        THEN 'Unavailable means scoped feature context is missing; do not treat as automatically bad.'

      WHEN r.Feature_Bucket = 'tiny_pregame_gap'
        THEN 'Tiny pregame separation should usually soften language.'

      WHEN r.Feature_Bucket = 'low_core_agreement'
        THEN 'Low internal agreement suggests the claim may be thin.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_all_runs_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
)

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_by_run

UNION ALL

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN Test_Run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  Test_Run,
  Scope_Checked,
  Feature_Bucket;

  -summary

  matchup_fragility_warning_v0 survives the standardized re-test as a strong claim-language calibration feature. The cleanest and most reliable finding is that stable_two_way_support consistently improves claim validation across category, Core Area, metric, and game-profile claim scopes. This means when a claim is backed by both offensive finish and defensive suppression support, GameLens can safely allow stronger support language. The available_mixed_fragile bucket also generally performs below baseline, especially in category, Core Area, and metric claims, which makes it useful as a language-restraint signal. However, one-sided support buckets like offense_only_support and defense_only_support remain too small or too inconsistent to promote. two_way_unavailable should stay context-only because it often reflects missing scoped feature context rather than bad football. Overall decision: promote scoped, using stable two-way support as a positive language signal and mixed-fragile support as a softening/caution signal.

-easily said

Is this claim standing on two legs, or is it wobbling?

--feature 3--

-query
-- ============================================================
-- Feature 3: passing_efficiency_claim_support_v0
--
-- Football question:
-- Do passing-related claims validate better when the claimed
-- team had a pregame yards_per_pass efficiency edge?
--
-- Testing table pitfalls handled:
-- 1) Table is one row per claim, not one row per game.
-- 2) There are no clean away/home passing efficiency feature fields.
-- 3) We reconstruct a game-level passing edge from yards_per_pass rows.
-- 4) pregame_percentile_gap is used as the comparable edge magnitude.
-- 5) Unavailable means missing scoped passing context, not bad football.
-- 6) This tests claim validation, not winner prediction.
--
-- Standard output columns:
-- Feature_Name
-- Test_Run
-- Scope_Checked
-- Feature_Bucket
-- Row_Count
-- Validation_Rate
-- Lift_vs_Baseline
-- Repeatable_Across_Runs
-- Decision
-- Notes
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(pregame_percentile_gap AS FLOAT64) AS pregame_percentile_gap,
    SAFE_CAST(pregame_abs_percentile_gap AS FLOAT64) AS pregame_abs_percentile_gap

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- Rebuild one passing-efficiency edge per run/game.
-- Positive = away team had the yards_per_pass edge.
-- Negative = home team had the yards_per_pass edge.
--
-- We keep the strongest available yards_per_pass gap per game because the
-- table may contain more than one claim row touching the same metric.
passing_efficiency_edges AS (
  SELECT
    run_id,
    game_id,

    ARRAY_AGG(
      CASE
        WHEN claimed_side = 'away'
          THEN SAFE_DIVIDE(ABS(pregame_percentile_gap), 100.0)

        WHEN claimed_side = 'home'
          THEN -SAFE_DIVIDE(ABS(pregame_percentile_gap), 100.0)

        ELSE NULL
      END
      IGNORE NULLS
      ORDER BY ABS(SAFE_DIVIDE(pregame_percentile_gap, 100.0)) DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS away_signed_yards_per_pass_edge,

    COUNT(*) AS yards_per_pass_source_rows

  FROM base
  WHERE metric = 'yards_per_pass'
    AND claimed_side IN ('away', 'home')
    AND pregame_percentile_gap IS NOT NULL
  GROUP BY
    run_id,
    game_id
),

-- Separate scopes so we can see whether the feature works narrowly
-- or only looks good when we over-broaden it.
scope_rows AS (
  SELECT
    b.*,
    'yards_per_pass_metric_claims' AS scope_checked
  FROM base b
  WHERE b.metric = 'yards_per_pass'

  UNION ALL

  SELECT
    b.*,
    'passing_category_claims' AS scope_checked
  FROM base b
  WHERE b.category = 'Passing Game'

  UNION ALL

  SELECT
    b.*,
    'passing_related_claims' AS scope_checked
  FROM base b
  WHERE b.category = 'Passing Game'
     OR b.metric IN (
        'yards_per_pass',
        'passing_yards',
        'passing_tds',
        'pass_attempts',
        'pass_completions',
        'pass_play_pct',
        'pass_run_ratio'
     )
),

bucketed AS (
  SELECT
    s.*,

    e.away_signed_yards_per_pass_edge,

    CASE
      WHEN s.claimed_side = 'away'
        THEN e.away_signed_yards_per_pass_edge

      WHEN s.claimed_side = 'home'
        THEN -e.away_signed_yards_per_pass_edge

      ELSE NULL
    END AS claimed_team_yards_per_pass_edge,

    CASE
      WHEN e.away_signed_yards_per_pass_edge IS NULL
        THEN 'unavailable'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_pass_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_pass_edge
          ELSE NULL
        END
      ) >= 0.15
        THEN 'clear_passing_efficiency_support'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_pass_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_pass_edge
          ELSE NULL
        END
      ) >= 0.05
        THEN 'mild_passing_efficiency_support'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_pass_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_pass_edge
          ELSE NULL
        END
      ) <= -0.15
        THEN 'passing_efficiency_against_claim'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_pass_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_pass_edge
          ELSE NULL
        END
      ) <= -0.05
        THEN 'mild_passing_efficiency_against'

      ELSE 'near_even_passing_efficiency'
    END AS feature_bucket

  FROM scope_rows s
  LEFT JOIN passing_efficiency_edges e
    ON s.run_id = e.run_id
   AND s.game_id = e.game_id
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run_raw AS (
  SELECT
    'passing_efficiency_claim_support_v0' AS Feature_Name,
    b.run_id AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

repeatability AS (
  SELECT
    Scope_Checked,
    Feature_Bucket,

    COUNT(DISTINCT Test_Run) AS runs_present,
    COUNTIF(Row_Count >= 30) AS runs_with_enough_rows,

    COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) AS positive_lift_runs,
    COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(Row_Count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_positive'

      WHEN COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_negative'

      ELSE 'unclear'
    END AS Repeatable_Across_Runs

  FROM bucket_summary_by_run_raw
  GROUP BY
    Scope_Checked,
    Feature_Bucket
),

bucket_summary_by_run AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'clear_passing_efficiency_support',
        'mild_passing_efficiency_support'
      )
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket IN (
        'passing_efficiency_against_claim',
        'mild_passing_efficiency_against'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'near_even_passing_efficiency'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'clear_passing_efficiency_support'
        THEN 'Claimed team had a clear yards-per-pass efficiency edge. Should validate above baseline if useful.'

      WHEN r.Feature_Bucket = 'mild_passing_efficiency_support'
        THEN 'Claimed team had mild passing-efficiency support. Useful only if it repeats above baseline.'

      WHEN r.Feature_Bucket = 'passing_efficiency_against_claim'
        THEN 'Passing efficiency worked against the claim. Should validate below baseline if this is a useful warning.'

      WHEN r.Feature_Bucket = 'mild_passing_efficiency_against'
        THEN 'Mild passing-efficiency resistance. Use only as soft caution if repeatable.'

      WHEN r.Feature_Bucket = 'near_even_passing_efficiency'
        THEN 'Passing efficiency was near even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Passing-efficiency context is missing for this claim/game. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_by_run_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs_raw AS (
  SELECT
    'passing_efficiency_claim_support_v0' AS Feature_Name,
    'ALL_SELECTED_RUNS' AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

bucket_summary_all_runs AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'clear_passing_efficiency_support',
        'mild_passing_efficiency_support'
      )
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket IN (
        'passing_efficiency_against_claim',
        'mild_passing_efficiency_against'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'near_even_passing_efficiency'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'clear_passing_efficiency_support'
        THEN 'Claimed team had a clear yards-per-pass efficiency edge. Should validate above baseline if useful.'

      WHEN r.Feature_Bucket = 'mild_passing_efficiency_support'
        THEN 'Claimed team had mild passing-efficiency support. Useful only if it repeats above baseline.'

      WHEN r.Feature_Bucket = 'passing_efficiency_against_claim'
        THEN 'Passing efficiency worked against the claim. Should validate below baseline if this is a useful warning.'

      WHEN r.Feature_Bucket = 'mild_passing_efficiency_against'
        THEN 'Mild passing-efficiency resistance. Use only as soft caution if repeatable.'

      WHEN r.Feature_Bucket = 'near_even_passing_efficiency'
        THEN 'Passing efficiency was near even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Passing-efficiency context is missing for this claim/game. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_all_runs_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
)

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_by_run

UNION ALL

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN Test_Run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  Test_Run,
  Scope_Checked,
  Feature_Bucket;

  -summary
passing_efficiency_claim_support_v0 shows a useful but incomplete signal. When the claimed team had clear passing efficiency support, passing-related claims generally validated above baseline in the fresh and larger test runs, and the all-run result was positive. However, the baseline run did not confirm the pattern, so this feature should not be fully promoted yet. The stronger finding is actually negative: mild_passing_efficiency_support performed poorly in the fresh and larger runs and was strongly below baseline overall. That means weak passing efficiency should not be treated as support. It is better understood as thin or fragile evidence. The current decision is keep scoped and re-test, using clear passing efficiency as cautious support and mild passing efficiency as a softening/warning signal.

-easily said
A real yards-per-pass edge matters, but a tiny passing edge is not enough to trust.

--feature 4--
-query
-- ============================================================
-- Feature 4: defensive_resistance_support_v0
--
-- Football question:
-- Do defensive-control / defensive-suppression claims validate
-- better when the claimed team had pregame defensive resistance
-- support?
--
-- Testing table pitfalls handled:
-- 1) Table is one row per claim, not one row per game.
-- 2) defensive_suppression_score is row-perspective, not away/home global.
-- 3) Missing defensive_suppression_score means the feature was not scoped
--    to that row, not automatically bad football.
-- 4) This tests claim validation, not winner prediction.
--
-- Standard output columns:
-- Feature_Name
-- Test_Run
-- Scope_Checked
-- Feature_Bucket
-- Row_Count
-- Validation_Rate
-- Lift_vs_Baseline
-- Repeatable_Across_Runs
-- Decision
-- Notes
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(defensive_suppression_score AS FLOAT64) AS defensive_suppression_score,
    SAFE_CAST(offense_finish_score AS FLOAT64) AS offense_finish_score,
    SAFE_CAST(two_way_edge_score AS FLOAT64) AS two_way_edge_score,
    two_way_context,

    SAFE_CAST(pregame_percentile_gap AS FLOAT64) AS pregame_percentile_gap,
    SAFE_CAST(pregame_abs_percentile_gap AS FLOAT64) AS pregame_abs_percentile_gap,
    SAFE_CAST(core_area_agreement_rate AS FLOAT64) AS core_area_agreement_rate

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- Separate scopes so we can tell whether this is truly defensive,
-- or only looks useful when over-broadened.
scope_rows AS (
  SELECT
    b.*,
    'defensive_control_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Defensive Control'

  UNION ALL

  SELECT
    b.*,
    'scoring_suppression_claims' AS scope_checked
  FROM base b
  WHERE b.category IN (
      'Scoring Suppression',
      'Defensive Efficiency'
    )
    OR b.metric IN (
      'points_allowed_per_play',
      'points_allowed_per_yard',
      'yards_allowed',
      'points_allowed',
      'defensive_success_rate'
    )

  UNION ALL

  SELECT
    b.*,
    'defensive_related_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Defensive Control'
     OR b.category IN (
        'Scoring Suppression',
        'Defensive Efficiency'
     )
     OR b.metric IN (
        'points_allowed_per_play',
        'points_allowed_per_yard',
        'yards_allowed',
        'points_allowed',
        'defensive_success_rate'
     )
),

bucketed AS (
  SELECT
    *,

    CASE
      WHEN defensive_suppression_score IS NULL
        THEN 'unavailable'

      WHEN defensive_suppression_score >= 0.25
        THEN 'strong_defensive_resistance_support'

      WHEN defensive_suppression_score >= 0.15
        THEN 'clear_defensive_resistance_support'

      WHEN defensive_suppression_score >= 0.05
        THEN 'mild_defensive_resistance_support'

      WHEN defensive_suppression_score <= -0.15
        THEN 'defensive_resistance_against_claim'

      WHEN defensive_suppression_score <= -0.05
        THEN 'mild_defensive_resistance_against'

      ELSE 'near_even_defensive_resistance'
    END AS feature_bucket

  FROM scope_rows
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run_raw AS (
  SELECT
    'defensive_resistance_support_v0' AS Feature_Name,
    b.run_id AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

repeatability AS (
  SELECT
    Scope_Checked,
    Feature_Bucket,

    COUNT(DISTINCT Test_Run) AS runs_present,
    COUNTIF(Row_Count >= 30) AS runs_with_enough_rows,

    COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) AS positive_lift_runs,
    COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(Row_Count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_positive'

      WHEN COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_negative'

      ELSE 'unclear'
    END AS Repeatable_Across_Runs

  FROM bucket_summary_by_run_raw
  GROUP BY
    Scope_Checked,
    Feature_Bucket
),

bucket_summary_by_run AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'strong_defensive_resistance_support',
        'clear_defensive_resistance_support'
      )
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'mild_defensive_resistance_support'
        THEN 'retest'

      WHEN r.Feature_Bucket IN (
        'defensive_resistance_against_claim',
        'mild_defensive_resistance_against'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'near_even_defensive_resistance'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_defensive_resistance_support'
        THEN 'Claimed team had strong defensive suppression support. Should validate above baseline if useful.'

      WHEN r.Feature_Bucket = 'clear_defensive_resistance_support'
        THEN 'Claimed team had clear defensive suppression support. Good candidate for defensive claim support.'

      WHEN r.Feature_Bucket = 'mild_defensive_resistance_support'
        THEN 'Mild defensive support may be too thin. Useful only if it repeats above baseline.'

      WHEN r.Feature_Bucket = 'defensive_resistance_against_claim'
        THEN 'Defensive suppression worked against the claim. Should validate below baseline if useful as a warning.'

      WHEN r.Feature_Bucket = 'mild_defensive_resistance_against'
        THEN 'Mild defensive resistance against the claim. Use only as soft caution if repeatable.'

      WHEN r.Feature_Bucket = 'near_even_defensive_resistance'
        THEN 'Defensive suppression was near even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Defensive suppression context is missing for this row. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_by_run_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs_raw AS (
  SELECT
    'defensive_resistance_support_v0' AS Feature_Name,
    'ALL_SELECTED_RUNS' AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

bucket_summary_all_runs AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'strong_defensive_resistance_support',
        'clear_defensive_resistance_support'
      )
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'mild_defensive_resistance_support'
        THEN 'retest'

      WHEN r.Feature_Bucket IN (
        'defensive_resistance_against_claim',
        'mild_defensive_resistance_against'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'near_even_defensive_resistance'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_defensive_resistance_support'
        THEN 'Claimed team had strong defensive suppression support. Should validate above baseline if useful.'

      WHEN r.Feature_Bucket = 'clear_defensive_resistance_support'
        THEN 'Claimed team had clear defensive suppression support. Good candidate for defensive claim support.'

      WHEN r.Feature_Bucket = 'mild_defensive_resistance_support'
        THEN 'Mild defensive support may be too thin. Useful only if it repeats above baseline.'

      WHEN r.Feature_Bucket = 'defensive_resistance_against_claim'
        THEN 'Defensive suppression worked against the claim. Should validate below baseline if useful as a warning.'

      WHEN r.Feature_Bucket = 'mild_defensive_resistance_against'
        THEN 'Mild defensive resistance against the claim. Use only as soft caution if repeatable.'

      WHEN r.Feature_Bucket = 'near_even_defensive_resistance'
        THEN 'Defensive suppression was near even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Defensive suppression context is missing for this row. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_all_runs_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
)

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_by_run

UNION ALL

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN Test_Run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  Test_Run,
  Scope_Checked,
  Feature_Bucket;

  -summary
  defensive_resistance_support_v0 shows a modest but incomplete signal. The only bucket worth keeping alive is strong_defensive_resistance_support, which produced positive lift across Defensive Control, defensive-related, and scoring-suppression scopes, especially in the larger 240-game run. However, the lift was generally modest and repeatability remained unclear, so the feature should not be promoted yet. The clear_defensive_resistance_support bucket was weaker than expected and often underperformed baseline, which suggests that defensive support needs to be strong before it becomes useful. The current decision is retest, with the feature kept as supporting context only. It should not yet drive stronger language, confidence, or boost behavior.

  -easily said
  A little defensive resistance is not enough. The defense has to really show up.
--feature 5--
-query
-- ============================================================
-- Feature 5: defense_vs_dynamite_v0
--
-- Football question:
-- Do defensive claims still validate when the claimed defense
-- is facing an opponent with offensive/firepower support?
--
-- Testing table pitfalls handled:
-- 1) Table is one row per claim, not one row per game.
-- 2) defensive_suppression_score is row-perspective.
-- 3) offense_finish_score is row-perspective.
-- 4) We reconstruct opponent offensive support at game/team side level.
-- 5) Missing opponent offense context means unavailable, not bad football.
-- 6) This tests claim validation, not winner prediction.
--
-- Standard output columns:
-- Feature_Name
-- Test_Run
-- Scope_Checked
-- Feature_Bucket
-- Row_Count
-- Validation_Rate
-- Lift_vs_Baseline
-- Repeatable_Across_Runs
-- Decision
-- Notes
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(defensive_suppression_score AS FLOAT64) AS defensive_suppression_score,
    SAFE_CAST(offense_finish_score AS FLOAT64) AS offense_finish_score,
    SAFE_CAST(two_way_edge_score AS FLOAT64) AS two_way_edge_score,
    two_way_context,

    SAFE_CAST(pregame_percentile_gap AS FLOAT64) AS pregame_percentile_gap,
    SAFE_CAST(pregame_abs_percentile_gap AS FLOAT64) AS pregame_abs_percentile_gap,
    SAFE_CAST(core_area_agreement_rate AS FLOAT64) AS core_area_agreement_rate

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- Build one offensive/firepower score per team side per run/game.
-- This lets defensive claims borrow the OPPONENT'S offensive support.
--
-- AVG is intentionally used here because the table is claim-row based;
-- we are trying to summarize side-level offensive support without pretending
-- the row itself has away/home global feature fields.
side_offense_scores AS (
  SELECT
    run_id,
    game_id,
    claimed_side AS side,

    AVG(offense_finish_score) AS side_offense_finish_score,
    COUNTIF(offense_finish_score IS NOT NULL) AS offense_score_rows

  FROM base
  WHERE offense_finish_score IS NOT NULL
    AND claimed_side IN ('away', 'home')
  GROUP BY
    run_id,
    game_id,
    claimed_side
),

-- Keep this feature focused on defensive/suppression claims.
scope_rows AS (
  SELECT
    b.*,
    'defensive_control_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Defensive Control'

  UNION ALL

  SELECT
    b.*,
    'scoring_suppression_claims' AS scope_checked
  FROM base b
  WHERE b.category IN (
      'Scoring Suppression',
      'Defensive Efficiency'
    )
    OR b.metric IN (
      'points_allowed_per_play',
      'points_allowed_per_yard',
      'yards_allowed',
      'points_allowed',
      'defensive_success_rate'
    )

  UNION ALL

  SELECT
    b.*,
    'defensive_related_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Defensive Control'
     OR b.category IN (
        'Scoring Suppression',
        'Defensive Efficiency'
     )
     OR b.metric IN (
        'points_allowed_per_play',
        'points_allowed_per_yard',
        'yards_allowed',
        'points_allowed',
        'defensive_success_rate'
     )
),

bucketed AS (
  SELECT
    s.*,

    opp.side_offense_finish_score AS opponent_offense_finish_score,
    opp.offense_score_rows AS opponent_offense_score_rows,

    CASE
      WHEN s.defensive_suppression_score IS NULL
        OR opp.side_offense_finish_score IS NULL
        THEN 'unavailable'

      -- Best case: claimed defense is strong and opponent offense is not explosive.
      WHEN s.defensive_suppression_score >= 0.25
        AND opp.side_offense_finish_score < 0.15
        THEN 'defense_controls_non_dynamite'

      -- Strength-on-strength: claimed defense is strong, but opponent offense has real firepower.
      WHEN s.defensive_suppression_score >= 0.25
        AND opp.side_offense_finish_score >= 0.15
        THEN 'defense_vs_dynamite_strength_on_strength'

      -- Claimed defense has clear support, but opponent also has offensive support.
      WHEN s.defensive_suppression_score >= 0.15
        AND opp.side_offense_finish_score >= 0.15
        THEN 'defense_challenged_by_dynamite'

      -- Opponent has firepower and claimed defense does not have enough support.
      WHEN s.defensive_suppression_score < 0.15
        AND opp.side_offense_finish_score >= 0.15
        THEN 'dynamite_warning'

      -- Claimed defense has support and opponent offense is weak/negative.
      WHEN s.defensive_suppression_score >= 0.15
        AND opp.side_offense_finish_score < 0.00
        THEN 'defense_clean_path'

      -- Claimed defense is weak and opponent offense also has no firepower.
      WHEN s.defensive_suppression_score < 0.00
        AND opp.side_offense_finish_score < 0.00
        THEN 'muted_matchup_no_clear_firepower'

      -- Claimed defense is weak/negative.
      WHEN s.defensive_suppression_score < 0.00
        THEN 'defense_resistance_against_claim'

      ELSE 'mixed_or_neutral'
    END AS feature_bucket

  FROM scope_rows s
  LEFT JOIN side_offense_scores opp
    ON s.run_id = opp.run_id
   AND s.game_id = opp.game_id
   AND s.opponent_side = opp.side
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run_raw AS (
  SELECT
    'defense_vs_dynamite_v0' AS Feature_Name,
    b.run_id AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

repeatability AS (
  SELECT
    Scope_Checked,
    Feature_Bucket,

    COUNT(DISTINCT Test_Run) AS runs_present,
    COUNTIF(Row_Count >= 30) AS runs_with_enough_rows,

    COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) AS positive_lift_runs,
    COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(Row_Count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_positive'

      WHEN COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_negative'

      ELSE 'unclear'
    END AS Repeatable_Across_Runs

  FROM bucket_summary_by_run_raw
  GROUP BY
    Scope_Checked,
    Feature_Bucket
),

bucket_summary_by_run AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket = 'defense_controls_non_dynamite'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'defense_vs_dynamite_strength_on_strength'
        THEN 'retest_strength_on_strength'

      WHEN r.Feature_Bucket = 'defense_challenged_by_dynamite'
        THEN 'soften'

      WHEN r.Feature_Bucket = 'dynamite_warning'
        AND rep.Repeatable_Across_Runs IN ('yes_negative', 'unclear')
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'defense_clean_path'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket IN (
        'defense_resistance_against_claim',
        'muted_matchup_no_clear_firepower'
      )
        THEN 'retest'

      WHEN r.Feature_Bucket = 'mixed_or_neutral'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'defense_controls_non_dynamite'
        THEN 'Claimed defense has strong support and opponent offense does not show firepower.'

      WHEN r.Feature_Bucket = 'defense_vs_dynamite_strength_on_strength'
        THEN 'Strong defense meets strong opponent offense. This should soften language unless validation clearly holds.'

      WHEN r.Feature_Bucket = 'defense_challenged_by_dynamite'
        THEN 'Defense has support, but opponent offensive firepower challenges the claim.'

      WHEN r.Feature_Bucket = 'dynamite_warning'
        THEN 'Opponent offense has firepower and claimed defense lacks enough resistance. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'defense_clean_path'
        THEN 'Defense has support and opponent offense is weak. Candidate support bucket.'

      WHEN r.Feature_Bucket = 'defense_resistance_against_claim'
        THEN 'Defensive support works against the claim. Review as possible warning.'

      WHEN r.Feature_Bucket = 'muted_matchup_no_clear_firepower'
        THEN 'Neither side creates a clean defensive/firepower read.'

      WHEN r.Feature_Bucket = 'mixed_or_neutral'
        THEN 'No clean defense-vs-firepower read.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Opponent offense or defensive support context is missing. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_by_run_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs_raw AS (
  SELECT
    'defense_vs_dynamite_v0' AS Feature_Name,
    'ALL_SELECTED_RUNS' AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

bucket_summary_all_runs AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket = 'defense_controls_non_dynamite'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'defense_vs_dynamite_strength_on_strength'
        THEN 'retest_strength_on_strength'

      WHEN r.Feature_Bucket = 'defense_challenged_by_dynamite'
        THEN 'soften'

      WHEN r.Feature_Bucket = 'dynamite_warning'
        AND rep.Repeatable_Across_Runs IN ('yes_negative', 'unclear')
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'defense_clean_path'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket IN (
        'defense_resistance_against_claim',
        'muted_matchup_no_clear_firepower'
      )
        THEN 'retest'

      WHEN r.Feature_Bucket = 'mixed_or_neutral'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'defense_controls_non_dynamite'
        THEN 'Claimed defense has strong support and opponent offense does not show firepower.'

      WHEN r.Feature_Bucket = 'defense_vs_dynamite_strength_on_strength'
        THEN 'Strong defense meets strong opponent offense. This should soften language unless validation clearly holds.'

      WHEN r.Feature_Bucket = 'defense_challenged_by_dynamite'
        THEN 'Defense has support, but opponent offensive firepower challenges the claim.'

      WHEN r.Feature_Bucket = 'dynamite_warning'
        THEN 'Opponent offense has firepower and claimed defense lacks enough resistance. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'defense_clean_path'
        THEN 'Defense has support and opponent offense is weak. Candidate support bucket.'

      WHEN r.Feature_Bucket = 'defense_resistance_against_claim'
        THEN 'Defensive support works against the claim. Review as possible warning.'

      WHEN r.Feature_Bucket = 'muted_matchup_no_clear_firepower'
        THEN 'Neither side creates a clean defensive/firepower read.'

      WHEN r.Feature_Bucket = 'mixed_or_neutral'
        THEN 'No clean defense-vs-firepower read.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Opponent offense or defensive support context is missing. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_all_runs_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
)

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_by_run

UNION ALL

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN Test_Run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  Test_Run,
  Scope_Checked,
  Feature_Bucket;

  -summary
  defense_vs_dynamite_v0 produced one of the cleaner feature reads so far. The strongest bucket is defense_controls_non_dynamite, where the claimed defense had strong support and the opponent offense did not show matching firepower. That bucket validated well above baseline across Defensive Control, defensive-related, and scoring-suppression scopes, and it repeated across all three test runs. The opposite matchup, defense_vs_dynamite_strength_on_strength, generally reduced or softened validation, especially in defensive-related and scoring-suppression claims. This means the feature should be promoted in a scoped way: it can support defensive language when the opponent offense does not have real “dynamite,” but it should soften defensive claims when a strong defense is facing a strong offense. Small buckets like dynamite_warning, defense_clean_path, and defense_challenged_by_dynamite are not ready because row counts were too low.

  -easily said
  A strong defense looks trustworthy when the other offense does not have enough firepower to stress it.

--feature 6--

-query
-- ============================================================
-- Feature 6: explosive_offense_warning_v0
--
-- Football question:
-- When the opponent has real offensive firepower, do claims
-- against that opponent become more fragile?
--
-- Testing table pitfalls handled:
-- 1) Table is one row per claim, not one row per game.
-- 2) offense_finish_score is row-perspective, not away/home global.
-- 3) We reconstruct opponent offensive firepower at game/team side level.
-- 4) Missing opponent offense context means unavailable, not bad football.
-- 5) This tests claim validation, not winner prediction.
--
-- Standard output columns:
-- Feature_Name
-- Test_Run
-- Scope_Checked
-- Feature_Bucket
-- Row_Count
-- Validation_Rate
-- Lift_vs_Baseline
-- Repeatable_Across_Runs
-- Decision
-- Notes
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(offense_finish_score AS FLOAT64) AS offense_finish_score,
    SAFE_CAST(defensive_suppression_score AS FLOAT64) AS defensive_suppression_score,
    SAFE_CAST(two_way_edge_score AS FLOAT64) AS two_way_edge_score,
    two_way_context,

    SAFE_CAST(pregame_percentile_gap AS FLOAT64) AS pregame_percentile_gap,
    SAFE_CAST(pregame_abs_percentile_gap AS FLOAT64) AS pregame_abs_percentile_gap,
    SAFE_CAST(core_area_agreement_rate AS FLOAT64) AS core_area_agreement_rate

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- Build one offensive-firepower score per team side per run/game.
-- AVG is used because this is a claim-row table, not a game/team feature table.
side_offense_scores AS (
  SELECT
    run_id,
    game_id,
    claimed_side AS side,

    AVG(offense_finish_score) AS side_offense_finish_score,
    COUNTIF(offense_finish_score IS NOT NULL) AS offense_score_rows

  FROM base
  WHERE offense_finish_score IS NOT NULL
    AND claimed_side IN ('away', 'home')
  GROUP BY
    run_id,
    game_id,
    claimed_side
),

-- Test several scopes separately so we do not over-broaden the signal.
scope_rows AS (
  SELECT
    b.*,
    'defensive_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Defensive Control'
     OR b.category IN (
        'Scoring Suppression',
        'Defensive Efficiency'
     )
     OR b.metric IN (
        'points_allowed_per_play',
        'points_allowed_per_yard',
        'yards_allowed',
        'points_allowed',
        'defensive_success_rate'
     )

  UNION ALL

  SELECT
    b.*,
    'game_profile_claims' AS scope_checked
  FROM base b
  WHERE b.claim_type = 'game_profile'

  UNION ALL

  SELECT
    b.*,
    'core_area_claims' AS scope_checked
  FROM base b
  WHERE b.claim_type IN (
    'core_area_comparison',
    'core_area_summary'
  )

  UNION ALL

  SELECT
    b.*,
    'metric_claims' AS scope_checked
  FROM base b
  WHERE b.claim_type IN (
    'metric_highlight',
    'team_comparison_metric'
  )

  UNION ALL

  SELECT
    b.*,
    'all_directional_claims' AS scope_checked
  FROM base b
  WHERE b.claim_type IN (
    'game_profile',
    'core_area_comparison',
    'core_area_summary',
    'category_summary',
    'metric_highlight',
    'team_comparison_metric'
  )
),

bucketed AS (
  SELECT
    s.*,

    opp.side_offense_finish_score AS opponent_offense_finish_score,
    opp.offense_score_rows AS opponent_offense_score_rows,

    CASE
      WHEN opp.side_offense_finish_score IS NULL
        THEN 'unavailable'

      -- Biggest warning: opponent offense has strong firepower and claimed row
      -- does not have strong two-way support.
      WHEN opp.side_offense_finish_score >= 0.25
        AND (
          s.two_way_context IS NULL
          OR s.two_way_context != 'supportive'
          OR s.two_way_edge_score < 0.15
        )
        THEN 'explosive_opponent_warning'

      -- Strength-on-strength: opponent offense is explosive, but claimed team
      -- also has stable two-way support.
      WHEN opp.side_offense_finish_score >= 0.25
        AND s.two_way_context = 'supportive'
        AND s.two_way_edge_score >= 0.15
        THEN 'explosive_opponent_vs_stable_support'

      -- Opponent has clear firepower, but not explosive enough for strongest bucket.
      WHEN opp.side_offense_finish_score >= 0.15
        AND (
          s.two_way_context IS NULL
          OR s.two_way_context != 'supportive'
          OR s.two_way_edge_score < 0.15
        )
        THEN 'clear_opponent_firepower_warning'

      -- Claimed team has stable support despite opponent firepower.
      WHEN opp.side_offense_finish_score >= 0.15
        AND s.two_way_context = 'supportive'
        AND s.two_way_edge_score >= 0.15
        THEN 'opponent_firepower_but_claim_supported'

      -- Mild opponent firepower should not be treated as a hard warning.
      WHEN opp.side_offense_finish_score >= 0.05
        THEN 'mild_opponent_firepower'

      -- No opponent offensive firepower.
      WHEN opp.side_offense_finish_score < 0.05
        THEN 'no_opponent_firepower'

      ELSE 'mixed_or_neutral'
    END AS feature_bucket

  FROM scope_rows s
  LEFT JOIN side_offense_scores opp
    ON s.run_id = opp.run_id
   AND s.game_id = opp.game_id
   AND s.opponent_side = opp.side
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run_raw AS (
  SELECT
    'explosive_offense_warning_v0' AS Feature_Name,
    b.run_id AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

repeatability AS (
  SELECT
    Scope_Checked,
    Feature_Bucket,

    COUNT(DISTINCT Test_Run) AS runs_present,
    COUNTIF(Row_Count >= 30) AS runs_with_enough_rows,

    COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) AS positive_lift_runs,
    COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(Row_Count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_positive'

      WHEN COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_negative'

      ELSE 'unclear'
    END AS Repeatable_Across_Runs

  FROM bucket_summary_by_run_raw
  GROUP BY
    Scope_Checked,
    Feature_Bucket
),

bucket_summary_by_run AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'explosive_opponent_warning',
        'clear_opponent_firepower_warning'
      )
        AND rep.Repeatable_Across_Runs IN ('yes_negative', 'unclear')
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket IN (
        'explosive_opponent_vs_stable_support',
        'opponent_firepower_but_claim_supported'
      )
        THEN 'soften'

      WHEN r.Feature_Bucket = 'mild_opponent_firepower'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'no_opponent_firepower'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'explosive_opponent_warning'
        THEN 'Opponent has explosive offensive firepower and claim lacks stable support. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'clear_opponent_firepower_warning'
        THEN 'Opponent has clear firepower and claim lacks stable support. Candidate soft warning.'

      WHEN r.Feature_Bucket = 'explosive_opponent_vs_stable_support'
        THEN 'Opponent offense is explosive, but claim also has stable support. Strength-on-strength; soften language.'

      WHEN r.Feature_Bucket = 'opponent_firepower_but_claim_supported'
        THEN 'Opponent has firepower, but claim has stable support. Do not overstate either side.'

      WHEN r.Feature_Bucket = 'mild_opponent_firepower'
        THEN 'Mild opponent firepower is context only; not enough for a warning by itself.'

      WHEN r.Feature_Bucket = 'no_opponent_firepower'
        THEN 'Opponent offense does not show firepower. Claims may be cleaner if validation lifts.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Opponent offensive context is missing. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_by_run_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs_raw AS (
  SELECT
    'explosive_offense_warning_v0' AS Feature_Name,
    'ALL_SELECTED_RUNS' AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

bucket_summary_all_runs AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'explosive_opponent_warning',
        'clear_opponent_firepower_warning'
      )
        AND rep.Repeatable_Across_Runs IN ('yes_negative', 'unclear')
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket IN (
        'explosive_opponent_vs_stable_support',
        'opponent_firepower_but_claim_supported'
      )
        THEN 'soften'

      WHEN r.Feature_Bucket = 'mild_opponent_firepower'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'no_opponent_firepower'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'explosive_opponent_warning'
        THEN 'Opponent has explosive offensive firepower and claim lacks stable support. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'clear_opponent_firepower_warning'
        THEN 'Opponent has clear firepower and claim lacks stable support. Candidate soft warning.'

      WHEN r.Feature_Bucket = 'explosive_opponent_vs_stable_support'
        THEN 'Opponent offense is explosive, but claim also has stable support. Strength-on-strength; soften language.'

      WHEN r.Feature_Bucket = 'opponent_firepower_but_claim_supported'
        THEN 'Opponent has firepower, but claim has stable support. Do not overstate either side.'

      WHEN r.Feature_Bucket = 'mild_opponent_firepower'
        THEN 'Mild opponent firepower is context only; not enough for a warning by itself.'

      WHEN r.Feature_Bucket = 'no_opponent_firepower'
        THEN 'Opponent offense does not show firepower. Claims may be cleaner if validation lifts.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Opponent offensive context is missing. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_all_runs_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
)

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_by_run

UNION ALL

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN Test_Run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  Test_Run,
  Scope_Checked,
  Feature_Bucket;

  -summary
  explosive_offense_warning_v0 shows a useful scoped warning signal. The strongest finding is that explosive_opponent_warning consistently reduced claim validation, especially in defensive claims and Core Area claims. This means when GameLens makes a claim against a team with real offensive firepower, and the claim does not have stable support, the language should be softened. The cleanest positive contrast is in defensive claims: when the opponent did not show offensive firepower, defensive claims validated better. However, the broader all-directional scope is noisier, and clear_opponent_firepower_warning is not reliable enough to act as a hard rule. The current decision is promote scoped as a caution/language-restraint feature, mainly for defensive and Core Area claims.

  -easily said
  If the other team has real offensive firepower, don’t talk like your defensive read is clean unless the support is very strong.
--feature 7--

-query
-- ============================================================
-- Feature 7: scoring_claim_support_v0
--
-- Football question:
-- Do scoring / finishing claims validate better when the claimed
-- team has real scoring-support evidence?
--
-- Testing table pitfalls handled:
-- 1) Table is one row per claim, not one row per game.
-- 2) offense_finish_score is row-perspective, not global away/home.
-- 3) This is testing claimed-team scoring support only.
-- 4) Opponent defensive resistance is NOT included here;
--    that was tested in offense_vs_defense_collision_v0.
-- 5) Missing offense_finish_score means unavailable, not bad football.
-- 6) This tests claim validation, not winner prediction.
--
-- Standard output columns:
-- Feature_Name
-- Test_Run
-- Scope_Checked
-- Feature_Bucket
-- Row_Count
-- Validation_Rate
-- Lift_vs_Baseline
-- Repeatable_Across_Runs
-- Decision
-- Notes
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(offense_finish_score AS FLOAT64) AS offense_finish_score,
    SAFE_CAST(defensive_suppression_score AS FLOAT64) AS defensive_suppression_score,
    SAFE_CAST(two_way_edge_score AS FLOAT64) AS two_way_edge_score,
    two_way_context,

    SAFE_CAST(pregame_percentile_gap AS FLOAT64) AS pregame_percentile_gap,
    SAFE_CAST(pregame_abs_percentile_gap AS FLOAT64) AS pregame_abs_percentile_gap,
    SAFE_CAST(core_area_agreement_rate AS FLOAT64) AS core_area_agreement_rate

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- Separate scopes so we can tell whether scoring support works
-- narrowly for scoring claims or only looks useful when over-broadened.
scope_rows AS (
  SELECT
    b.*,
    'scoring_efficiency_core_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Scoring Efficiency'

  UNION ALL

  SELECT
    b.*,
    'scoring_category_claims' AS scope_checked
  FROM base b
  WHERE b.category IN (
      'Drive Conversion',
      'Red Zone Finish',
      'Scoring Production',
      'Scoring Efficiency'
    )

  UNION ALL

  SELECT
    b.*,
    'scoring_metric_claims' AS scope_checked
  FROM base b
  WHERE b.metric IN (
      'points_per_play',
      'points_per_yard',
      'points',
      'td_rate',
      'touchdown_rate',
      'red_zone_efficiency',
      'third_down_pct',
      '1st_down_rate',
      'first_down_rate',
      'drive_success_rate'
    )

  UNION ALL

  SELECT
    b.*,
    'scoring_related_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Scoring Efficiency'
     OR b.category IN (
        'Drive Conversion',
        'Red Zone Finish',
        'Scoring Production',
        'Scoring Efficiency'
     )
     OR b.metric IN (
        'points_per_play',
        'points_per_yard',
        'points',
        'td_rate',
        'touchdown_rate',
        'red_zone_efficiency',
        'third_down_pct',
        '1st_down_rate',
        'first_down_rate',
        'drive_success_rate'
     )
),

bucketed AS (
  SELECT
    *,

    CASE
      WHEN offense_finish_score IS NULL
        THEN 'unavailable'

      WHEN offense_finish_score >= 0.25
        THEN 'strong_scoring_support'

      WHEN offense_finish_score >= 0.15
        THEN 'clear_scoring_support'

      WHEN offense_finish_score >= 0.05
        THEN 'mild_scoring_support'

      WHEN offense_finish_score <= -0.15
        THEN 'scoring_support_against_claim'

      WHEN offense_finish_score <= -0.05
        THEN 'mild_scoring_against_claim'

      ELSE 'near_even_scoring_support'
    END AS feature_bucket

  FROM scope_rows
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run_raw AS (
  SELECT
    'scoring_claim_support_v0' AS Feature_Name,
    b.run_id AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

repeatability AS (
  SELECT
    Scope_Checked,
    Feature_Bucket,

    COUNT(DISTINCT Test_Run) AS runs_present,
    COUNTIF(Row_Count >= 30) AS runs_with_enough_rows,

    COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) AS positive_lift_runs,
    COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(Row_Count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_positive'

      WHEN COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_negative'

      ELSE 'unclear'
    END AS Repeatable_Across_Runs

  FROM bucket_summary_by_run_raw
  GROUP BY
    Scope_Checked,
    Feature_Bucket
),

bucket_summary_by_run AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'strong_scoring_support',
        'clear_scoring_support'
      )
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'mild_scoring_support'
        THEN 'retest'

      WHEN r.Feature_Bucket IN (
        'scoring_support_against_claim',
        'mild_scoring_against_claim'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'near_even_scoring_support'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_scoring_support'
        THEN 'Claimed team had strong scoring/finish support. Should validate above baseline if useful.'

      WHEN r.Feature_Bucket = 'clear_scoring_support'
        THEN 'Claimed team had clear scoring/finish support. Candidate scoring-language support.'

      WHEN r.Feature_Bucket = 'mild_scoring_support'
        THEN 'Mild scoring support may be too thin. Useful only if it repeats above baseline.'

      WHEN r.Feature_Bucket = 'scoring_support_against_claim'
        THEN 'Scoring support worked against the claim. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'mild_scoring_against_claim'
        THEN 'Mild scoring resistance against the claim. Use only as soft caution if repeatable.'

      WHEN r.Feature_Bucket = 'near_even_scoring_support'
        THEN 'Scoring support was near even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Scoring support context is missing for this row. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_by_run_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs_raw AS (
  SELECT
    'scoring_claim_support_v0' AS Feature_Name,
    'ALL_SELECTED_RUNS' AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

bucket_summary_all_runs AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'strong_scoring_support',
        'clear_scoring_support'
      )
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'mild_scoring_support'
        THEN 'retest'

      WHEN r.Feature_Bucket IN (
        'scoring_support_against_claim',
        'mild_scoring_against_claim'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'near_even_scoring_support'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_scoring_support'
        THEN 'Claimed team had strong scoring/finish support. Should validate above baseline if useful.'

      WHEN r.Feature_Bucket = 'clear_scoring_support'
        THEN 'Claimed team had clear scoring/finish support. Candidate scoring-language support.'

      WHEN r.Feature_Bucket = 'mild_scoring_support'
        THEN 'Mild scoring support may be too thin. Useful only if it repeats above baseline.'

      WHEN r.Feature_Bucket = 'scoring_support_against_claim'
        THEN 'Scoring support worked against the claim. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'mild_scoring_against_claim'
        THEN 'Mild scoring resistance against the claim. Use only as soft caution if repeatable.'

      WHEN r.Feature_Bucket = 'near_even_scoring_support'
        THEN 'Scoring support was near even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Scoring support context is missing for this row. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_all_runs_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
)

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_by_run

UNION ALL

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN Test_Run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  Test_Run,
  Scope_Checked,
  Feature_Bucket;

  -summary
  scoring_claim_support_v0 passed revalidation as a scoped scoring-language feature, but only at the strong-support level. The strongest and most repeatable finding is that strong_scoring_support lifted claim validation across scoring category claims, Scoring Efficiency core claims, scoring metric claims, and broader scoring-related claims. The warning side also worked: scoring_support_against_claim was consistently below baseline in category, metric, and related scopes. The surprise finding is that clear_scoring_support and mild_scoring_support should not be promoted; they were often below baseline or inconsistent. The product decision is promote scoped, but only allow stronger scoring language when support is truly strong. Do not boost scoring claims from mild or merely clear scoring support.

  -easily said
  A team needs a real scoring engine, not just a little scoring signal.

--feature 8--

-query
-- ============================================================
-- Feature 8: drive_conversion_scoring_support_v0
--
-- Football question:
-- Do drive-conversion / scoring-finish claims validate better
-- when the claimed team has support from conversion and finish
-- indicators like 1st down rate, third down rate, red zone finish,
-- TD rate, and points per play?
--
-- Important naming caution:
-- This is NOT true drive sustainability yet.
-- It is conversion + scoring support using available claim-table fields.
--
-- Testing table pitfalls handled:
-- 1) Table is one row per claim, not one row per game.
-- 2) There are no clean away/home drive-conversion feature fields.
-- 3) We reconstruct side-level conversion/scoring support from available
--    metric rows using pregame_percentile_gap.
-- 4) Missing context means unavailable, not bad football.
-- 5) This tests claim validation, not winner prediction.
--
-- Standard output columns:
-- Feature_Name
-- Test_Run
-- Scope_Checked
-- Feature_Bucket
-- Row_Count
-- Validation_Rate
-- Lift_vs_Baseline
-- Repeatable_Across_Runs
-- Decision
-- Notes
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(offense_finish_score AS FLOAT64) AS offense_finish_score,
    SAFE_CAST(defensive_suppression_score AS FLOAT64) AS defensive_suppression_score,
    SAFE_CAST(two_way_edge_score AS FLOAT64) AS two_way_edge_score,
    two_way_context,

    SAFE_CAST(pregame_percentile_gap AS FLOAT64) AS pregame_percentile_gap,
    SAFE_CAST(pregame_abs_percentile_gap AS FLOAT64) AS pregame_abs_percentile_gap,
    SAFE_CAST(core_area_agreement_rate AS FLOAT64) AS core_area_agreement_rate

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- Build side-level conversion/scoring support from available metric rows.
-- Positive = away side has the edge.
-- Negative = home side has the edge.
--
-- We use percentile gap because it is the comparable signal available
-- across claim rows.
--
-- This is intentionally limited to conversion / finish metrics,
-- not all offensive metrics.
side_conversion_scores AS (
  SELECT
    run_id,
    game_id,
    claimed_side AS side,

    AVG(
      CASE
        WHEN claimed_side = 'away'
          THEN SAFE_DIVIDE(ABS(pregame_percentile_gap), 100.0)

        WHEN claimed_side = 'home'
          THEN SAFE_DIVIDE(ABS(pregame_percentile_gap), 100.0)

        ELSE NULL
      END
    ) AS side_conversion_finish_score,

    COUNT(*) AS conversion_source_rows

  FROM base
  WHERE metric IN (
      '1st_down_rate',
      'first_down_rate',
      'third_down_pct',
      'red_zone_efficiency',
      'td_rate',
      'touchdown_rate',
      'points_per_play',
      'points_per_yard'
    )
    AND claimed_side IN ('away', 'home')
    AND pregame_percentile_gap IS NOT NULL
  GROUP BY
    run_id,
    game_id,
    claimed_side
),

-- Keep the feature focused on conversion / finish claims.
-- Separate scopes let us see if the feature only works narrowly.
scope_rows AS (
  SELECT
    b.*,
    'drive_conversion_category_claims' AS scope_checked
  FROM base b
  WHERE b.category IN (
      'Drive Conversion',
      'Red Zone Finish'
    )

  UNION ALL

  SELECT
    b.*,
    'conversion_finish_metric_claims' AS scope_checked
  FROM base b
  WHERE b.metric IN (
      '1st_down_rate',
      'first_down_rate',
      'third_down_pct',
      'red_zone_efficiency',
      'td_rate',
      'touchdown_rate',
      'points_per_play',
      'points_per_yard'
    )

  UNION ALL

  SELECT
    b.*,
    'scoring_efficiency_core_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Scoring Efficiency'

  UNION ALL

  SELECT
    b.*,
    'conversion_scoring_related_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Scoring Efficiency'
     OR b.category IN (
        'Drive Conversion',
        'Red Zone Finish',
        'Scoring Production',
        'Scoring Efficiency'
     )
     OR b.metric IN (
        '1st_down_rate',
        'first_down_rate',
        'third_down_pct',
        'red_zone_efficiency',
        'td_rate',
        'touchdown_rate',
        'points_per_play',
        'points_per_yard'
     )
),

bucketed AS (
  SELECT
    s.*,

    conv.side_conversion_finish_score AS claimed_team_conversion_finish_score,
    conv.conversion_source_rows,

    CASE
      WHEN conv.side_conversion_finish_score IS NULL
        THEN 'unavailable'

      WHEN conv.conversion_source_rows < 2
        THEN 'thin_conversion_evidence'

      WHEN conv.side_conversion_finish_score >= 0.25
        THEN 'strong_conversion_scoring_support'

      WHEN conv.side_conversion_finish_score >= 0.15
        THEN 'clear_conversion_scoring_support'

      WHEN conv.side_conversion_finish_score >= 0.05
        THEN 'mild_conversion_scoring_support'

      WHEN conv.side_conversion_finish_score < 0.05
        THEN 'weak_or_near_even_conversion_support'

      ELSE 'mixed_or_neutral'
    END AS feature_bucket

  FROM scope_rows s
  LEFT JOIN side_conversion_scores conv
    ON s.run_id = conv.run_id
   AND s.game_id = conv.game_id
   AND s.claimed_side = conv.side
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run_raw AS (
  SELECT
    'drive_conversion_scoring_support_v0' AS Feature_Name,
    b.run_id AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

repeatability AS (
  SELECT
    Scope_Checked,
    Feature_Bucket,

    COUNT(DISTINCT Test_Run) AS runs_present,
    COUNTIF(Row_Count >= 30) AS runs_with_enough_rows,

    COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) AS positive_lift_runs,
    COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(Row_Count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_positive'

      WHEN COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_negative'

      ELSE 'unclear'
    END AS Repeatable_Across_Runs

  FROM bucket_summary_by_run_raw
  GROUP BY
    Scope_Checked,
    Feature_Bucket
),

bucket_summary_by_run AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket = 'strong_conversion_scoring_support'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'clear_conversion_scoring_support'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep_scoped'

      WHEN r.Feature_Bucket IN (
        'mild_conversion_scoring_support',
        'weak_or_near_even_conversion_support',
        'thin_conversion_evidence'
      )
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_conversion_scoring_support'
        THEN 'Claimed team had strong conversion/finish support. Candidate positive language-support bucket.'

      WHEN r.Feature_Bucket = 'clear_conversion_scoring_support'
        THEN 'Claimed team had clear conversion/finish support. Useful only if lift repeats.'

      WHEN r.Feature_Bucket = 'mild_conversion_scoring_support'
        THEN 'Mild conversion support may be too thin. Do not boost from this alone.'

      WHEN r.Feature_Bucket = 'weak_or_near_even_conversion_support'
        THEN 'Conversion/finish support was weak or near-even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'thin_conversion_evidence'
        THEN 'Only one conversion/finish source row found. Treat as thin evidence, not a signal.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Conversion/finish context is missing for this row. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_by_run_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs_raw AS (
  SELECT
    'drive_conversion_scoring_support_v0' AS Feature_Name,
    'ALL_SELECTED_RUNS' AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

bucket_summary_all_runs AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket = 'strong_conversion_scoring_support'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'clear_conversion_scoring_support'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep_scoped'

      WHEN r.Feature_Bucket IN (
        'mild_conversion_scoring_support',
        'weak_or_near_even_conversion_support',
        'thin_conversion_evidence'
      )
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_conversion_scoring_support'
        THEN 'Claimed team had strong conversion/finish support. Candidate positive language-support bucket.'

      WHEN r.Feature_Bucket = 'clear_conversion_scoring_support'
        THEN 'Claimed team had clear conversion/finish support. Useful only if lift repeats.'

      WHEN r.Feature_Bucket = 'mild_conversion_scoring_support'
        THEN 'Mild conversion support may be too thin. Do not boost from this alone.'

      WHEN r.Feature_Bucket = 'weak_or_near_even_conversion_support'
        THEN 'Conversion/finish support was weak or near-even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'thin_conversion_evidence'
        THEN 'Only one conversion/finish source row found. Treat as thin evidence, not a signal.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Conversion/finish context is missing for this row. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_all_runs_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
)

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_by_run

UNION ALL

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN Test_Run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  Test_Run,
  Scope_Checked,
  Feature_Bucket;
-summary
drive_conversion_scoring_support_v0 did not pass as a clean feature. The strongest bucket, strong_conversion_scoring_support, showed modest positive lift across conversion-finish, conversion-scoring-related, drive-conversion, and Scoring Efficiency scopes, but repeatability stayed unclear and the lift was weaker than Feature 7’s strong_scoring_support. The bigger finding is negative: clear_conversion_scoring_support and mild_conversion_scoring_support consistently underperformed baseline, which means partial conversion support should not be treated as a positive signal. This feature should remain research-only / retest, and the naming caution still matters: this is not true drive sustainability, only conversion-plus-finish context from the claim table.
-easily said
Converting drives helps, but this version of the feature is not strong enough to trust yet.
--feature 9--

-query
-- ============================================================
-- Feature 9: rush_claim_support_score
--
-- Football question:
-- Do rushing-related claims validate better when the claimed
-- team had a real pregame rushing efficiency edge?
--
-- Important scope note:
-- This is a narrow rushing-claim support feature.
-- It is NOT broad rushing control, game control, or winner prediction.
--
-- Testing table pitfalls handled:
-- 1) Table is one row per claim, not one row per game.
-- 2) There are no clean away/home rushing-support feature fields.
-- 3) We reconstruct game-level rushing efficiency edge from yards_per_rush rows.
-- 4) pregame_percentile_gap is used as the comparable edge magnitude.
-- 5) Unavailable means missing scoped rushing context, not bad football.
-- 6) This tests claim validation, not winner prediction.
--
-- Standard output columns:
-- Feature_Name
-- Test_Run
-- Scope_Checked
-- Feature_Bucket
-- Row_Count
-- Validation_Rate
-- Lift_vs_Baseline
-- Repeatable_Across_Runs
-- Decision
-- Notes
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(pregame_percentile_gap AS FLOAT64) AS pregame_percentile_gap,
    SAFE_CAST(pregame_abs_percentile_gap AS FLOAT64) AS pregame_abs_percentile_gap,
    SAFE_CAST(offense_finish_score AS FLOAT64) AS offense_finish_score,
    SAFE_CAST(defensive_suppression_score AS FLOAT64) AS defensive_suppression_score,
    SAFE_CAST(two_way_edge_score AS FLOAT64) AS two_way_edge_score,
    two_way_context,
    SAFE_CAST(core_area_agreement_rate AS FLOAT64) AS core_area_agreement_rate

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- Rebuild one rushing-efficiency edge per run/game.
-- Positive = away team had the yards_per_rush edge.
-- Negative = home team had the yards_per_rush edge.
--
-- We use yards_per_rush because it is a cleaner rushing-efficiency
-- signal than raw rushing volume.
rush_efficiency_edges AS (
  SELECT
    run_id,
    game_id,

    ARRAY_AGG(
      CASE
        WHEN claimed_side = 'away'
          THEN SAFE_DIVIDE(ABS(pregame_percentile_gap), 100.0)

        WHEN claimed_side = 'home'
          THEN -SAFE_DIVIDE(ABS(pregame_percentile_gap), 100.0)

        ELSE NULL
      END
      IGNORE NULLS
      ORDER BY ABS(SAFE_DIVIDE(pregame_percentile_gap, 100.0)) DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS away_signed_yards_per_rush_edge,

    COUNT(*) AS yards_per_rush_source_rows

  FROM base
  WHERE metric = 'yards_per_rush'
    AND claimed_side IN ('away', 'home')
    AND pregame_percentile_gap IS NOT NULL
  GROUP BY
    run_id,
    game_id
),

-- Keep this narrow.
-- We test rushing category, yards_per_rush metric, and broader
-- rushing-related claims separately.
scope_rows AS (
  SELECT
    b.*,
    'yards_per_rush_metric_claims' AS scope_checked
  FROM base b
  WHERE b.metric = 'yards_per_rush'

  UNION ALL

  SELECT
    b.*,
    'rushing_category_claims' AS scope_checked
  FROM base b
  WHERE b.category = 'Rushing Game'

  UNION ALL

  SELECT
    b.*,
    'rushing_related_claims' AS scope_checked
  FROM base b
  WHERE b.category = 'Rushing Game'
     OR b.metric IN (
        'yards_per_rush',
        'rushing_yards',
        'rush_yards',
        'rush_attempts',
        'rushing_attempts',
        'rush_tds',
        'rushing_tds',
        'rush_td_rate',
        'explosive_rush_rate'
     )
),

bucketed AS (
  SELECT
    s.*,

    e.away_signed_yards_per_rush_edge,

    CASE
      WHEN s.claimed_side = 'away'
        THEN e.away_signed_yards_per_rush_edge

      WHEN s.claimed_side = 'home'
        THEN -e.away_signed_yards_per_rush_edge

      ELSE NULL
    END AS claimed_team_yards_per_rush_edge,

    CASE
      WHEN e.away_signed_yards_per_rush_edge IS NULL
        THEN 'unavailable'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_rush_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_rush_edge
          ELSE NULL
        END
      ) >= 0.25
        THEN 'strong_rush_efficiency_support'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_rush_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_rush_edge
          ELSE NULL
        END
      ) >= 0.15
        THEN 'clear_rush_efficiency_support'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_rush_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_rush_edge
          ELSE NULL
        END
      ) >= 0.05
        THEN 'mild_rush_efficiency_support'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_rush_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_rush_edge
          ELSE NULL
        END
      ) <= -0.25
        THEN 'strong_rush_efficiency_against_claim'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_rush_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_rush_edge
          ELSE NULL
        END
      ) <= -0.15
        THEN 'clear_rush_efficiency_against_claim'

      WHEN (
        CASE
          WHEN s.claimed_side = 'away' THEN e.away_signed_yards_per_rush_edge
          WHEN s.claimed_side = 'home' THEN -e.away_signed_yards_per_rush_edge
          ELSE NULL
        END
      ) <= -0.05
        THEN 'mild_rush_efficiency_against_claim'

      ELSE 'near_even_rush_efficiency'
    END AS feature_bucket

  FROM scope_rows s
  LEFT JOIN rush_efficiency_edges e
    ON s.run_id = e.run_id
   AND s.game_id = e.game_id
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run_raw AS (
  SELECT
    'rush_claim_support_score' AS Feature_Name,
    b.run_id AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

repeatability AS (
  SELECT
    Scope_Checked,
    Feature_Bucket,

    COUNT(DISTINCT Test_Run) AS runs_present,
    COUNTIF(Row_Count >= 30) AS runs_with_enough_rows,

    COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) AS positive_lift_runs,
    COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(Row_Count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_positive'

      WHEN COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_negative'

      ELSE 'unclear'
    END AS Repeatable_Across_Runs

  FROM bucket_summary_by_run_raw
  GROUP BY
    Scope_Checked,
    Feature_Bucket
),

bucket_summary_by_run AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'strong_rush_efficiency_support',
        'clear_rush_efficiency_support'
      )
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'mild_rush_efficiency_support'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket IN (
        'strong_rush_efficiency_against_claim',
        'clear_rush_efficiency_against_claim'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'mild_rush_efficiency_against_claim'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'near_even_rush_efficiency'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_rush_efficiency_support'
        THEN 'Claimed team had strong yards-per-rush support. Candidate rushing-language support.'

      WHEN r.Feature_Bucket = 'clear_rush_efficiency_support'
        THEN 'Claimed team had clear yards-per-rush support. Useful only if lift repeats.'

      WHEN r.Feature_Bucket = 'mild_rush_efficiency_support'
        THEN 'Mild rushing efficiency support is context only; do not boost from this alone.'

      WHEN r.Feature_Bucket = 'strong_rush_efficiency_against_claim'
        THEN 'Rushing efficiency strongly worked against the claim. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'clear_rush_efficiency_against_claim'
        THEN 'Rushing efficiency worked against the claim. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'mild_rush_efficiency_against_claim'
        THEN 'Mild rushing resistance against the claim. Use as context only.'

      WHEN r.Feature_Bucket = 'near_even_rush_efficiency'
        THEN 'Rushing efficiency was near even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Rushing efficiency context is missing. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_by_run_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs_raw AS (
  SELECT
    'rush_claim_support_score' AS Feature_Name,
    'ALL_SELECTED_RUNS' AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

bucket_summary_all_runs AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket IN (
        'strong_rush_efficiency_support',
        'clear_rush_efficiency_support'
      )
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep'

      WHEN r.Feature_Bucket = 'mild_rush_efficiency_support'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket IN (
        'strong_rush_efficiency_against_claim',
        'clear_rush_efficiency_against_claim'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket = 'mild_rush_efficiency_against_claim'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'near_even_rush_efficiency'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_rush_efficiency_support'
        THEN 'Claimed team had strong yards-per-rush support. Candidate rushing-language support.'

      WHEN r.Feature_Bucket = 'clear_rush_efficiency_support'
        THEN 'Claimed team had clear yards-per-rush support. Useful only if lift repeats.'

      WHEN r.Feature_Bucket = 'mild_rush_efficiency_support'
        THEN 'Mild rushing efficiency support is context only; do not boost from this alone.'

      WHEN r.Feature_Bucket = 'strong_rush_efficiency_against_claim'
        THEN 'Rushing efficiency strongly worked against the claim. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'clear_rush_efficiency_against_claim'
        THEN 'Rushing efficiency worked against the claim. Candidate warning bucket.'

      WHEN r.Feature_Bucket = 'mild_rush_efficiency_against_claim'
        THEN 'Mild rushing resistance against the claim. Use as context only.'

      WHEN r.Feature_Bucket = 'near_even_rush_efficiency'
        THEN 'Rushing efficiency was near even. This should not create strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Rushing efficiency context is missing. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_all_runs_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
)

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_by_run

UNION ALL

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN Test_Run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  Test_Run,
  Scope_Checked,
  Feature_Bucket;
-summary
rush_claim_support_score passed as a scoped rushing-language feature. The clean finding is that strong_rush_efficiency_support consistently lifted validation across rushing category claims, rushing-related claims, and yards-per-rush metric claims. This suggests that when a team has a truly strong yards-per-rush edge, GameLens can use that as support for rushing language. However, clear_rush_efficiency_support and mild_rush_efficiency_support both underperformed baseline, which means partial rushing support is not enough. This feature should be promoted only in a narrow way: use strong rushing efficiency as support, but do not boost from clear or mild rushing edges.
-easily said
A real rushing edge matters. A small rushing edge does not.
--feature 10--

-query
-- ============================================================
-- Feature 10: broad_rushing_control_score
--
-- Football question:
-- Does broad rushing/control support help broader matchup,
-- game-shape, offensive-output, or core-area claims validate?
--
-- Important scope warning:
-- This is NOT the same as rush_claim_support_score.
--
-- Feature 9 was narrow:
--   yards_per_rush support for rushing-related claims.
--
-- Feature 10 is broad:
--   rushing efficiency + rushing volume + run-heavy/control context.
--
-- Testing table pitfalls handled:
-- 1) Table is one row per claim, not one row per game.
-- 2) There are no clean away/home rushing-control feature fields.
-- 3) We reconstruct side-level rushing/control context from metric rows.
-- 4) Volume/control metrics are contextual, not automatically "better."
-- 5) Missing control context means unavailable, not bad football.
-- 6) This tests claim validation, not winner prediction.
--
-- Standard output columns:
-- Feature_Name
-- Test_Run
-- Scope_Checked
-- Feature_Bucket
-- Row_Count
-- Validation_Rate
-- Lift_vs_Baseline
-- Repeatable_Across_Runs
-- Decision
-- Notes
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claimed_team,
    claimed_side,
    opponent_team,
    opponent_side,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int,

    SAFE_CAST(pregame_percentile_gap AS FLOAT64) AS pregame_percentile_gap,
    SAFE_CAST(pregame_abs_percentile_gap AS FLOAT64) AS pregame_abs_percentile_gap,
    SAFE_CAST(offense_finish_score AS FLOAT64) AS offense_finish_score,
    SAFE_CAST(defensive_suppression_score AS FLOAT64) AS defensive_suppression_score,
    SAFE_CAST(two_way_edge_score AS FLOAT64) AS two_way_edge_score,
    two_way_context,
    SAFE_CAST(core_area_agreement_rate AS FLOAT64) AS core_area_agreement_rate

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- Rebuild away-signed rushing/control metric edges.
-- Positive = away team had the edge.
-- Negative = home team had the edge.
--
-- Notes:
-- yards_per_rush is the cleanest true rushing efficiency signal.
-- rushing_attempts / run_play_pct / time_of_possession are control/style context,
-- not automatically better football.
metric_edges AS (
  SELECT
    run_id,
    game_id,
    metric,

    ARRAY_AGG(
      CASE
        WHEN claimed_side = 'away'
          THEN SAFE_DIVIDE(ABS(pregame_percentile_gap), 100.0)

        WHEN claimed_side = 'home'
          THEN -SAFE_DIVIDE(ABS(pregame_percentile_gap), 100.0)

        ELSE NULL
      END
      IGNORE NULLS
      ORDER BY ABS(SAFE_DIVIDE(pregame_percentile_gap, 100.0)) DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS away_signed_edge

  FROM base
  WHERE metric IN (
      'yards_per_rush',
      'rushing_yards',
      'rushing_attempts',
      'rush_attempts',
      'run_play_pct',
      'time_of_possession',
      'total_plays',
      'total_drives',
      '1st_down_rate',
      'first_down_rate'
    )
    AND claimed_side IN ('away', 'home')
    AND pregame_percentile_gap IS NOT NULL
  GROUP BY
    run_id,
    game_id,
    metric
),

-- Turn metric edges into side-level components.
side_components AS (
  SELECT
    run_id,
    game_id,
    'away' AS side,
    metric,
    away_signed_edge AS side_signed_edge,

    CASE
      WHEN metric = 'yards_per_rush' THEN 'efficiency'
      WHEN metric IN ('rushing_yards', 'rushing_attempts', 'rush_attempts') THEN 'rushing_volume'
      WHEN metric IN ('run_play_pct', 'time_of_possession', 'total_plays', 'total_drives') THEN 'control_context'
      WHEN metric IN ('1st_down_rate', 'first_down_rate') THEN 'drive_support'
      ELSE 'other'
    END AS component_group,

    CASE
      WHEN metric = 'yards_per_rush' THEN 0.40
      WHEN metric IN ('rushing_yards') THEN 0.15
      WHEN metric IN ('rushing_attempts', 'rush_attempts') THEN 0.15
      WHEN metric = 'run_play_pct' THEN 0.10
      WHEN metric = 'time_of_possession' THEN 0.10
      WHEN metric IN ('total_plays', 'total_drives') THEN 0.05
      WHEN metric IN ('1st_down_rate', 'first_down_rate') THEN 0.10
      ELSE 0.05
    END AS component_weight

  FROM metric_edges
  WHERE away_signed_edge IS NOT NULL

  UNION ALL

  SELECT
    run_id,
    game_id,
    'home' AS side,
    metric,
    -away_signed_edge AS side_signed_edge,

    CASE
      WHEN metric = 'yards_per_rush' THEN 'efficiency'
      WHEN metric IN ('rushing_yards', 'rushing_attempts', 'rush_attempts') THEN 'rushing_volume'
      WHEN metric IN ('run_play_pct', 'time_of_possession', 'total_plays', 'total_drives') THEN 'control_context'
      WHEN metric IN ('1st_down_rate', 'first_down_rate') THEN 'drive_support'
      ELSE 'other'
    END AS component_group,

    CASE
      WHEN metric = 'yards_per_rush' THEN 0.40
      WHEN metric IN ('rushing_yards') THEN 0.15
      WHEN metric IN ('rushing_attempts', 'rush_attempts') THEN 0.15
      WHEN metric = 'run_play_pct' THEN 0.10
      WHEN metric = 'time_of_possession' THEN 0.10
      WHEN metric IN ('total_plays', 'total_drives') THEN 0.05
      WHEN metric IN ('1st_down_rate', 'first_down_rate') THEN 0.10
      ELSE 0.05
    END AS component_weight

  FROM metric_edges
  WHERE away_signed_edge IS NOT NULL
),

side_control_scores AS (
  SELECT
    run_id,
    game_id,
    side,

    COUNT(*) AS control_source_rows,
    COUNTIF(side_signed_edge > 0.05) AS positive_component_count,
    COUNTIF(side_signed_edge < -0.05) AS negative_component_count,

    ROUND(
      SAFE_DIVIDE(
        SUM(component_weight * side_signed_edge),
        SUM(component_weight)
      ),
      4
    ) AS rushing_control_score,

    ROUND(
      MAX(CASE WHEN metric = 'yards_per_rush' THEN side_signed_edge END),
      4
    ) AS yards_per_rush_edge,

    ROUND(
      AVG(CASE WHEN component_group = 'rushing_volume' THEN side_signed_edge END),
      4
    ) AS rushing_volume_edge,

    ROUND(
      AVG(CASE WHEN component_group = 'control_context' THEN side_signed_edge END),
      4
    ) AS control_context_edge,

    ROUND(
      AVG(CASE WHEN component_group = 'drive_support' THEN side_signed_edge END),
      4
    ) AS drive_support_edge

  FROM side_components
  GROUP BY
    run_id,
    game_id,
    side
),

-- Broad scopes on purpose.
-- This tests whether rushing/control helps broader game-shape language,
-- not just narrow rushing claims.
scope_rows AS (
  SELECT
    b.*,
    'game_profile_claims' AS scope_checked
  FROM base b
  WHERE b.claim_type = 'game_profile'

  UNION ALL

  SELECT
    b.*,
    'core_area_claims' AS scope_checked
  FROM base b
  WHERE b.claim_type IN (
    'core_area_comparison',
    'core_area_summary'
  )

  UNION ALL

  SELECT
    b.*,
    'offensive_output_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Offensive Output'

  UNION ALL

  SELECT
    b.*,
    'scoring_efficiency_claims' AS scope_checked
  FROM base b
  WHERE b.core_area = 'Scoring Efficiency'

  UNION ALL

  SELECT
    b.*,
    'broad_game_shape_claims' AS scope_checked
  FROM base b
  WHERE b.claim_type IN (
      'game_profile',
      'core_area_comparison',
      'core_area_summary',
      'category_summary',
      'metric_highlight',
      'team_comparison_metric'
    )
    AND (
      b.core_area IN (
        'Offensive Output',
        'Scoring Efficiency',
        'Defensive Control'
      )
      OR b.category IN (
        'Rushing Game',
        'Offensive Rhythm',
        'Drive Conversion',
        'Scoring Production',
        'Scoring Efficiency',
        'Defensive Efficiency',
        'Scoring Suppression'
      )
      OR b.metric IN (
        'yards_per_rush',
        'rushing_yards',
        'rushing_attempts',
        'rush_attempts',
        'run_play_pct',
        'time_of_possession',
        'total_plays',
        'total_drives',
        '1st_down_rate',
        'first_down_rate',
        'points_per_play',
        'points_allowed_per_play'
      )
    )

  UNION ALL

  SELECT
    b.*,
    'all_directional_claims' AS scope_checked
  FROM base b
  WHERE b.claim_type IN (
    'game_profile',
    'core_area_comparison',
    'core_area_summary',
    'category_summary',
    'metric_highlight',
    'team_comparison_metric'
  )
),

bucketed AS (
  SELECT
    s.*,

    c.rushing_control_score,
    c.yards_per_rush_edge,
    c.rushing_volume_edge,
    c.control_context_edge,
    c.drive_support_edge,
    c.control_source_rows,
    c.positive_component_count,
    c.negative_component_count,

    CASE
      WHEN c.rushing_control_score IS NULL
        THEN 'unavailable'

      WHEN c.control_source_rows < 3
        THEN 'thin_control_evidence'

      -- Best version: efficiency plus broader control support.
      WHEN c.rushing_control_score >= 0.18
        AND c.yards_per_rush_edge >= 0.15
        AND c.positive_component_count >= 3
        THEN 'strong_rushing_control_support'

      -- Efficiency is strong, but broader control profile may not be complete.
      WHEN c.yards_per_rush_edge >= 0.20
        AND c.positive_component_count < 3
        THEN 'efficiency_only_rush_support'

      -- Broad control without efficiency is dangerous; this can be script/style noise.
      WHEN c.rushing_control_score >= 0.12
        AND (
          c.yards_per_rush_edge IS NULL
          OR c.yards_per_rush_edge < 0.05
        )
        THEN 'volume_control_without_efficiency'

      WHEN c.rushing_control_score >= 0.12
        THEN 'clear_rushing_control_support'

      WHEN c.rushing_control_score >= 0.05
        THEN 'mild_rushing_control_support'

      WHEN c.rushing_control_score <= -0.12
        THEN 'rushing_control_against_claim'

      WHEN c.rushing_control_score <= -0.05
        THEN 'mild_rushing_control_against_claim'

      ELSE 'near_even_or_mixed_rushing_control'
    END AS feature_bucket

  FROM scope_rows s
  LEFT JOIN side_control_scores c
    ON s.run_id = c.run_id
   AND s.game_id = c.game_id
   AND s.claimed_side = c.side
),

scope_baseline_by_run AS (
  SELECT
    run_id,
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    run_id,
    scope_checked
),

bucket_summary_by_run_raw AS (
  SELECT
    'broad_rushing_control_score' AS Feature_Name,
    b.run_id AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_by_run s
    ON b.run_id = s.run_id
   AND b.scope_checked = s.scope_checked

  GROUP BY
    b.run_id,
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

repeatability AS (
  SELECT
    Scope_Checked,
    Feature_Bucket,

    COUNT(DISTINCT Test_Run) AS runs_present,
    COUNTIF(Row_Count >= 30) AS runs_with_enough_rows,

    COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) AS positive_lift_runs,
    COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(Row_Count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(Lift_vs_Baseline > 0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_positive'

      WHEN COUNTIF(Lift_vs_Baseline < -0.03 AND Row_Count >= 30) >= 2
        THEN 'yes_negative'

      ELSE 'unclear'
    END AS Repeatable_Across_Runs

  FROM bucket_summary_by_run_raw
  GROUP BY
    Scope_Checked,
    Feature_Bucket
),

bucket_summary_by_run AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket = 'strong_rushing_control_support'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep_scoped_retest'

      WHEN r.Feature_Bucket = 'efficiency_only_rush_support'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'volume_control_without_efficiency'
        THEN 'caution_only'

      WHEN r.Feature_Bucket IN (
        'clear_rushing_control_support',
        'mild_rushing_control_support'
      )
        THEN 'retest'

      WHEN r.Feature_Bucket IN (
        'rushing_control_against_claim',
        'mild_rushing_control_against_claim'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket IN (
        'thin_control_evidence',
        'near_even_or_mixed_rushing_control',
        'unavailable'
      )
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_rushing_control_support'
        THEN 'Strong rushing efficiency plus broader control support. Candidate broad control support, but must beat Feature 9.'

      WHEN r.Feature_Bucket = 'efficiency_only_rush_support'
        THEN 'Rushing efficiency exists, but broader control support is thin. This may simply duplicate Feature 9.'

      WHEN r.Feature_Bucket = 'volume_control_without_efficiency'
        THEN 'Control/volume exists without rushing efficiency. This may reflect style or script noise; use caution.'

      WHEN r.Feature_Bucket = 'clear_rushing_control_support'
        THEN 'Some broad rushing/control support exists. Useful only if lift repeats cleanly.'

      WHEN r.Feature_Bucket = 'mild_rushing_control_support'
        THEN 'Mild broad control support is too thin for strong language.'

      WHEN r.Feature_Bucket = 'rushing_control_against_claim'
        THEN 'Broad rushing/control profile works against the claim. Candidate warning if repeatable.'

      WHEN r.Feature_Bucket = 'mild_rushing_control_against_claim'
        THEN 'Mild control resistance. Use as context only unless repeatability is strong.'

      WHEN r.Feature_Bucket = 'thin_control_evidence'
        THEN 'Too few control inputs. Do not use as a signal.'

      WHEN r.Feature_Bucket = 'near_even_or_mixed_rushing_control'
        THEN 'Rushing/control profile is mixed or near even. No strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Rushing/control context is missing. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_by_run_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
),

scope_baseline_all_runs AS (
  SELECT
    scope_checked,
    COUNT(*) AS baseline_row_count,
    SUM(validated_flag_int) AS baseline_validated_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_rate
  FROM bucketed
  GROUP BY
    scope_checked
),

bucket_summary_all_runs_raw AS (
  SELECT
    'broad_rushing_control_score' AS Feature_Name,
    'ALL_SELECTED_RUNS' AS Test_Run,
    b.scope_checked AS Scope_Checked,
    b.feature_bucket AS Feature_Bucket,

    COUNT(*) AS Row_Count,
    SUM(validated_flag_int) AS Validated_Count,
    COUNTIF(validation_result = 'not_validated') AS Not_Validated_Count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS Neutral_Or_Mixed_Count,
    COUNTIF(validation_result = 'unavailable') AS Unavailable_Count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS Validation_Rate,
    ROUND(s.baseline_rate, 4) AS Baseline_Rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - s.baseline_rate,
      4
    ) AS Lift_vs_Baseline

  FROM bucketed b
  LEFT JOIN scope_baseline_all_runs s
    ON b.scope_checked = s.scope_checked

  GROUP BY
    b.scope_checked,
    b.feature_bucket,
    s.baseline_rate
),

bucket_summary_all_runs AS (
  SELECT
    r.Feature_Name,
    r.Test_Run,
    r.Scope_Checked,
    r.Feature_Bucket,
    r.Row_Count,
    r.Validation_Rate,
    r.Lift_vs_Baseline,
    rep.Repeatable_Across_Runs,

    CASE
      WHEN r.Row_Count < 30
        THEN 'hold_too_small'

      WHEN r.Feature_Bucket = 'strong_rushing_control_support'
        AND rep.Repeatable_Across_Runs = 'yes_positive'
        THEN 'keep_scoped_retest'

      WHEN r.Feature_Bucket = 'efficiency_only_rush_support'
        THEN 'hold_context_only'

      WHEN r.Feature_Bucket = 'volume_control_without_efficiency'
        THEN 'caution_only'

      WHEN r.Feature_Bucket IN (
        'clear_rushing_control_support',
        'mild_rushing_control_support'
      )
        THEN 'retest'

      WHEN r.Feature_Bucket IN (
        'rushing_control_against_claim',
        'mild_rushing_control_against_claim'
      )
        AND rep.Repeatable_Across_Runs = 'yes_negative'
        THEN 'keep_as_warning'

      WHEN r.Feature_Bucket IN (
        'thin_control_evidence',
        'near_even_or_mixed_rushing_control',
        'unavailable'
      )
        THEN 'hold_context_only'

      ELSE 'retest'
    END AS Decision,

    CASE
      WHEN r.Row_Count < 30
        THEN 'Small row count; do not use as evidence yet.'

      WHEN r.Feature_Bucket = 'strong_rushing_control_support'
        THEN 'Strong rushing efficiency plus broader control support. Candidate broad control support, but must beat Feature 9.'

      WHEN r.Feature_Bucket = 'efficiency_only_rush_support'
        THEN 'Rushing efficiency exists, but broader control support is thin. This may simply duplicate Feature 9.'

      WHEN r.Feature_Bucket = 'volume_control_without_efficiency'
        THEN 'Control/volume exists without rushing efficiency. This may reflect style or script noise; use caution.'

      WHEN r.Feature_Bucket = 'clear_rushing_control_support'
        THEN 'Some broad rushing/control support exists. Useful only if lift repeats cleanly.'

      WHEN r.Feature_Bucket = 'mild_rushing_control_support'
        THEN 'Mild broad control support is too thin for strong language.'

      WHEN r.Feature_Bucket = 'rushing_control_against_claim'
        THEN 'Broad rushing/control profile works against the claim. Candidate warning if repeatable.'

      WHEN r.Feature_Bucket = 'mild_rushing_control_against_claim'
        THEN 'Mild control resistance. Use as context only unless repeatability is strong.'

      WHEN r.Feature_Bucket = 'thin_control_evidence'
        THEN 'Too few control inputs. Do not use as a signal.'

      WHEN r.Feature_Bucket = 'near_even_or_mixed_rushing_control'
        THEN 'Rushing/control profile is mixed or near even. No strong language.'

      WHEN r.Feature_Bucket = 'unavailable'
        THEN 'Rushing/control context is missing. Do not treat as bad football.'

      ELSE 'Review pattern across individual runs before promoting.'
    END AS Notes

  FROM bucket_summary_all_runs_raw r
  LEFT JOIN repeatability rep
    ON r.Scope_Checked = rep.Scope_Checked
   AND r.Feature_Bucket = rep.Feature_Bucket
)

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_by_run

UNION ALL

SELECT
  Feature_Name,
  Test_Run,
  Scope_Checked,
  Feature_Bucket,
  Row_Count,
  Validation_Rate,
  Lift_vs_Baseline,
  Repeatable_Across_Runs,
  Decision,
  Notes
FROM bucket_summary_all_runs

ORDER BY
  CASE WHEN Test_Run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  Test_Run,
  Scope_Checked,
  Feature_Bucket;
-summary
broad_rushing_control_score should not be promoted. The test did not produce meaningful strong_rushing_control_support, clear_rushing_control_support, or warning buckets. Instead, almost every row fell into thin_control_evidence, and those rows showed essentially zero lift across all major scopes, including all-directional claims, broad game-shape claims, Core Area claims, Game Profile claims, Offensive Output claims, and Scoring Efficiency claims. This means the current claim-training table and reconstruction method do not provide enough broad rushing/control context to test this feature properly. The right conclusion is hold research, not “rushing control does not matter.” For now, Feature 9’s narrow strong_rush_efficiency_support is much cleaner and should be preferred.
-easily said
We tried to test “can this team control the game on the ground?” but the data did not give us enough real control evidence to answer that.
--fun part-- do some data discovery
-query
-- ============================================================
-- Exploratory Query 1:
-- Metric Opportunity Inventory
--
-- Goal:
-- Let the data show which metrics are frequent, useful,
-- noisy, or potentially misleading.
--
-- This is not testing a hand-built feature.
-- This is looking for patterns in the claim-training data.
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
    AND metric IS NOT NULL
),

overall_baseline AS (
  SELECT
    run_id,
    COUNT(*) AS baseline_row_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
  GROUP BY run_id
),

metric_by_run AS (
  SELECT
    b.run_id,
    b.metric,
    ANY_VALUE(b.category) AS sample_category,
    ANY_VALUE(b.core_area) AS sample_core_area,

    COUNT(*) AS row_count,
    SUM(validated_flag_int) AS validated_count,
    COUNTIF(validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(o.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - o.baseline_validation_rate,
      4
    ) AS lift_vs_baseline

  FROM base b
  LEFT JOIN overall_baseline o
    ON b.run_id = o.run_id
  GROUP BY
    b.run_id,
    b.metric,
    o.baseline_validation_rate
),

repeatability AS (
  SELECT
    metric,

    COUNT(DISTINCT run_id) AS runs_present,
    COUNTIF(row_count >= 30) AS runs_with_enough_rows,

    COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) AS positive_lift_runs,
    COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(row_count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) >= 2
        THEN 'repeat_positive'

      WHEN COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) >= 2
        THEN 'repeat_negative'

      ELSE 'mixed_or_unclear'
    END AS repeatability_label

  FROM metric_by_run
  GROUP BY metric
),

all_runs AS (
  SELECT
    'ALL_SELECTED_RUNS' AS test_run,
    metric,
    ANY_VALUE(sample_category) AS sample_category,
    ANY_VALUE(sample_core_area) AS sample_core_area,

    COUNT(*) AS run_rows_present,
    SUM(row_count) AS row_count,
    SUM(validated_count) AS validated_count,
    SUM(not_validated_count) AS not_validated_count,
    SUM(neutral_or_mixed_count) AS neutral_or_mixed_count,
    SUM(unavailable_count) AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(validated_count), SUM(row_count)), 4) AS validation_rate

  FROM metric_by_run
  GROUP BY metric
),

all_baseline AS (
  SELECT
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
),

final AS (
  SELECT
    a.test_run,
    a.metric,
    a.sample_category,
    a.sample_core_area,
    a.row_count,
    a.validated_count,
    a.not_validated_count,
    a.neutral_or_mixed_count,
    a.unavailable_count,
    a.validation_rate,
    ROUND(ab.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(a.validation_rate - ab.baseline_validation_rate, 4) AS lift_vs_baseline,
    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    CASE
      WHEN a.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN a.row_count >= 100 AND ABS(a.validation_rate - ab.baseline_validation_rate) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label

  FROM all_runs a
  CROSS JOIN all_baseline ab
  LEFT JOIN repeatability r
    ON a.metric = r.metric
)

SELECT *
FROM final
ORDER BY
  CASE exploratory_label
    WHEN 'opportunity_candidate' THEN 1
    WHEN 'warning_candidate' THEN 2
    WHEN 'high_volume_neutral' THEN 3
    WHEN 'review' THEN 4
    ELSE 5
  END,
  row_count DESC,
  ABS(lift_vs_baseline) DESC;

--interpretation

Exploratory Metric Inventory — Findings
1. Points per play is the strongest opportunity metric

points_per_play had the best overall profile:

Row Count: 987
Validation Rate: 56.53%
Lift vs Baseline: +7.61 pts
Repeatability: repeat_positive

Finding: This is currently the cleanest support metric in the inventory. GameLens claims tied to efficient scoring are more likely to validate.

Football read:
Teams that turn plays into points efficiently create cleaner matchup claims.

2. Yards per rush is a real support signal

yards_per_rush also came through as repeat-positive:

Row Count: 810
Validation Rate: 53.95%
Lift vs Baseline: +5.03 pts
Repeatability: repeat_positive

Finding: This supports what we saw in Feature 9. Strong rushing efficiency matters, but only when the edge is strong enough.

Football read:
A real rushing efficiency edge helps validate rushing-related claims.

3. Yards per play looks promising

yards_per_play was also repeat-positive:

Row Count: 540
Validation Rate: 53.89%
Lift vs Baseline: +4.97 pts
Repeatability: repeat_positive

Finding: Broad offensive efficiency may deserve more attention. This could become a future feature family around “efficient offensive production.”

Football read:
Teams that gain more per snap are easier to trust than teams with raw volume.

4. Turnover margin per game is a warning metric

turnover_margin_per_game was repeat-negative:

Row Count: 1442
Validation Rate: 41.96%
Lift vs Baseline: -6.96 pts
Repeatability: repeat_negative

Finding: This is a major caution flag. Turnovers may describe past advantage, but they are dangerous as standalone claim support.

Football read:
Turnovers are noisy. Do not let them drive strong language by themselves.

5. Red zone efficiency is also a warning metric

red_zone_efficiency was repeat-negative:

Row Count: 1193
Validation Rate: 44.59%
Lift vs Baseline: -4.33 pts
Repeatability: repeat_negative

Finding: Red zone efficiency sounds useful, but the data says it can mislead claim language. This lines up with Feature 8 struggling.

Football read:
Red zone finish is too volatile to trust as a strong standalone signal.

6. TD rate is the biggest red flag

td_rate had the worst lift:

Row Count: 587
Validation Rate: 32.88%
Lift vs Baseline: -16.04 pts
Repeatability: repeat_negative

Finding: Touchdown rate is very dangerous as support. It should probably remain blocked, softened, or treated as volatile context.

Football read:
Touchdowns are high-value, but too situational to use as clean proof.

7. Third down percentage is high-volume but neutral

third_down_pct had a large row count but basically no lift:

Row Count: 1177
Validation Rate: 49.36%
Lift vs Baseline: +0.44 pts
Repeatability: mixed_or_unclear

Finding: Third down percentage is not useless, but it does not appear to be a strong driver. It belongs in context, not confidence logic.

Football read:
Third down rate helps describe a team, but it should not steer the matchup read.

8. First down rate is also neutral/noisy

1st_down_rate was slightly below baseline:

Row Count: 634
Validation Rate: 48.11%
Lift vs Baseline: -0.81 pts
Repeatability: mixed_or_unclear

Finding: This challenges the idea that drive-sustainability stats are automatically useful. Good context, weak driver.

Football read:
Moving the chains matters, but this metric alone is not proving claim quality.

9. Points allowed per play is interesting but not clean yet

points_allowed_per_play showed positive lift, but repeatability was not strong enough:

Row Count: 1431
Validation Rate: 52.69%
Lift vs Baseline: +3.77 pts
Repeatability: mixed_or_unclear

Finding: This is a review candidate. It supports Feature 4 being directionally useful but not clean enough to fully promote.

Football read:
Defensive scoring suppression matters, but it needs better scoped support.

10. Yards per pass is promising but inconsistent

yards_per_pass had good all-run lift but mixed repeatability:

Row Count: 810
Validation Rate: 55.06%
Lift vs Baseline: +6.14 pts
Repeatability: mixed_or_unclear

Finding: This supports Feature 3’s result: useful, but not fully proven. It may need tighter scoping or stronger thresholds.

Football read:
Passing efficiency matters, but the current test does not prove it as cleanly as rushing efficiency or points per play.

The big picture

The data is pointing toward three families:

Support candidates:
- points_per_play
- yards_per_rush
- yards_per_play

Warning candidates:
- turnover_margin_per_game
- red_zone_efficiency
- td_rate

Context-only / noisy:
- third_down_pct
- 1st_down_rate

Review candidates:
- points_allowed_per_play
- yards_per_pass
Simple football summary

Trust efficiency. Be careful with volatile finish stats. Treat conversion stats as context.

--category level query --
-- ============================================================
-- Exploratory Query 2:
-- Category Opportunity Inventory
--
-- Goal:
-- Let the data show which categories are frequent, useful,
-- noisy, or potentially misleading.
--
-- This is not testing a hand-built feature.
-- This is looking for category-level patterns in the
-- claim-training data.
--
-- Output includes:
-- - Per-run rows
-- - ALL_SELECTED_RUNS rows
-- - validation rate
-- - lift vs baseline
-- - repeatability label
-- - exploratory label
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
    AND category IS NOT NULL
),

overall_baseline_by_run AS (
  SELECT
    run_id,
    COUNT(*) AS baseline_row_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
  GROUP BY run_id
),

category_by_run AS (
  SELECT
    b.run_id AS test_run,
    b.category,

    STRING_AGG(DISTINCT COALESCE(b.core_area, 'missing'), ', ' ORDER BY COALESCE(b.core_area, 'missing')) AS core_areas_seen,
    COUNT(DISTINCT b.metric) AS distinct_metric_count,
    COUNT(DISTINCT b.claim_type) AS distinct_claim_type_count,

    COUNT(*) AS row_count,
    SUM(validated_flag_int) AS validated_count,
    COUNTIF(validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(o.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - o.baseline_validation_rate,
      4
    ) AS lift_vs_baseline

  FROM base b
  LEFT JOIN overall_baseline_by_run o
    ON b.run_id = o.run_id

  GROUP BY
    b.run_id,
    b.category,
    o.baseline_validation_rate
),

repeatability AS (
  SELECT
    category,

    COUNT(DISTINCT test_run) AS runs_present,
    COUNTIF(row_count >= 30) AS runs_with_enough_rows,

    COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) AS positive_lift_runs,
    COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(row_count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) >= 2
        THEN 'repeat_positive'

      WHEN COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) >= 2
        THEN 'repeat_negative'

      ELSE 'mixed_or_unclear'
    END AS repeatability_label

  FROM category_by_run
  GROUP BY category
),

overall_baseline_all_runs AS (
  SELECT
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
),

category_all_runs AS (
  SELECT
    'ALL_SELECTED_RUNS' AS test_run,
    category,

    STRING_AGG(DISTINCT COALESCE(core_area, 'missing'), ', ' ORDER BY COALESCE(core_area, 'missing')) AS core_areas_seen,
    COUNT(DISTINCT metric) AS distinct_metric_count,
    COUNT(DISTINCT claim_type) AS distinct_claim_type_count,

    COUNT(*) AS row_count,
    SUM(validated_flag_int) AS validated_count,
    COUNTIF(validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS validation_rate

  FROM base
  GROUP BY category
),

category_all_runs_final AS (
  SELECT
    a.test_run,
    a.category,
    a.core_areas_seen,
    a.distinct_metric_count,
    a.distinct_claim_type_count,
    a.row_count,
    a.validated_count,
    a.not_validated_count,
    a.neutral_or_mixed_count,
    a.unavailable_count,
    a.validation_rate,
    ROUND(o.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(a.validation_rate - o.baseline_validation_rate, 4) AS lift_vs_baseline,

    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    CASE
      WHEN a.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN a.row_count >= 100 AND ABS(a.validation_rate - o.baseline_validation_rate) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label

  FROM category_all_runs a
  CROSS JOIN overall_baseline_all_runs o
  LEFT JOIN repeatability r
    ON a.category = r.category
),

category_by_run_final AS (
  SELECT
    c.test_run,
    c.category,
    c.core_areas_seen,
    c.distinct_metric_count,
    c.distinct_claim_type_count,
    c.row_count,
    c.validated_count,
    c.not_validated_count,
    c.neutral_or_mixed_count,
    c.unavailable_count,
    c.validation_rate,
    c.baseline_validation_rate,
    c.lift_vs_baseline,

    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    CASE
      WHEN c.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN c.row_count >= 100 AND ABS(c.validation_rate - c.baseline_validation_rate) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label

  FROM category_by_run c
  LEFT JOIN repeatability r
    ON c.category = r.category
)

SELECT *
FROM category_all_runs_final

UNION ALL

SELECT *
FROM category_by_run_final

ORDER BY
  CASE WHEN test_run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  CASE exploratory_label
    WHEN 'opportunity_candidate' THEN 1
    WHEN 'warning_candidate' THEN 2
    WHEN 'high_volume_neutral' THEN 3
    WHEN 'review' THEN 4
    ELSE 5
  END,
  row_count DESC,
  ABS(lift_vs_baseline) DESC;

  -grand summary
  Category-Level Findings
1. Rushing Game is the cleanest category opportunity ✅
Category: Rushing Game
Row Count: 695
Validation Rate: 53.53%
Lift vs Baseline: +4.98 pts
Repeatability: repeat_positive
Label: opportunity_candidate

Finding: Rushing Game repeated positively and lines up with our metric-level finding that yards_per_rush was strong.

Football read:

Rushing efficiency/quality is becoming one of the cleaner claim-support areas.

2. Scoring Production is a warning category ⚠️
Category: Scoring Production
Row Count: 1062
Validation Rate: 44.35%
Lift vs Baseline: -4.20 pts
Repeatability: repeat_negative
Label: warning_candidate

Finding: This is important because points_per_play was excellent at the metric level, but the broader Scoring Production category was negative.

Football read:

Specific scoring efficiency is useful, but broad scoring-production claims can get noisy fast.

3. Turnovers are a warning category ⚠️
Category: Turnovers
Row Count: 684
Validation Rate: 42.54%
Lift vs Baseline: -6.01 pts
Repeatability: repeat_negative
Label: warning_candidate

Finding: This confirms the metric-level warning from turnover_margin_per_game.

Football read:

Turnover claims should be softened. They are descriptive, but volatile.

4. Turnover Risk is also a warning category ⚠️
Category: Turnover Risk
Row Count: 391
Validation Rate: 42.20%
Lift vs Baseline: -6.35 pts
Repeatability: repeat_negative
Label: warning_candidate

Finding: This is probably game-profile style turnover language, not a direct metric category, because it has distinct_metric_count = 0.

Football read:

Even when GameLens talks about turnover environment broadly, it should avoid sounding too confident.

5. Pressure is a strong warning category ⚠️
Category: Pressure
Row Count: 334
Validation Rate: 41.92%
Lift vs Baseline: -6.63 pts
Repeatability: repeat_negative
Label: warning_candidate

Finding: Pressure was negative in all three runs. That’s loud.

Football read:

Pressure may explain matchup stress, but it should not be treated as a clean claim-validation driver by itself.

6. Drive Conversion is high-volume neutral
Category: Drive Conversion
Row Count: 695
Validation Rate: 49.78%
Lift vs Baseline: +1.23 pts
Repeatability: mixed_or_unclear
Label: high_volume_neutral

Finding: This matches our Feature 8 result. Conversion stats are not useless, but they are not strong support.

Football read:

Drive conversion helps describe a team, but it should not steer confidence.

7. Offensive Rhythm is a review candidate
Category: Offensive Rhythm
Row Count: 1035
Validation Rate: 51.11%
Lift vs Baseline: +2.56 pts
Repeatability: mixed_or_unclear
Label: review

Finding: Positive overall, but not clean enough yet. This likely connects to yards_per_play being strong while 1st_down_rate was neutral.

Football read:

Offensive rhythm has useful ingredients, but the category is a blend of strong and noisy signals.

8. Passing Game is promising but not repeat-clean
Category: Passing Game
Row Count: 704
Validation Rate: 54.12%
Lift vs Baseline: +5.57 pts
Repeatability: mixed_or_unclear
Label: review

Finding: Strong overall lift, but not repeat-positive. This lines up with Feature 3: passing efficiency is useful, but not fully proven.

Football read:

Passing Game may be useful, but it needs tighter scoping before promotion.

9. Red Zone Finish remains suspicious
Category: Red Zone Finish
Row Count: 694
Validation Rate: 44.81%
Lift vs Baseline: -3.74 pts
Repeatability: mixed_or_unclear
Label: review

Finding: Negative overall, though not repeat-negative by the query rules. This supports our caution around red_zone_efficiency.

Football read:

Red-zone claims should stay soft unless backed by stronger surrounding support.

10. Scoring Suppression is interesting but not clean
Category: Scoring Suppression
Row Count: 673
Validation Rate: 52.90%
Lift vs Baseline: +4.35 pts
Repeatability: mixed_or_unclear
Label: review

Finding: Positive all-runs result, but repeatability is not clean. This mirrors Feature 4.

Football read:

Defensive scoring suppression may be useful, but it needs stronger confirmation before becoming a boost.

11. Scoring Efficiency is positive but weirdly scoped
Category: Scoring Efficiency
Row Count: 380
Validation Rate: 53.95%
Lift vs Baseline: +5.40 pts
Repeatability: mixed_or_unclear
Label: review

Finding: Looks good overall, but distinct_metric_count = 0, so this is likely coming from higher-level claim rows rather than direct metric rows. Treat it carefully.

Football read:

Scoring Efficiency as a broad claim area looks promising, but we need to understand the row shape before promoting.

Big Category-Level Summary
Clean opportunity:
- Rushing Game

Warning candidates:
- Pressure
- Turnovers
- Turnover Risk
- Scoring Production

Neutral/context:
- Drive Conversion

Review candidates:
- Passing Game
- Scoring Suppression
- Scoring Efficiency
- Offensive Rhythm
- Red Zone Finish
Main lesson

The category-level data says:

Rushing is the cleanest broad category. Turnovers and pressure are the most dangerous broad categories. Scoring is split: points-per-play is strong, but broader Scoring Production is noisy.

That last part matters a lot. It means we should not say:

“Scoring metrics are good.”

Better version:

Specific scoring efficiency metrics can be good, but broad scoring-production language needs restraint.

--core area grouping features--

-query
-- ============================================================
-- Exploratory Query 3:
-- Core Area Opportunity Inventory
--
-- Goal:
-- Let the data show which Core Areas are frequent, useful,
-- noisy, or potentially misleading.
--
-- This is not testing a hand-built feature.
-- This is looking for Core Area-level patterns in the
-- claim-training data.
--
-- Output includes:
-- - Per-run rows
-- - ALL_SELECTED_RUNS rows
-- - validation rate
-- - lift vs baseline
-- - repeatability label
-- - exploratory label
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
    AND core_area IS NOT NULL
),

overall_baseline_by_run AS (
  SELECT
    run_id,
    COUNT(*) AS baseline_row_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
  GROUP BY run_id
),

core_area_by_run AS (
  SELECT
    b.run_id AS test_run,
    b.core_area,

    STRING_AGG(
      DISTINCT COALESCE(b.category, 'missing'),
      ', '
      ORDER BY COALESCE(b.category, 'missing')
    ) AS categories_seen,

    COUNT(DISTINCT b.metric) AS distinct_metric_count,
    COUNT(DISTINCT b.category) AS distinct_category_count,
    COUNT(DISTINCT b.claim_type) AS distinct_claim_type_count,

    COUNT(*) AS row_count,
    SUM(validated_flag_int) AS validated_count,
    COUNTIF(validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(o.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(
      SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) - o.baseline_validation_rate,
      4
    ) AS lift_vs_baseline

  FROM base b
  LEFT JOIN overall_baseline_by_run o
    ON b.run_id = o.run_id

  GROUP BY
    b.run_id,
    b.core_area,
    o.baseline_validation_rate
),

repeatability AS (
  SELECT
    core_area,

    COUNT(DISTINCT test_run) AS runs_present,
    COUNTIF(row_count >= 30) AS runs_with_enough_rows,

    COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) AS positive_lift_runs,
    COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(row_count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) >= 2
        THEN 'repeat_positive'

      WHEN COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) >= 2
        THEN 'repeat_negative'

      ELSE 'mixed_or_unclear'
    END AS repeatability_label

  FROM core_area_by_run
  GROUP BY core_area
),

overall_baseline_all_runs AS (
  SELECT
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
),

core_area_all_runs AS (
  SELECT
    'ALL_SELECTED_RUNS' AS test_run,
    core_area,

    STRING_AGG(
      DISTINCT COALESCE(category, 'missing'),
      ', '
      ORDER BY COALESCE(category, 'missing')
    ) AS categories_seen,

    COUNT(DISTINCT metric) AS distinct_metric_count,
    COUNT(DISTINCT category) AS distinct_category_count,
    COUNT(DISTINCT claim_type) AS distinct_claim_type_count,

    COUNT(*) AS row_count,
    SUM(validated_flag_int) AS validated_count,
    COUNTIF(validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)), 4) AS validation_rate

  FROM base
  GROUP BY core_area
),

core_area_all_runs_final AS (
  SELECT
    a.test_run,
    a.core_area,
    a.categories_seen,
    a.distinct_metric_count,
    a.distinct_category_count,
    a.distinct_claim_type_count,
    a.row_count,
    a.validated_count,
    a.not_validated_count,
    a.neutral_or_mixed_count,
    a.unavailable_count,
    a.validation_rate,
    ROUND(o.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(a.validation_rate - o.baseline_validation_rate, 4) AS lift_vs_baseline,

    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    CASE
      WHEN a.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN a.row_count >= 100 AND ABS(a.validation_rate - o.baseline_validation_rate) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label

  FROM core_area_all_runs a
  CROSS JOIN overall_baseline_all_runs o
  LEFT JOIN repeatability r
    ON a.core_area = r.core_area
),

core_area_by_run_final AS (
  SELECT
    c.test_run,
    c.core_area,
    c.categories_seen,
    c.distinct_metric_count,
    c.distinct_category_count,
    c.distinct_claim_type_count,
    c.row_count,
    c.validated_count,
    c.not_validated_count,
    c.neutral_or_mixed_count,
    c.unavailable_count,
    c.validation_rate,
    c.baseline_validation_rate,
    c.lift_vs_baseline,

    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    CASE
      WHEN c.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN c.row_count >= 100 AND ABS(c.validation_rate - c.baseline_validation_rate) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label

  FROM core_area_by_run c
  LEFT JOIN repeatability r
    ON c.core_area = r.core_area
)

SELECT *
FROM core_area_all_runs_final

UNION ALL

SELECT *
FROM core_area_by_run_final

ORDER BY
  CASE WHEN test_run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  CASE exploratory_label
    WHEN 'opportunity_candidate' THEN 1
    WHEN 'warning_candidate' THEN 2
    WHEN 'high_volume_neutral' THEN 3
    WHEN 'review' THEN 4
    ELSE 5
  END,
  row_count DESC,
  ABS(lift_vs_baseline) DESC;

-findings
Core Area Findings
1. Offensive Output is the clean opportunity ✅
Core Area: Offensive Output
Row Count: 2,074
Validation Rate: 53.66%
Lift vs Baseline: +4.06 pts
Repeatability: repeat_positive
Label: opportunity_candidate

Finding: This is the strongest Core Area-level support signal.

Football read:

Offensive efficiency and production claims are generally more trustworthy than the average GameLens claim.

This lines up with the metric/category findings:

yards_per_play = good
yards_per_rush = good
Passing Game = promising
Rushing Game = clean category opportunity
2. Disruption and Turnovers is the clean warning ⚠️
Core Area: Disruption and Turnovers
Row Count: 1,059
Validation Rate: 43.06%
Lift vs Baseline: -6.54 pts
Repeatability: repeat_negative
Label: warning_candidate

Finding: This is the strongest Core Area-level caution area.

Football read:

Turnover/disruption claims are volatile and should not get strong language without extra support.

This lines up perfectly with:

turnover_margin_per_game = warning
Turnovers category = warning
Turnover Risk category = warning
Pressure category = warning
3. Scoring Efficiency is mixed and weaker than expected
Core Area: Scoring Efficiency
Row Count: 2,054
Validation Rate: 47.13%
Lift vs Baseline: -2.47 pts
Repeatability: mixed_or_unclear
Label: review

Finding: This is not a clean promote at the Core Area level.

This is important because points_per_play looked excellent as a metric, but the broader Scoring Efficiency Core Area includes noisier categories:

Drive Conversion
Red Zone Finish
Scoring Production

Football read:

Scoring Efficiency is not automatically clean. Specific scoring efficiency metrics can be useful, but the full Core Area gets noisy.

4. Defensive Control is promising but not repeat-clean
Core Area: Defensive Control
Row Count: 1,027
Validation Rate: 53.07%
Lift vs Baseline: +3.47 pts
Repeatability: mixed_or_unclear
Label: review

Finding: Defensive Control has positive lift, but not enough repeatability to promote broadly.

Football read:

Defensive Control probably matters, but it needs scoped support before GameLens talks strongly.

This matches Feature 4:

strong defensive resistance = useful directionally
clear defensive resistance = not enough
Biggest Lesson

The Core Area view says:

Promote / explore positively:
- Offensive Output

Use caution / language restraint:
- Disruption and Turnovers

Review carefully:
- Defensive Control
- Scoring Efficiency
Clean football summary

GameLens should trust offensive efficiency more than chaos stats.

Or even simpler:

Offense output tells a cleaner story. Turnovers and pressure tell a dangerous story. Defense and scoring need more context.

How this connects to the earlier layers
Metric layer:
- points_per_play, yards_per_rush, yards_per_play were good
- turnovers, red zone, TD rate were dangerous

Category layer:
- Rushing Game was good
- Turnovers, Turnover Risk, Pressure, Scoring Production were warnings

Core Area layer:
- Offensive Output was good
- Disruption and Turnovers was bad
- Scoring Efficiency and Defensive Control need scoped handling

That is a very useful pattern map. The data is not saying “all football features are equal.” It is saying:

Efficiency survives. Chaos needs restraint. Context needs scoping.

--looking at all 3--

-query

-- ============================================================
-- Exploratory Query 4:
-- Metric + Category + Core Area Hierarchical Inventory
--
-- Goal:
-- Look at all 3 levels together:
--   core_area
--   core_area > category
--   core_area > category > metric
--
-- This helps answer:
-- - Is the whole Core Area useful?
-- - Is the category useful?
-- - Or is only one metric inside that category useful?
--
-- Important:
-- This is not a model.
-- This is a data map / opportunity inventory.
--
-- Baseline:
-- Uses one consistent global claim baseline across all selected runs.
-- This makes Core Area, Category, and Metric levels easier to compare.
--
-- Table:
-- nfl-stream-406420.Analytics.gamelens_claim_training_examples
-- ============================================================

WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claim_type,
    claim_layer,
    claim_name,
    core_area,
    category,
    metric,
    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
),

-- One consistent baseline per run across all claim rows.
baseline_by_run AS (
  SELECT
    run_id,
    COUNT(*) AS baseline_row_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
  GROUP BY run_id
),

baseline_all_runs AS (
  SELECT
    COUNT(*) AS baseline_row_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
),

-- Build one row per hierarchy level.
-- A single claim can appear once at each applicable level.
hierarchy_rows AS (
  -- -------------------------
  -- Level 1: Core Area
  -- -------------------------
  SELECT
    run_id,
    claim_key,
    game_id,
    claim_type,
    claim_layer,
    validation_result,
    validated_flag_int,

    'core_area' AS grain_level,
    core_area AS core_area,
    CAST(NULL AS STRING) AS category,
    CAST(NULL AS STRING) AS metric,
    core_area AS hierarchy_path,

    0 AS missing_core_area_parent_flag,
    0 AS missing_category_parent_flag

  FROM base
  WHERE core_area IS NOT NULL

  UNION ALL

  -- -------------------------
  -- Level 2: Core Area > Category
  -- -------------------------
  SELECT
    run_id,
    claim_key,
    game_id,
    claim_type,
    claim_layer,
    validation_result,
    validated_flag_int,

    'category' AS grain_level,
    COALESCE(core_area, 'missing_core_area') AS core_area,
    category AS category,
    CAST(NULL AS STRING) AS metric,
    CONCAT(COALESCE(core_area, 'missing_core_area'), ' > ', category) AS hierarchy_path,

    CASE WHEN core_area IS NULL THEN 1 ELSE 0 END AS missing_core_area_parent_flag,
    0 AS missing_category_parent_flag

  FROM base
  WHERE category IS NOT NULL

  UNION ALL

  -- -------------------------
  -- Level 3: Core Area > Category > Metric
  -- -------------------------
  SELECT
    run_id,
    claim_key,
    game_id,
    claim_type,
    claim_layer,
    validation_result,
    validated_flag_int,

    'metric' AS grain_level,
    COALESCE(core_area, 'missing_core_area') AS core_area,
    COALESCE(category, 'missing_category') AS category,
    metric AS metric,
    CONCAT(
      COALESCE(core_area, 'missing_core_area'),
      ' > ',
      COALESCE(category, 'missing_category'),
      ' > ',
      metric
    ) AS hierarchy_path,

    CASE WHEN core_area IS NULL THEN 1 ELSE 0 END AS missing_core_area_parent_flag,
    CASE WHEN category IS NULL THEN 1 ELSE 0 END AS missing_category_parent_flag

  FROM base
  WHERE metric IS NOT NULL
),

node_by_run_raw AS (
  SELECT
    h.run_id AS test_run,
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,

    COUNT(*) AS row_count,
    COUNT(DISTINCT h.game_id) AS distinct_game_count,
    COUNT(DISTINCT h.claim_key) AS unique_claim_count,
    COUNT(DISTINCT h.claim_type) AS distinct_claim_type_count,
    COUNT(DISTINCT h.claim_layer) AS distinct_claim_layer_count,

    SUM(h.validated_flag_int) AS validated_count,
    COUNTIF(h.validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(h.validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(h.validation_result = 'unavailable') AS unavailable_count,

    COUNTIF(h.missing_core_area_parent_flag = 1) AS missing_core_area_parent_rows,
    COUNTIF(h.missing_category_parent_flag = 1) AS missing_category_parent_rows,

    ROUND(SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'actual_neutral_or_mixed'), COUNT(*)), 4) AS neutral_or_mixed_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'unavailable'), COUNT(*)), 4) AS unavailable_rate,

    ROUND(b.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(
      SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)) - b.baseline_validation_rate,
      4
    ) AS lift_vs_baseline

  FROM hierarchy_rows h
  LEFT JOIN baseline_by_run b
    ON h.run_id = b.run_id

  GROUP BY
    h.run_id,
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,
    b.baseline_validation_rate
),

repeatability AS (
  SELECT
    grain_level,
    hierarchy_path,

    COUNT(DISTINCT test_run) AS runs_present,
    COUNTIF(row_count >= 30) AS runs_with_enough_rows,

    COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) AS positive_lift_runs,
    COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(row_count >= 30) < 2
        THEN 'too_small'

      WHEN COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) >= 2
        THEN 'repeat_positive'

      WHEN COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) >= 2
        THEN 'repeat_negative'

      ELSE 'mixed_or_unclear'
    END AS repeatability_label

  FROM node_by_run_raw
  GROUP BY
    grain_level,
    hierarchy_path
),

node_by_run_final AS (
  SELECT
    n.*,

    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    ROUND(SAFE_DIVIDE(n.missing_core_area_parent_rows, n.row_count), 4) AS missing_core_area_parent_rate,
    ROUND(SAFE_DIVIDE(n.missing_category_parent_rows, n.row_count), 4) AS missing_category_parent_rate,

    CASE
      WHEN n.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN n.row_count >= 100 AND ABS(n.lift_vs_baseline) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label,

    CASE
      WHEN n.grain_level = 'metric'
        AND n.missing_category_parent_rows > 0
        THEN 'metric_has_missing_category_parent'

      WHEN n.grain_level IN ('category', 'metric')
        AND n.missing_core_area_parent_rows > 0
        THEN 'has_missing_core_area_parent'

      WHEN n.unavailable_rate > 0.05
        THEN 'validation_availability_check_needed'

      ELSE 'ok'
    END AS data_shape_note

  FROM node_by_run_raw n
  LEFT JOIN repeatability r
    ON n.grain_level = r.grain_level
   AND n.hierarchy_path = r.hierarchy_path
),

node_all_runs_raw AS (
  SELECT
    'ALL_SELECTED_RUNS' AS test_run,
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,

    COUNT(*) AS row_count,
    COUNT(DISTINCT h.game_id) AS distinct_game_count,
    COUNT(DISTINCT h.claim_key) AS unique_claim_count,
    COUNT(DISTINCT h.claim_type) AS distinct_claim_type_count,
    COUNT(DISTINCT h.claim_layer) AS distinct_claim_layer_count,

    SUM(h.validated_flag_int) AS validated_count,
    COUNTIF(h.validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(h.validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(h.validation_result = 'unavailable') AS unavailable_count,

    COUNTIF(h.missing_core_area_parent_flag = 1) AS missing_core_area_parent_rows,
    COUNTIF(h.missing_category_parent_flag = 1) AS missing_category_parent_rows,

    ROUND(SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'actual_neutral_or_mixed'), COUNT(*)), 4) AS neutral_or_mixed_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'unavailable'), COUNT(*)), 4) AS unavailable_rate,

    ROUND(b.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(
      SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)) - b.baseline_validation_rate,
      4
    ) AS lift_vs_baseline

  FROM hierarchy_rows h
  CROSS JOIN baseline_all_runs b

  GROUP BY
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,
    b.baseline_validation_rate
),

node_all_runs_final AS (
  SELECT
    n.*,

    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    ROUND(SAFE_DIVIDE(n.missing_core_area_parent_rows, n.row_count), 4) AS missing_core_area_parent_rate,
    ROUND(SAFE_DIVIDE(n.missing_category_parent_rows, n.row_count), 4) AS missing_category_parent_rate,

    CASE
      WHEN n.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN n.row_count >= 100 AND ABS(n.lift_vs_baseline) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label,

    CASE
      WHEN n.grain_level = 'metric'
        AND n.missing_category_parent_rows > 0
        THEN 'metric_has_missing_category_parent'

      WHEN n.grain_level IN ('category', 'metric')
        AND n.missing_core_area_parent_rows > 0
        THEN 'has_missing_core_area_parent'

      WHEN n.unavailable_rate > 0.05
        THEN 'validation_availability_check_needed'

      ELSE 'ok'
    END AS data_shape_note

  FROM node_all_runs_raw n
  LEFT JOIN repeatability r
    ON n.grain_level = r.grain_level
   AND n.hierarchy_path = r.hierarchy_path
)

SELECT *
FROM node_all_runs_final

UNION ALL

SELECT *
FROM node_by_run_final

ORDER BY
  CASE WHEN test_run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  CASE grain_level
    WHEN 'core_area' THEN 1
    WHEN 'category' THEN 2
    WHEN 'metric' THEN 3
    ELSE 4
  END,
  CASE exploratory_label
    WHEN 'opportunity_candidate' THEN 1
    WHEN 'warning_candidate' THEN 2
    WHEN 'high_volume_neutral' THEN 3
    WHEN 'review' THEN 4
    ELSE 5
  END,
  row_count DESC,
  ABS(lift_vs_baseline) DESC;

-findings
Hierarchy Findings
1. Core Area level
Core Area	Result	Read
Offensive Output	+4.72 pts, repeat-positive	Clean opportunity
Disruption and Turnovers	-5.88 pts, repeat-negative	Clean warning
Scoring Efficiency	-1.82 pts, mixed	Noisy / split
Defensive Control	+4.12 pts, mixed	Interesting but not clean

Translation:
Offensive Output is the strongest broad neighborhood. Disruption and Turnovers is the clearest “slow down” neighborhood.

2. Category level
Clean opportunity categories
Offensive Output > Passing Game
Row Count: 322
Validation Rate: 55.28%
Lift: +6.34 pts
Repeatability: repeat_positive

Offensive Output > Rushing Game
Row Count: 309
Validation Rate: 53.72%
Lift: +4.78 pts
Repeatability: repeat_positive

Finding: Passing and rushing both work better when they are inside the clean Offensive Output hierarchy.

Clean warning categories
Scoring Efficiency > Scoring Production
Row Count: 685
Validation Rate: 43.94%
Lift: -5.00 pts
Repeatability: repeat_negative

Scoring Efficiency > Red Zone Finish
Row Count: 319
Validation Rate: 44.51%
Lift: -4.43 pts
Repeatability: repeat_negative

Disruption and Turnovers > Turnovers
Row Count: 305
Validation Rate: 43.93%
Lift: -5.01 pts
Repeatability: repeat_negative

Finding: The danger zones are very consistent: scoring production, red zone finish, and turnovers.

3. Metric level
Best clean opportunity metrics
Offensive Output > Offensive Rhythm > yards_per_play
Row Count: 338
Validation Rate: 55.92%
Lift: +6.97 pts
Repeatability: repeat_positive

Scoring Efficiency > Scoring Production > points_per_play
Row Count: 345
Validation Rate: 55.36%
Lift: +6.42 pts
Repeatability: repeat_positive

Offensive Output > Passing Game > yards_per_pass
Row Count: 322
Validation Rate: 55.28%
Lift: +6.34 pts
Repeatability: repeat_positive

Offensive Output > Rushing Game > yards_per_rush
Row Count: 309
Validation Rate: 53.72%
Lift: +4.78 pts
Repeatability: repeat_positive

Finding: The cleanest support family is:

yards_per_play
points_per_play
yards_per_pass
yards_per_rush

That is a very clear efficiency cluster.

4. Metric warning signals
Scoring Efficiency > Scoring Production > td_rate
Row Count: 340
Validation Rate: 32.35%
Lift: -16.59 pts
Repeatability: repeat_negative
Neutral/Mixed Rate: 50.59%

Scoring Efficiency > Red Zone Finish > red_zone_efficiency
Row Count: 319
Validation Rate: 44.51%
Lift: -4.43 pts
Repeatability: repeat_negative

Disruption and Turnovers > Turnovers > turnover_margin_per_game
Row Count: 305
Validation Rate: 43.93%
Lift: -5.01 pts
Repeatability: repeat_negative

Finding: The biggest red flag is td_rate. It has a huge negative lift and over half the rows are neutral/mixed. That screams:

“Do not let touchdown rate drive confident language.”

5. Neutral/context metrics
Scoring Efficiency > Drive Conversion > third_down_pct
Row Count: 314
Validation Rate: 49.36%
Lift: +0.42 pts

Offensive Output > Offensive Rhythm > 1st_down_rate
Row Count: 346
Validation Rate: 47.98%
Lift: -0.97 pts

Finding: Conversion metrics are mostly context. They are not worthless, but they should not steer the model.

The important data-quality catch

Your worry about the underlying data is valid. The query surfaced a lot of rows with:

missing_core_area_parent
metric_has_missing_category_parent

That does not automatically mean the data is bad, but it does mean the hierarchy is not always complete for every claim row.

So the safer interpretation is:

Trust the clean hierarchy paths first. Treat missing-parent rows as useful clues, not final truth.

The cleanest paths are the ones with:

data_shape_note = ok

That is where the most reliable pattern map comes from.

Big takeaway
Promote / explore:
- Offensive Output
- Passing Game inside Offensive Output
- Rushing Game inside Offensive Output
- yards_per_play
- points_per_play
- yards_per_pass
- yards_per_rush

Use caution / soften:
- Disruption and Turnovers
- Turnovers
- turnover_margin_per_game
- Scoring Production
- Red Zone Finish
- td_rate
- red_zone_efficiency

Context only:
- third_down_pct
- 1st_down_rate

Review:
- Defensive Control
- Scoring Suppression
- points_allowed_per_play


My recommendation
0. First: fix/validate hierarchy metadata before adding new features

Because the hierarchy work exposed lots of missing_core_area_parent and metric_has_missing_category_parent, I would do one cleanup/audit first.

Goal: make sure Level 3 is attaching canonical core_area, category, and metric-family metadata from metric_registry.py, not trusting stale parser metadata.

This matters because your new strongest findings depend on paths like:

Offensive Output > Passing Game > yards_per_pass
Offensive Output > Rushing Game > yards_per_rush
Scoring Efficiency > Scoring Production > points_per_play
Disruption and Turnovers > Turnovers > turnover_margin_per_game

If the hierarchy is messy, Level 4 rules will be messier too.

Ordered implementation plan
1. Add a Level 3 “clean hierarchy context” feature first

File likely: agg/gamelens_training/update_claim_training_features.py

Add fields like:

registry_core_area
registry_category
registry_metric_family
clean_hierarchy_path_flag
missing_hierarchy_parent_flag

This is boring, but it is the foundation. 🧱

Why first: before adding smarter features, we need confidence that the rows are grouped correctly.

2. Add offensive_efficiency_support_v1

This is the cleanest new contender.

Based on the hierarchy work, the strongest support cluster is:

yards_per_play
points_per_play
yards_per_pass
yards_per_rush

Level 3 should compute:

offensive_efficiency_support_signal
offensive_efficiency_support_score
offensive_efficiency_metric_family

Possible buckets:

strong_support
clear_support
mixed
against_claim
unavailable

Level 4 should later decide:

Does this allow stronger language?
Measured language only?
Context only?

This is where I’d start.

3. Add volatile_finish_warning_v1

This is the opposite side of the map.

The warning cluster is:

td_rate
red_zone_efficiency
turnover_margin_per_game

This should not be a support feature.

Level 3 should compute:

volatile_finish_warning_signal
volatile_finish_warning_score
volatile_metric_family

Possible buckets:

high_warning
soft_warning
neutral
unavailable

Level 4 use: soften language, block boosts, or add caution metadata.

This fits your current Level 4 boundary perfectly because Level 4 is already an “editor,” not a prediction layer.

4. Promote Feature 5/6 style logic into Level 3 as matchup-context features

These were two of the better tested features:

defense_vs_dynamite_v0
explosive_offense_warning_v0

I would combine them carefully into Level 3 as:

opponent_explosiveness_context_v1
defense_vs_explosive_offense_context_v1

Purpose: tell Level 4 when a defensive claim is clean versus fragile.

Example language behavior later:

Strong defense + opponent not explosive = can support defensive language
Strong defense + explosive opponent = soften
Weak support + explosive opponent = warning
5. Add rushing_efficiency_support_v1, but keep it scoped

Feature 9 was good, but narrow.

Use it only for:

Rushing Game
yards_per_rush
rushing-related claim language

Do not make it broad “game control.”

Level 3 field:

rushing_efficiency_support_signal

Feature 10 showed that broad rushing control is not ready.

6. Revisit Level 4 registry rules after Level 3 fields exist

Your current Level 4 summary says:

Strong allowlist:
- points_allowed_per_play
- 1st_down_rate

Watch:
- points_per_play

Blocked:
- red_zone_efficiency
- turnover_margin_per_game
- td_rate
- yards_per_play
- yards_per_pass
- yards_per_rush

That was correct for v0.1, but today’s hierarchy work challenges part of it.

I would not immediately unblock everything. I’d revise like this:

Move to review/watch:
- yards_per_play
- yards_per_pass
- yards_per_rush

Consider promoting:
- points_per_play, but only on clean scoped surfaces

Keep blocked/caution:
- td_rate
- red_zone_efficiency
- turnover_margin_per_game

That lets Level 4 evolve without getting reckless.

My suggested next concrete task

Before coding all this, run one more query:

Clean-path-only hierarchy validation

Filter to:

data_shape_note = 'ok'

Then confirm the same families still hold.

If they do, implement in this order:

1. clean_hierarchy_context_v1
2. offensive_efficiency_support_v1
3. volatile_finish_warning_v1
4. opponent_explosiveness_context_v1
5. rushing_efficiency_support_v1
6. Level 4 registry v0.3 calibration updates
Plain-English strategy

You now have more Level 3 contenders, but they should be treated like ingredients:

Level 3 measures the football evidence.
Level 4 decides whether the sentence gets louder, softer, or stays normal.

That keeps GameLens from turning into a hand-coded pick machine. It becomes what you wanted: a tool that makes the football explanation smarter.
--
--
########
Level 3 clean hierarchy context is populating correctly for metric rows. Registry parent recovery is working. Non-metric rows with missing Core Area are being flagged instead of silently trusted.

######
Completed Level 3 clean_hierarchy_context_v1 work for GameLens. This adds registry-backed hierarchy metadata to each claim row so future feature engineering can rely on canonical Core Area → Category → Metric paths instead of inconsistent parser metadata. The update added registry fields, clean hierarchy path/status fields, and flags for clean hierarchy paths and missing parent metadata. Dry-run succeeded on 6,775 rows for larger_240_level3_qa_20260517, with 1,791 registry_metric_clean rows, 3,537 registry_metric_parent_recovered rows, 834 row_metadata_core_area rows, and 613 row_metadata_missing_core_area rows. Existing Level 3 score coverage remained intact, and follow-up spot checks confirmed key metrics mapped correctly to expected hierarchy paths. A mismatch audit returned zero rows, so the registry recovery behavior is working as intended. No Level 4 rules, frontend behavior, matchup lean, confidence, Model Trust, or winner logic were changed.