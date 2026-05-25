# GameLens Tomorrow Starter — Admin Claim Health MVP Follow-Up

## Where We Left Off

Today we completed the first functional full-stack version of the GameLens Admin Claim Health dashboard.

The dashboard is now available through the main app navigation as an **Admin** tab for admin users. It routes to:

```text
/admin/claim-health

This page is powered by the backend aggregate claim-health API:

GET /admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025

The purpose of this dashboard is to review aggregate claim-validation health.

It answers:

Were GameLens pregame football claims supported by postgame data?

It is not measuring:

winner prediction accuracy
betting success
model profitability
public-facing performance
game-by-game drilldown

This is an internal/admin QA dashboard.

Current Working State

The following pieces are now working:

Cloud Run backend builds successfully
Admin Claim Health API exists
Admin Claim Health frontend page exists
Backend admin auth protects /admin/gamelens/claim-health
/me endpoint exists and returns user/admin context
Frontend uses /me to decide whether to show the Admin tab
Admin tab appears for admin users
Admin tab routes to /admin/claim-health
Dashboard loads real aggregate 2025 claim-health data
Frontend handles 401/403/error states instead of blanking
Feature Scorecard labels were fixed so buckets are readable
Important Security / Auth Setup

Backend security is the real protection.

The admin API is protected by backend admin auth:

require_admin_auth

Expected behavior:

User State	Expected Result
No token	401
Signed-in non-admin	403
Signed-in admin	200

The frontend tab is only a UX convenience.

The app now calls:

GET /me

Expected /me response:

{
  "email": "user@example.com",
  "role": "admin",
  "active": true,
  "is_admin": true
}

The Admin tab appears only when:

is_admin === true
Current Dashboard Sections

The Admin Claim Health dashboard currently shows:

Summary cards
Coverage
No-claim games
Baseline validation rate
Claim rows
Neutral/mixed rate
Core Area Health chart
Offensive Efficiency Feature Scorecard chart
Category Health table
Confidence by Core Area table
Claim Surface Health table
Known Current State: It Works, But It Is Not Polished

The dashboard is functional but still visually rough.

Known areas to review tomorrow:

Chart label spacing
Table density
Whether the chart bars are readable enough
Whether the dashboard explains itself clearly
Whether missing_category / missing_core_area rows should be visually softened
Whether the Confidence by Core Area table now reads correctly after pivot fix
Whether the Feature Scorecard labels are now readable after the bucket-label fix
Whether we need a small “How to read this page” explanation box

Do not start by adding major new features.

Start with visual QA and interpretation.

Useful Interpretation From Current Data

Baseline claim validation is around:

49.1%

This is the comparison point for all sections.

Current high-level reads from the API:

Offensive Output is the strongest broad Core Area
Defensive Control is also slightly above baseline
Scoring Efficiency is slightly below baseline overall
Disruption and Turnovers is weaker/noisier
Passing Game appears strong within Offensive Output
Pressure and Turnovers appear weak/noisy
Feature Scorecard shows a useful ladder:
repeat_positive_strong is clearly above baseline
repeat_positive_supportive is above baseline
caution/mixed/opposing buckets are weaker

Important: this is claim validation, not winner accuracy.

Suggested Tomorrow Plan
Step 1 — Quick Functional QA

Open the app and confirm:

Admin tab still appears
Admin routes to /admin/claim-health
Dashboard loads
Refreshing /admin/claim-health works
No blank page
No console errors
/me returns expected admin data
/admin/gamelens/claim-health returns data for admin session

This should be quick.

Step 2 — Visual QA Pass

Review each dashboard section and write down what is confusing.

Check:

Are chart labels readable?
Are table headers clear?
Are values too cramped?
Are percentages obvious?
Are baseline comparisons easy to see?
Do red/green deltas feel helpful or too loud?
Do missing_core_area / missing_category rows need explanation?

Do not redesign yet. Just make a punch list.

Step 3 — Interpretation Pass

Ask:

What is this dashboard telling us?

Focus on:

Which Core Areas validate above baseline?
Which categories are carrying the model?
Which categories are dragging it down?
Does confidence actually help?
Does the feature scorecard separate stronger claim contexts from weaker ones?
Which claim surfaces seem trustworthy?
Which claim surfaces need softer language?

This is probably the most valuable next step.

Step 4 — Decide Small UX Improvements

Possible small improvements:

Add a short “How to read this dashboard” card
Add baseline callout text
Add minimum sample-size note
Improve chart/table spacing
Rename unclear labels
Add tooltip explanations
Add a run_id / season selector later, but not immediately
Add a small sample badge consistently

Avoid:

game drilldown
public accuracy framing
betting language
new model logic
major redesign
automatic recommendations
Suggested First Question Tomorrow

Start tomorrow by asking:

Can we do a QA pass on the Admin Claim Health dashboard and separate visual polish issues from actual data interpretation issues?

That should keep the work focused.

Tomorrow’s Likely Best Outcome

By the end of tomorrow, the goal should be:

A short list of frontend polish fixes
A clear read of what the dashboard is saying
A decision on whether the Feature Scorecard is useful enough to inform future Level 4 language calibration
No new giant scope opened unless necessary
Current Mental Model

GameLens is becoming a claim-quality learning tool.

The Admin Claim Health dashboard is the first real admin view that lets us inspect whether GameLens is telling accurate football stories at the claim level.

This is valuable because a game pick can be wrong while a specific football claim can still be right.

Example:

GameLens says Team A has the better passing profile.

Team A loses the game.

But Team A actually does produce the better passing efficiency.

That claim can still validate.

This dashboard helps separate:

Was the winner correct?

from:

Was the football reasoning truthful?

That distinction is the whole point.


My suggested tomorrow opener: **“Let’s QA the dashboard as a user, not as a coder.”** That should keep you out of the weeds for the first 20 minutes.

#######

Admin Dashboard v2 — Calibration + Claim Health
0. How to read this dashboard

Purpose:

This dashboard tracks whether GameLens is giving useful game-level reads and truthful football explanations.

Game-level calibration asks:
When GameLens gave a Matchup Lean, did the final result align?

Claim health asks:
When GameLens made a football claim, did postgame data support that claim?

These are related, but not the same.

Add row/tooltips everywhere. No mystery meat labels. 🥩

1. Calibration Over Time

This is the new line graph.

Question

How is GameLens performing over time?

X-axis options
Day
Week
Season Phase

Default should be:

Week

Day can be noisy for NFL. Week is the cleaner starting point.

Season phase groupings

Use these as filter chips or dropdown options:

Full Season
Early Season
Mid Season
Late Season
Postseason

Suggested default definitions:

Early Season: Weeks 1–5
Mid Season: Weeks 6–12
Late Season: Weeks 13–18
Postseason: Wild Card through Super Bowl

Week 1 should get a special note if no claim rows exist because of missing pregame ranking/window context.

Y-axis
Rate %
Legend lines

Use clearer names than “baseline” if possible:

Line	Meaning
Overall Claim Validation	Percent of eligible claim rows that validated
Game Pick Accuracy	Percent of game-level picks/leans that were correct
Selected Segment Validation	Validation rate for selected Core Area / Category / Feature / Matchup group
Tooltip per point

Example:

Week 6

Overall Claim Validation
49.8%
614 eligible claims
306 validated
187 not validated
121 neutral/mixed
18 unavailable

Game Pick Accuracy
56.3%
16 games with pick
9 correct
7 incorrect
2 no-pick games

Selected Segment: Offensive Output
58.7%
126 eligible claims
+8.9 pts vs claim baseline

This chart should be first because it answers:

“Are we getting healthier over time?”

2. Game-Level Calibration
Question

When GameLens gave a game-level read, how often did that read align with the final result?

Rows
No Clear Edge
Thin Edge
Mixed Profile
Clear Lean
Strong Profile
Columns
Low Confidence
Medium Confidence
High Confidence
Cell values
games
correct %
incorrect %
no pick %
avg margin
close miss %
severe miss %

This directly answers:

“Was Medium Confidence actually better than Low Confidence?”

Tooltip should explain:

Correct % is based on games where GameLens made a pick or directional lean. No Pick is tracked separately and should not automatically count as wrong.
3. Matchup Lean × Core Area Alignment
Question

When Matchup Lean said one thing, did Core Areas confirm it, split, or push back?

Rows
confirmed_edge
split_profile
conflicting_profile
coin_flip_profile
no_clear_edge
Columns
Low Confidence
Medium Confidence
High Confidence
Cell values
game_count
correct_rate
avg_core_gap
avg_signal_gap
avg_final_margin

This is where your screenshots matter most.

It connects:

Clear Lean / Medium Confidence

to:

Core Areas confirmed the lean
Core Areas split
Core Areas conflicted
Core gap was tiny

This should probably be one of the most important dashboard sections.

4. Pillar Health Matrix

This replaces the idea of making “Claim Surface Health” a headline section.

Question

Which football pillars are producing truthful claims?

Primary dimensions
Core Area
Category
Metric
Week
Season Phase
Confidence
Measures
claim_rows
validated_claims
validation_rate
neutral_mixed_rate
not_validated_rate
lift_vs_claim_baseline
games_represented
Suggested rows

Start with:

Core Area > Category

Expandable later to:

Core Area > Category > Metric

Example:

Offensive Output
  Passing Game
  Rushing Game
  Offensive Rhythm

Defensive Control
  Scoring Suppression
  Defensive Efficiency

Disruption and Turnovers
  Pressure
  Turnovers

Tooltip:

Pillar Health measures claim validation, not game winner accuracy. It asks whether claims inside this football area were supported by postgame data.
5. Weekly Trend View

This is related to the line graph but more table-like.

Question

What happened each week?

Rows
Week 1
Week 2
Week 3
...
Week 18
Wild Card
Divisional
Conference Championship
Super Bowl
Columns
games_total
games_with_claims
claim_rows
claim_validation_rate
game_pick_correct_rate
no_pick_rate
avg_confidence
top_core_area
weakest_core_area

This is the “box score” underneath the line graph.

Line graph = visual trend.
Weekly Trend View = readable audit table.

6. Feature Health Matrix

Do not leave this as only Offensive Efficiency.

Question

Which engineered features are earning trust?

Feature families
offensive_efficiency_support_v1
offense_finish_score
defensive_suppression_score
two_way_context
clean_hierarchy_context_v1
Columns
bucket/group
claim_rows
games_represented
validation_rate
lift_vs_claim_baseline
neutral_mixed_rate
not_validated_rate
sample_warning
Important split

Separate into two groups:

Football Calibration Features
offensive_efficiency_support_v1
offense_finish_score
defensive_suppression_score
two_way_context
Data Quality / Metadata Features
clean_hierarchy_context_v1
missing_hierarchy_parent_flag
registry_core_area
registry_category

That prevents comparing football signals to metadata cleanup signals.

7. Technical Surface Debug

Rename current:

Claim Surface Health

to:

Technical Claim Surface Debug

or move it below the fold.

Why?

Because claim type/layer is useful for debugging, but it is not the main product lens. The main product lens is:

Game read
Core Areas
Football pillars
Feature health
Time trend

Claim layer can stay, but it should stop being treated like the star of the show.

Baseline language

Use three explicit baselines.

Claim Baseline
Of eligible claim rows, what percent validated?
Game Pick Baseline
Of games where GameLens made a pick/lean, what percent were correct?
Segment Baseline
Of claims inside the selected segment, what percent validated?

Do not just say “baseline” by itself. That will confuse future-you fast.

Updated API sections

This is the consolidated backend target:

"sections": {
    # new top-level calibration
    "calibration_over_time": get_calibration_over_time(run_id, season, grain="week"),
    "game_level_calibration": get_game_level_calibration(run_id, season),
    "matchup_lean_matrix": get_matchup_lean_matrix(run_id, season),
    "core_area_alignment_matrix": get_core_area_alignment_matrix(run_id, season),

    # pillar / claim truth
    "pillar_weekly_health": get_pillar_weekly_health(run_id, season),
    "pillar_health_matrix": get_pillar_health_matrix(run_id, season),

    # feature health
    "feature_health_matrix": get_feature_health_matrix(run_id, season),

    # existing / demoted
    "core_area_matrix": get_core_area_matrix(run_id),
    "category_matrix": get_category_matrix(run_id),
    "confidence_core_area_matrix": get_confidence_core_area_matrix(run_id),
    "surface_matrix": get_surface_matrix(run_id)
}

Add metadata:

"date_grains": ["day", "week", "season_phase"],
"season_phase_groups": {
    "early_season": "Weeks 1-5",
    "mid_season": "Weeks 6-12",
    "late_season": "Weeks 13-18",
    "postseason": "Wild Card through Super Bowl"
},
"formula_notes": {
    "claim_validation_rate": "validated / eligible claim rows",
    "game_pick_accuracy": "correct picks / games with pick or lean",
    "selected_segment_validation": "validated claims in selected segment / eligible claims in selected segment"
}
Best copy/paste build request
We need to evolve the Admin Claim Health dashboard into an Admin Calibration + Claim Health dashboard.

Do not remove existing sections yet.
Do not add game drilldown.
Do not change model logic.
Do not add public/betting language.

Add these new aggregate-only dashboard sections:

## 1. Calibration Over Time

Purpose:
Show GameLens health over time.

Default x-axis grain:
- week

Supported grains:
- day
- week
- season_phase

Season phase groups:
- Full Season
- Early Season: Weeks 1–5
- Mid Season: Weeks 6–12
- Late Season: Weeks 13–18
- Postseason: Wild Card through Super Bowl

Lines:
1. Overall Claim Validation
2. Game Pick Accuracy
3. Selected Segment Validation

Each row should include:
- period_label
- period_start
- period_end
- grain
- season_phase
- claim_rows
- eligible_claim_rows
- validated_claims
- not_validated_claims
- neutral_mixed_claims
- unavailable_claims
- claim_validation_rate
- games_total
- games_with_claims
- games_with_pick
- correct_picks
- incorrect_picks
- no_pick_games
- game_pick_correct_rate
- selected_segment_label
- selected_segment_claim_rows
- selected_segment_validation_rate
- selected_segment_lift_vs_claim_baseline

Also include tooltip/formula metadata explaining:
- claim validation rate
- game pick accuracy
- selected segment validation
- the different denominators

## 2. Game-Level Calibration

Question:
When GameLens gave a game-level read, how often did that read align with the final result?

Rows:
- No Clear Edge
- Thin Edge
- Mixed Profile
- Clear Lean
- Strong Profile

Columns:
- Low Confidence
- Medium Confidence
- High Confidence

Each cell:
- game_count
- correct_count
- incorrect_count
- no_pick_count
- correct_rate
- incorrect_rate
- no_pick_rate
- avg_final_margin_abs
- close_miss_count
- severe_miss_count

## 3. Matchup Lean × Core Area Alignment

Question:
When Matchup Lean said one thing, did Core Areas confirm it, split, or push back?

Rows:
- confirmed_edge
- split_profile
- conflicting_profile
- coin_flip_profile
- no_clear_edge

Columns:
- Low Confidence
- Medium Confidence
- High Confidence

Each cell:
- game_count
- correct_rate
- avg_core_gap
- avg_signal_gap
- avg_final_margin_abs

## 4. Pillar Health Matrix

Question:
Which football pillars are producing truthful claims?

Dimensions:
- Core Area
- Category
- Metric
- Week
- Season Phase
- Confidence

Measures:
- claim_rows
- eligible_claim_rows
- validated_claims
- validation_rate
- neutral_mixed_rate
- not_validated_rate
- lift_vs_claim_baseline
- games_represented

Default display:
Core Area > Category.

## 5. Weekly Trend View

Question:
What happened each week?

Rows:
- Week 1 through Week 18
- Wild Card
- Divisional
- Conference Championship
- Super Bowl

Columns:
- games_total
- games_with_claims
- claim_rows
- claim_validation_rate
- game_pick_correct_rate
- no_pick_rate
- avg_confidence
- top_core_area
- weakest_core_area

## 6. Feature Health Matrix

Replace the current single Offensive Efficiency Feature Scorecard with a broader feature matrix.

Feature families:
- offensive_efficiency_support_v1
- offense_finish_score
- defensive_suppression_score
- two_way_context
- clean_hierarchy_context_v1

Columns:
- feature_family
- bucket_or_group
- claim_rows
- games_represented
- validation_rate
- lift_vs_claim_baseline
- neutral_mixed_rate
- not_validated_rate
- sample_warning

Split feature display into:
1. Football Calibration Features
2. Data Quality / Metadata Features

## 7. Technical Surface Debug

Demote or rename current Claim Surface Health to:
Technical Claim Surface Debug.

This can remain available but should not be the primary dashboard lens.

Important UX:
- Add row tooltips throughout.
- Define every rate and denominator.
- Show sample sizes beside percentages.
- Clearly separate claim-level validation from game-level pick accuracy.

#############

# Daily Tracker — Admin Calibration + Claim Health API

## Goal

Today’s goal was to evolve the existing Admin Claim Health API into a backend-ready Admin Calibration + Claim Health API.

The point was not to build frontend yet. The point was to give Lovable a clean backend response contract that can support a tabbed admin dashboard without guessing, hardcoding, or inventing fields.

The endpoint remains:

GET /admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025

## Guardrails followed

- Backend only
- No frontend implementation
- No game drilldown
- No model logic changes
- No matchup lean logic changes
- No confidence logic changes
- No winner/pick logic changes
- No betting/public scoreboard language
- Existing sections preserved
- Admin auth kept in place

## What changed

The existing Claim Health API was expanded into a tab-ready Admin Calibration + Claim Health response.

Added top-level metadata:

- `default_tab`
- `tabs`
- `formula_notes`
- `season_phase_groups`

The default tab is now:

- `overview`

The tab structure now includes:

- Overview
- Game Calibration
- Core Area Alignment
- Pillar Health
- Feature Health
- Technical Debug

Each tab includes:

- id
- label
- description
- sections

This gives Lovable enough structure to build the Admin page from the API contract instead of guessing where sections belong.

## New aggregate sections added

### 1. `calibration_over_time`

Purpose:

Shows how GameLens health changes over time.

Default grain:

- week

Future-supported grains:

- day
- week
- season_phase

This section includes both claim-level and game-level calibration fields, including:

- claim rows
- eligible claim rows
- validated claims
- not validated claims
- neutral/mixed claims
- unavailable claims
- claim validation rate
- games total
- games with claims
- games with pick
- correct picks
- incorrect picks
- no-pick games
- game pick correct rate
- selected segment validation fields

This is intended to become the main Admin line graph.

### 2. `game_level_calibration`

Purpose:

Answers whether GameLens game-level reads aligned with final results.

Grouped by:

- profile_strength_label
- outcome_confidence_label

This should help answer questions like:

- Was Medium Confidence actually better than Low Confidence?
- Are Strong Profile games behaving better than Thin Edge games?
- Are No Pick games being tracked separately instead of treated as wrong?

### 3. `core_area_alignment_matrix`

Purpose:

Connects Matchup Lean behavior to Core Area context.

Grouped by:

- profile_type
- outcome_confidence_label

This helps show whether outcomes were better when Core Areas confirmed the lean, split, conflicted, or looked coin-flippy.

### 4. `pillar_health_matrix`

Purpose:

Shows claim truth by football area.

Default grouping:

- core_area
- category

This is the cleaner product-facing replacement for making claim surface/layer the main dashboard story.

### 5. `pillar_weekly_health`

Purpose:

Gives a week-by-week audit table under the line graph.

Includes:

- games total
- games with claims
- claim rows
- claim validation rate
- game pick correct rate
- no-pick rate
- average confidence
- top core area
- weakest core area

### 6. `feature_health_matrix`

Purpose:

Broadens feature health beyond the old offensive efficiency scorecard.

Feature families included:

Football Calibration Features:

- offensive_efficiency_support_v1
- offense_finish_score
- defensive_suppression_score
- two_way_context

Data Quality / Metadata Features:

- clean_hierarchy_context_v1

This keeps football signal features separate from metadata/data-quality features, which avoids comparing unlike things.

## Old sections preserved

The old sections still exist:

- `core_area_matrix`
- `category_matrix`
- `confidence_core_area_matrix`
- `feature_scorecard`
- `surface_matrix`

`surface_matrix` was intentionally preserved but demoted in metadata as a technical debug section.

This is important because claim type/layer is useful for QA, but it should not be the headline product lens.

## Formula metadata added

Added `formula_notes` to define the major rates and denominators.

Important distinction:

`claim_validation_rate`

Means:

- validated claims / eligible claim rows

This measures whether football explanation language was truthful.

`game_pick_correct_rate`

Means:

- correct picks / games with actual directional picks

This measures game-level outcome alignment.

`selected_segment_validation_rate`

Means:

- validation rate within the selected segment

This allows future frontend filtering by Core Area, category, feature, confidence bucket, or season phase.

This was added because using a generic word like “baseline” will confuse future dashboard interpretation.

## Season phase metadata added

Added `season_phase_groups`:

- early_season: Weeks 1–5
- mid_season: Weeks 6–12
- late_season: Weeks 13–18
- postseason: Wild Card through Super Bowl

This supports future filter chips or dropdowns.

## Testing completed

First local import test passed:

```bash
python -c "import app; print('app imported ok')"

Then local endpoint test was run:

http://127.0.0.1:8080/admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025

Initial local request hit auth because the admin route uses admin auth, not the regular Firebase auth decorator.

After resolving local admin auth/bypass behavior, the endpoint reached BigQuery.

A BigQuery error appeared:

Aggregations of aggregations are not allowed

This was caused by the first version of calibration_over_time.

The query was fixed by restructuring the period aggregation so BigQuery only aggregates the raw date values once.

After the fix, the endpoint returned a successful full response.

Confirmed response included:

tabs
default_tab
formula_notes
season_phase_groups
section_metadata
sections
all new sections
all old sections
Current status

Backend goal achieved.

The Admin API is now ready to be handed to Lovable for a first frontend pass.

Lovable should use:

tabs as the source of truth for tab layout
section_metadata as the source of truth for titles, descriptions, chart types, primary metrics, and section placement
sections as the data source

Lovable should not invent new backend fields yet.

Known non-blocking polish items

These are not blockers before commit.

1. matchup_lean_matrix

The planning notes mentioned a possible matchup_lean_matrix, but the current API uses:

core_area_alignment_matrix

This section effectively covers the Matchup Lean × Core Area Alignment need.

Do not add a duplicate section unless the frontend later needs an alias.

2. date_grains

The planning notes mentioned top-level date_grains.

The current response exposes supported grains inside:

section_metadata.calibration_over_time.supported_grains

This is acceptable for the first frontend pass.

A future polish item could add top-level date_grains if desired.

3. surface_matrix title

surface_matrix is correctly marked as technical debug, but the title may still read like “Claim Surface Health.”

Future polish:

Rename display title to “Technical Claim Surface Debug”
4. No-pick display semantics

In some game-level calibration rows, no-pick groups can show correct_rate as 0.0 when there were no actual directional picks.

Future polish:

Make correct_rate null when the denominator has no picks
Let no_pick_rate carry the meaning instead
Lovable handoff note

Use the existing admin endpoint response to build a tabbed Admin Calibration + Claim Health dashboard.

Do not add frontend drilldown yet.

Recommended frontend behavior:

First tab: Overview
Main visual: Calibration Over Time line chart
Under it: Weekly trend table
Game Calibration tab: profile strength × confidence matrix
Core Area Alignment tab: profile type × confidence matrix plus preserved core area matrices
Pillar Health tab: core area/category matrix and weekly health table
Feature Health tab: feature health matrix and legacy feature scorecard
Technical Debug tab: surface matrix only

Important UX rules:

Show sample sizes beside percentages
Define every rate and denominator in tooltips
Clearly separate claim validation from game pick accuracy
Treat surface_matrix as debug, not the main dashboard story
Keep language internal/admin-only

One tiny thing before you commit: only include `auth/firebase_auth.py` in the commit if you actually changed it for local admin bypass. Otherwise leave it out.

------######

Massive Lovable prompt starting Frontend work:

We are ready to build the first frontend pass for the Admin Calibration + Claim Health dashboard.

This is an ADMIN-ONLY dashboard for GameLens.

Use the existing backend endpoint:

GET /admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025

Do not change backend logic.
Do not change model logic.
Do not add game drilldown.
Do not add betting/public scoreboard language.
Do not remove old sections yet.
Do not invent new API fields.
Do not rename backend fields.
Do not hardcode a separate dashboard contract if the API already provides metadata.

Most important frontend rule:
Use the API response as the source of truth.

Use:
- `tabs` for tab layout
- `default_tab` for initial selected tab
- `section_metadata` for section titles, descriptions, chart types, tab placement, and primary metrics
- `sections` for actual data
- `formula_notes` for tooltips/help text explaining rates and denominators
- `season_phase_groups` for season phase explanation/filter labels

## Brand / styling guardrail

Keep the existing GameLens brand, fonts, color palette, spacing, cards, borders, backgrounds, and component style.

Before adding new dashboard UI:
- Inspect the existing theme files, Tailwind config, CSS variables, global styles, layout components, cards, badges, table styles, and chart styles.
- Reuse existing typography and color tokens.
- Do not introduce a new font.
- Do not introduce a new color palette.
- Do not redesign the app shell/sidebar/header.
- Do not make this look like a generic SaaS dashboard.
- Match the current GameLens visual language.

If new colors are needed for charts, derive them from the existing palette or CSS variables. Keep contrast readable, but stay on-brand.

## Purpose of this Admin page

This dashboard tracks two related but different things:

1. Game-level calibration

Question:
When GameLens gave a Matchup Lean or directional game read, did the final result align?

This is outcome calibration.

2. Claim health

Question:
When GameLens made a football claim, did postgame data support that claim?

This is explanation/claim-quality calibration.

These are NOT the same thing.

The frontend must clearly separate:
- claim validation rate
- game pick correct rate
- selected segment validation rate

Do not label everything as “accuracy.” Use the API’s formula metadata.

## Top-level API fields

The response includes:

- `available`
- `scope`
- `run_id`
- `season`
- `generated_at`
- `coverage`
- `baseline`
- `default_tab`
- `tabs`
- `formula_notes`
- `season_phase_groups`
- `section_metadata`
- `sections`

### `available`

Boolean showing whether the response is usable.

### `scope`

Expected value:

`admin_calibration_claim_health`

This tells the frontend this is no longer only the old claim-health dashboard. It is the expanded calibration + claim health API.

### `run_id`

The source claim-training run.

Example:

`full_2025_reg_post_claim_matrix_pilot`

Display this somewhere small in the admin header or metadata area.

### `season`

The season being analyzed.

Example:

`2025`

### `generated_at`

UTC timestamp when the response was generated.

Display in a subtle “Last generated” area.

## Coverage section

`coverage` tells us how many expected games had claim rows.

Fields:

- `expected_games`
- `games_with_claims`
- `games_without_claims`
- `context_note`

Use this in the Overview tab.

Important:
The response may show Week 1 games without claims because pregame ranking/window context was unavailable. Display `context_note` as an info callout, not an error.

Recommended Overview card:

Title:
Coverage

Stats:
- Expected games
- Games with claims
- Games without claims

Then show `context_note` if present.

## Baseline section

`baseline` gives overall claim validation health.

Fields include:

- `claim_row_count`
- `eligible_claim_row_count`
- `game_count`
- `validated_count`
- `not_validated_count`
- `neutral_or_mixed_count`
- `unavailable_count`
- `validation_rate`
- `neutral_or_mixed_rate`

Use this in the Overview tab.

Important:
`validation_rate` is claim-level validation, not game winner accuracy.

Recommended cards:
- Overall Claim Validation
- Claim Rows
- Eligible Claim Rows
- Validated Claims
- Not Validated Claims
- Neutral/Mixed Claims
- Games Represented

## Formula notes

The response has `formula_notes` for:

### `claim_validation_rate`

Meaning:
How often GameLens claim language was supported by postgame metric validation.

Formula:
validated_claims / eligible_claim_rows

This is claim-quality calibration, not game winner accuracy.

### `game_pick_correct_rate`

Meaning:
How often games with an actual directional pick aligned with the final result.

Formula:
correct_picks / (correct_picks + incorrect_picks)

No-pick games are tracked separately and excluded from this denominator.

### `selected_segment_validation_rate`

Meaning:
The same claim-validation formula applied to a selected segment.

Default:
all claims in the returned period.

Use these notes in tooltips or an “How to read this dashboard” accordion.

## Season phase groups

The response includes:

- `early_season`: Weeks 1–5
- `mid_season`: Weeks 6–12
- `late_season`: Weeks 13–18
- `postseason`: Wild Card through Super Bowl

Use this for labels/tooltips. Do not invent different phase definitions.

## Tabs

Use the API’s `tabs` array directly.

Expected tabs:

1. Overview
2. Game Calibration
3. Core Area Alignment
4. Pillar Health
5. Feature Health
6. Technical Debug

Use `default_tab` to select the initial tab.

Default should be:

`overview`

Each tab object includes:

- `id`
- `label`
- `description`
- `sections`

Render the tab label and a short description at the top of each tab.

Do not create unrelated tabs.

## Section metadata

Use `section_metadata` to drive section rendering.

Each section can include:

- `title`
- `description`
- `chart_type`
- `primary_metric`
- `tab_id`
- `supported_grains`
- `grain_default`
- `compatibility`
- `section_role`

Important:
If `section_role` is `technical_debug`, treat it as lower priority and place it in the Technical Debug tab.

## Sections to render

The response contains these new sections:

- `calibration_over_time`
- `game_level_calibration`
- `core_area_alignment_matrix`
- `pillar_health_matrix`
- `pillar_weekly_health`
- `feature_health_matrix`

The response also preserves these older sections:

- `core_area_matrix`
- `category_matrix`
- `confidence_core_area_matrix`
- `feature_scorecard`
- `surface_matrix`

Do not remove the old sections yet.

## 1. Overview tab

Sections:

- `coverage`
- `baseline`
- `calibration_over_time`
- `game_level_calibration`

The Overview should answer:

“Is GameLens getting healthier over time?”

Recommended layout:

Top row:
- Coverage card
- Baseline claim validation card
- Games represented card
- Run metadata card

Main chart:
- Calibration Over Time line chart

Below:
- compact Game-Level Calibration summary

## 2. Calibration Over Time

Section key:

`sections.calibration_over_time`

This should be the main line chart.

Default grain:

`week`

Supported grains are in:

`section_metadata.calibration_over_time.supported_grains`

Supported:
- day
- week
- season_phase

For first frontend pass, render week by default. A grain selector can be added if easy, but do not overbuild.

Each row includes:

- `period_label`
- `period_start`
- `period_end`
- `grain`
- `season_phase`
- `claim_rows`
- `eligible_claim_rows`
- `validated_claims`
- `not_validated_claims`
- `neutral_mixed_claims`
- `unavailable_claims`
- `claim_validation_rate`
- `games_total`
- `games_with_claims`
- `games_with_pick`
- `correct_picks`
- `incorrect_picks`
- `no_pick_games`
- `game_pick_correct_rate`
- `selected_segment_label`
- `selected_segment_claim_rows`
- `selected_segment_validation_rate`
- `selected_segment_lift_vs_claim_baseline`

Render line chart with these lines:

1. Overall Claim Validation
   - field: `claim_validation_rate`

2. Game Pick Accuracy
   - field: `game_pick_correct_rate`

3. Selected Segment Validation
   - field: `selected_segment_validation_rate`

Use percentage formatting.

Handle nulls gracefully. Week 1 may have null claim validation because it has games but no claim rows.

Tooltip should show:

- period label
- period start/end
- claim validation %
- eligible claims
- validated claims
- not validated claims
- neutral/mixed claims
- unavailable claims
- game pick correct %
- games with pick
- correct picks
- incorrect picks
- no-pick games
- selected segment label
- selected segment validation %
- lift vs claim baseline

Important:
Do not hide no-pick games. They are important signal.

## 3. Game Calibration tab

Primary section:

`sections.game_level_calibration`

Purpose:
Shows whether game-level reads aligned with final results.

Grouped by:
- `profile_strength_label`
- `outcome_confidence_label`

Fields:

- `game_count`
- `correct_count`
- `incorrect_count`
- `no_pick_count`
- `correct_rate`
- `incorrect_rate`
- `no_pick_rate`
- `avg_final_margin_abs`
- `close_miss_count`
- `severe_miss_count`

Render as a matrix/table:

Rows:
- No Clear Edge
- Thin Edge
- Mixed Profile
- Clear Lean
- Strong Profile

Columns:
- Low
- Medium
- High

Each cell should show:
- games
- correct %
- no-pick %
- avg margin

Tooltip/details:
- correct count
- incorrect count
- no-pick count
- close misses
- severe misses

Important:
Correct rate is based on games with directional picks/leans. No-pick games should be tracked separately, not treated as wrong.

If a row has no directional picks, avoid visually implying the model was “0% correct.” Make no-pick status obvious.

## 4. Core Area Alignment tab

Sections:

- `core_area_alignment_matrix`
- `core_area_matrix`
- `confidence_core_area_matrix`

### `core_area_alignment_matrix`

Purpose:
Shows how Matchup Lean behaved when Core Areas confirmed, split, conflicted, or were coin-flippy.

Grouped by:

- `profile_type`
- `outcome_confidence_label`

Fields:

- `game_count`
- `correct_rate`
- `avg_core_gap`
- `avg_signal_gap`
- `avg_final_margin_abs`

Rows should use `profile_type`:

- confirmed_edge
- split_profile
- conflicting_profile
- coin_flip_profile
- no_clear_edge

Columns:
- Low
- Medium
- High

This should probably be one of the most important matrix sections.

### `core_area_matrix`

Legacy/preserved section.

Shows claim validation by broad football Core Area.

Use as supporting context.

### `confidence_core_area_matrix`

Legacy/preserved section.

Shows claim validation by confidence and Core Area.

Use below the main alignment matrix.

## 5. Pillar Health tab

Sections:

- `pillar_health_matrix`
- `pillar_weekly_health`
- `category_matrix`

### `pillar_health_matrix`

Purpose:
Shows which football pillars/categories are producing truthful claims.

Default grouping:
- `core_area`
- `category`

Fields:

- `claim_rows`
- `eligible_claim_rows`
- `validated_claims`
- `validation_rate`
- `neutral_mixed_rate`
- `not_validated_rate`
- `lift_vs_claim_baseline`
- `games_represented`

Render as grouped table:
Core Area > Category

Show:
- validation rate
- claim rows
- lift vs baseline
- neutral/mixed rate
- games represented

Recommended:
Use color carefully for lift:
- positive lift good
- negative lift caution
But keep existing brand colors.

Tooltip:
Pillar Health measures claim validation, not game winner accuracy.

### `pillar_weekly_health`

Purpose:
Readable weekly audit table under the line graph.

Fields:

- `game_week`
- `games_total`
- `games_with_claims`
- `claim_rows`
- `claim_validation_rate`
- `game_pick_correct_rate`
- `no_pick_rate`
- `avg_confidence`
- `top_core_area`
- `weakest_core_area`

Render as table.

Use percent formatting for rates.

Handle nulls gracefully.

### `category_matrix`

Legacy/preserved section.

This is similar to pillar health and can be shown lower on the tab or in an “Older compatibility matrix” subsection.

## 6. Feature Health tab

Sections:

- `feature_health_matrix`
- `feature_scorecard`

### `feature_health_matrix`

Purpose:
Shows which engineered features are earning trust.

Feature groups:

- Football Calibration Features
- Data Quality / Metadata Features

Feature families include:

- `offensive_efficiency_support_v1`
- `offense_finish_score`
- `defensive_suppression_score`
- `two_way_context`
- `clean_hierarchy_context_v1`

Fields:

- `feature_group`
- `feature_family`
- `bucket_or_group`
- `claim_rows`
- `eligible_claim_rows`
- `games_represented`
- `validation_rate`
- `lift_vs_claim_baseline`
- `neutral_mixed_rate`
- `not_validated_rate`
- `sample_warning`

Render grouped by `feature_group`, then `feature_family`.

Important:
Do not compare football features and metadata features as if they are the same kind of signal.

Football Calibration Features are about claim-language support.

Data Quality / Metadata Features are about hierarchy/metadata quality.

Show `sample_warning` where present, especially `low_sample`.

### `feature_scorecard`

Legacy/preserved offensive efficiency scorecard.

Keep it below the broader feature matrix for backwards compatibility.

## 7. Technical Debug tab

Section:

- `surface_matrix`

This section is preserved but demoted.

Use section metadata:

`section_role: technical_debug`

Do not treat it as a primary dashboard story.

Possible title:
Technical Claim Surface Debug

Purpose:
Useful for QA and regression checks by claim type/layer.

Fields:
- `claim_type`
- `claim_layer`
- `claim_row_count`
- `game_count`
- `validated_count`
- `not_validated_count`
- `neutral_or_mixed_count`
- `validation_rate`
- `neutral_or_mixed_rate`

Keep this below the fold or inside the Technical Debug tab only.

## Formatting rules

Rates:
Display as percentages with one decimal place where practical.

Examples:
- 0.4908 -> 49.1%
- 0.5738 -> 57.4%

Counts:
Display as whole numbers.

Lift:
Display as percentage points.

Example:
- 0.083 -> +8.3 pts
- -0.035 -> -3.5 pts

Null values:
Show as:
- `—`
not `0%`

This is important because null often means no denominator, not bad performance.

## UX notes

Add an “How to read this dashboard” info panel or accordion.

Must explain:

- Claim validation is not the same as game pick accuracy.
- No-pick games are tracked separately.
- Neutral/mixed claim results are useful calibration feedback.
- Surface Matrix is technical debug, not the headline story.
- Week 1 may have missing claim rows because pregame ranking/window context was unavailable.

## First-pass implementation goal

Build a useful first version, not a perfect final dashboard.

Priority order:

1. Preserve existing Admin page route and admin-only access.
2. Add tab layout using API `tabs`.
3. Add Overview with coverage, baseline, and calibration line chart.
4. Add Game Calibration matrix.
5. Add Core Area Alignment matrix.
6. Add Pillar Health tables.
7. Add Feature Health grouped table.
8. Move Surface Matrix into Technical Debug.
9. Keep brand styling unchanged.

Do not attempt game drilldown in this pass.
Do not add filters beyond what the API already supports unless simple.
Do not create new backend expectations.

## Acceptance criteria

The first frontend pass is acceptable if:

- Admin page loads with the existing brand style.
- The Overview tab opens by default.
- All API tabs render.
- Calibration Over Time line chart renders.
- Game-Level Calibration matrix renders.
- Core Area Alignment matrix renders.
- Pillar Health Matrix renders.
- Pillar Weekly Health table renders.
- Feature Health Matrix renders.
- Old sections are still accessible.
- Surface Matrix appears only under Technical Debug.
- Formula/tooltips clarify denominators.
- Null values are not displayed as zero.
- No public/betting language is introduced.

##### 
troubleshooting frontend