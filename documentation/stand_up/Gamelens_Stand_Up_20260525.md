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