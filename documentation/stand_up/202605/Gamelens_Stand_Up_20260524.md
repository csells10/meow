finished offensive efficiency level 4 mapping

API Responses from /game now report this feature.

Outlined plan for Admin page for accuracy

rebuilt all 2025 data

noticed week 1 data was missing due to missing ranking data for week 1 (no data available)

Level 1 full-slate pilot produced 8,067 claim rows across 269 of 285 expected 2025 regular/postseason games. The 16 missing games appear to be Week 1 games with no ranking context / matchup breakdown available. This is expected if GameLens requires pregame ranking/windowed history before extracting normal matchup claims. For claim-validation matrices, use 269 claim-producing games as the claim-level universe. For algorithm-health coverage, retain 285 games as the full game-level universe and report 16 no-claim/insufficient-context games separately.

Algorithm Coverage
- Expected games: 285
- Payloads collected: 285
- Games with claims: 269
- Games without claims: 16
- No-claim reason: Week 1 / no pregame ranking context

Claim matrix denominator: 269 games / 8,067 claims
Coverage denominator: 285 games



Early full-slate matrix finding:

Offensive Output is the strongest broad claim area, but category drilldown shows Passing Game is carrying much of that strength. Rushing Game is closer to baseline and should not yet be treated as a strong automatic claim driver.

Defensive Control is modestly above baseline, mainly through Scoring Suppression. It appears useful, but not overwhelmingly strong.

Scoring Efficiency is below baseline across Red Zone Finish, Drive Conversion, and Scoring Production. Scoring Production has a high neutral/mixed rate, suggesting it may describe volatility more than clean claim truth.

Disruption and Turnovers remains the weakest core area. Turnover claims validate below baseline and have a high neutral/mixed rate, supporting the idea that turnover-related signals should stay contextual/cautionary rather than drive strong language.

Pressure currently looks weak, but because it appears under missing_core_area, inspect claim_type and claim_layer before changing product behavior.


####
Full-slate matrix findings:

Passing Game is currently the strongest GameLens claim family. It validates well across headline metric highlights, supporting category summaries, and core-area supporting surfaces.

Defensive Control / Scoring Suppression is modestly reliable. It is useful, but not dominant enough to drive loud conclusions by itself.

Rushing Game is close to baseline. It may matter for football interpretation, but it does not yet appear to be a strong automatic claim validator.

Turnovers are weak across all surfaces and carry high neutral/mixed rates. They should remain volatility/caution context, not confidence boosters.

Scoring Production is noisy, especially in supporting and summary surfaces. It often lands neutral/mixed, so stronger scoring-production language should stay gated.

Pressure is weak as a Game Profile headline signal in this run and should be treated carefully.

####
## Full-Slate Metric-Level Findings

Passing Game / yards_per_pass is the strongest metric family found so far. It validates well across headline, supporting, category, and core-area summary surfaces.

points_per_play is a useful Scoring Production signal and should not be grouped together with weaker finishing metrics. It validates above baseline across several surfaces.

td_rate is highly volatile and should remain blocked/caution-only for stronger language. It has very high neutral/mixed rates and weak validation.

points_allowed_per_play is a solid defensive support metric, especially as a headline or core-area supporting claim, but should remain measured rather than loud.

turnover_margin_per_game remains weak and volatile across all surfaces. It should explain chaos/risk, not drive confidence or strong claim language.

third_down_pct is conditionally interesting as a headline metric, but weaker as a broader supporting/category signal.

red_zone_efficiency remains mixed and should stay cautious.

yards_per_rush is close to baseline. It may matter contextually, but it is not yet a strong claim-validation signal.

####
This is the key product lesson

offensive_efficiency_support_v1 is a good language-calibration feature, not a winner-prediction feature.

I would describe it like this:

offensive_efficiency_support_v1 successfully separates offensive/scoring claim contexts into stronger and weaker language-support buckets. The `repeat_positive_strong` bucket validates far above the full-slate baseline, while caution, mixed, unavailable, and opposing-signal buckets validate below baseline. This supports using the feature as a Level 4 claim-language calibration input, while keeping it out of winner confidence, Matchup Lean, and Model Trust.
My recommendation

This deserves to be promoted from:

metadata-only experiment

to:

Level 4 language-calibration candidate

But I would not flip frontend language yet.

###
# GameLens Finding: `offensive_efficiency_support_v1` Is a Real Level 4 Candidate

_Date: 2026-05-24_  
_Run ID: `full_2025_reg_post_claim_matrix_pilot`_  
_Scope: 2025 regular season + postseason full-slate pilot_

## Plain-English Summary

`offensive_efficiency_support_v1` is doing what we hoped.

It separates offensive/scoring claims into buckets that actually behave differently historically.

The strongest bucket validates much better than the overall claim baseline, while caution/mixed/unavailable buckets validate worse.

This means the feature is useful for **claim-language calibration**.

It should help GameLens decide when offensive efficiency language can be more confident, and when it should stay cautious.

It should **not** be used yet for:

- winner prediction
- Matchup Lean confidence
- Model Trust overrides
- public-facing badges
- automatic frontend language changes

---

## Baseline

The full-slate pilot baseline was:

```text
Overall claim validation rate: ~49.1%

That means across all claim rows, about half of GameLens pregame claims were clearly supported by postgame data.

So for a feature to matter, it should separate claims meaningfully above or below that baseline.

Main Finding

The repeat_positive_strong bucket performed very well:

repeat_positive_strong validation rate: 63.6%
row_count: 923
game_count: 104

That is roughly 14.5 percentage points above baseline.

The repeat_positive_supportive bucket was also above baseline:

repeat_positive_supportive validation rate: 54.4%
row_count: 759
game_count: 112

That supports the idea that this bucket is useful, but should use more measured language than the strong bucket.

Bucket Ladder

The feature produced a useful validation ladder:

Bucket	Strength	Validation Rate	Simple Read
repeat_positive_strong	strong_support	63.6%	Strong signal
repeat_positive_supportive	measured_support	54.4%	Useful measured signal
not_relevant	not_applicable	49.2%	Around baseline
context_only	context_only	46.9%	Context, not boost
negative_caution	caution	46.2%	Caution
mixed_near_even	mixed	45.2%	Mixed / do not boost
anchor_unavailable	unavailable	45.1%	Missing anchor / do not boost
caution_only	caution_only	43.8%	Weak/noisy
opposing_efficiency_signal	caution	35.1%	Very weak / caution
Why This Matters

This is a strong calibration pattern.

The feature is not just adding labels. It is separating claim contexts into groups that validate differently.

That means it can help answer:

“When should GameLens sound more confident about an offensive efficiency claim?”

And just as important:

“When should GameLens stay quiet or measured?”

Surface Survival Finding

The strongest bucket survived across multiple claim surfaces.

For repeat_positive_strong:

Claim Surface	Validation Rate
metric_highlight / headline	71.0%
core_area_summary / supporting	67.0%
core_area_comparison / headline	65.5%
category_summary / supporting	62.6%
team_comparison_metric / supporting	61.0%
metric_highlight / supporting	57.5%

This is important because the feature does not only work in one tiny place. It survives across several ways GameLens writes claims.

Measured Support Finding

The repeat_positive_supportive bucket also helped, but it was weaker and less consistent than the strong bucket.

For repeat_positive_supportive:

Claim Surface	Validation Rate
metric_highlight / supporting	59.1%
category_summary / supporting	55.3%
core_area_comparison / headline	53.2%
team_comparison_metric / supporting	52.2%
core_area_summary / supporting	50.9%
metric_highlight / headline	50.0%

This suggests the right language is:

adds support
supports the read
helps confirm the offensive efficiency picture

Not:

strongly confirms
locks in
dominates

Metric-Level Support

The feature was especially useful for offensive efficiency metrics like:

yards_per_pass
yards_per_play
points_per_play
some yards_per_rush cases when backed by repeat-positive support

Examples:

yards_per_pass + repeat_positive_strong: 69.8%
points_per_play + repeat_positive_strong: 61.5%
yards_per_play + repeat_positive_strong: 62.0%
yards_per_rush + repeat_positive_strong: 57.4%

This shows that the feature can identify when certain offensive metrics are more trustworthy than they look in raw form.

Caution Buckets Worked Too

The weak/caution buckets also behaved correctly.

Examples:

caution_only: 43.8%
opposing_efficiency_signal: 35.1%
mixed_near_even: 45.2%
anchor_unavailable: 45.1%

That matters because a good feature should not only find stronger spots. It should also identify when GameLens should avoid stronger language.

Product Interpretation

This feature should be treated as:

Level 4 language-calibration candidate

Not:

winner-prediction feature

The best current use is to help backend language rules decide when to allow stronger offensive-efficiency support language.

Suggested Rule Direction
Strong Support Rule

If:

offensive_efficiency_support_bucket = repeat_positive_strong

Then allow stronger but still measured language, such as:

backed by repeat offensive-efficiency support
strong offensive-efficiency support behind this claim
the efficiency profile supports this read
Measured Support Rule

If:

offensive_efficiency_support_bucket = repeat_positive_supportive

Then allow measured support language, such as:

adds support to the read
leans supportive
helps the offensive efficiency case
No-Boost Rule

If bucket is:

caution_only
mixed_near_even
anchor_unavailable
negative_caution
opposing_efficiency_signal
context_only

Then do not boost language.

These buckets should remain caution/context only.

Final Takeaway

offensive_efficiency_support_v1 passed the first serious full-slate validation test.

It improves claim-language calibration by separating strong offensive-efficiency claim contexts from mixed, unavailable, caution, and opposing-signal contexts.

The next step is to decide whether this becomes a formal Level 4 v0.2 rule, likely starting with backend-only annotation before any frontend display changes.

Added an MVP admin claim-health API endpoint for GameLens.

The endpoint exposes aggregate claim-validation data for a selected run_id and season, using `Analytics.gamelens_claim_training_examples` as the source table. It returns coverage, baseline validation, core area matrix, category matrix, confidence/core-area matrix, offensive efficiency feature scorecard, and claim surface matrix.

The response is intentionally aggregate-level only and does not include game-ID drilldowns. It also includes `section_metadata` so future admin UI charts can understand each section’s title, purpose, chart type, and primary metric.

Tested locally with:

`/admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025`

Confirmed the endpoint returns the expected 2025 full-slate pilot values, including 8,067 claim rows, 269 games with claims, 16 games without claims, and the expected validation matrices.

####
Cloud Run deploy failed after the Admin Claim Health backend endpoint was added. The Docker image built successfully, but the Cloud Run revision could not start because the container exited during app import. Runtime logs showed the crash came from a Python 3.10+ type hint (`str | None`) being evaluated under Python 3.9 in Cloud Run.

Local testing initially passed because the local environment was running Python 3.12, which supports that syntax. The fix was to make the admin claim-health service Python 3.9-compatible by replacing the problematic union annotation with `Optional[str]`, then checking for other `| None` hints that could become follow-up startup failures.

The new admin route files were confirmed clean with grep, and `python -c "import app; print('app imported ok')"` now succeeds. Next step is redeploying to confirm Cloud Run starts successfully, then adding the true `require_admin_auth` backend guard before treating the admin dashboard as production-safe.

####
Added backend admin protection for the Admin Claim Health endpoint. The endpoint previously returned the aggregate claim-health payload without auth during local testing. After adding the admin-only guard, unauthenticated requests now correctly return 401 with “Missing Authorization Bearer token.”

The existing /games protected route still returns 401 without auth, confirming normal auth behavior was not broken. The app health endpoint continues to return 200. Remaining validation after deploy: confirm the frontend dashboard loads for an active admin user and shows a clean admin-required state for non-admin/forbidden access.

######
After adding backend admin protection, /admin/claim-health showed a blank black page instead of a useful auth/error state. Lovable inspected the frontend integration and confirmed the admin API helper was already attaching the Firebase ID token correctly. The issue was page hardening: the Admin Claim Health page needed safer render/error handling now that the backend can return 401/403.

The page now has explicit loading, unauthorized, forbidden, network-error, generic-error, no-data, and background-refresh states. It also uses an AdminErrorBoundary and safe section-reading helper so missing or unexpected section data cannot blank the page. No backend behavior, API response shape, routing, nav, or drilldown behavior changed.

###

# GameLens Daily Tracker — Admin Claim Health Dashboard + Admin Access

## Date
2026-05-24 / 2026-05-25 work session

## Main Goal

Continue building the GameLens Admin Claim Health dashboard so claim-validation performance can be reviewed visually at an aggregate level.

The dashboard is intended to answer:

> Are GameLens pregame football claims being supported by postgame data?

This is **not** winner-prediction accuracy, not betting accuracy, and not a public scoreboard.

---

## Work Completed

### 1. Admin Claim Health Frontend MVP

Lovable built the first frontend version of the Admin Claim Health dashboard at:

```text
/admin/claim-health

The page includes:

Header with run_id, season, generated_at, status, and availability
Coverage card
No-claim games card
Baseline validation rate card
Claim rows card
Neutral/mixed card
Core Area Health bar chart
Offensive Efficiency Feature Scorecard bar chart
Category Health table
Confidence by Core Area table
Claim Surface Health table

The page uses the existing aggregate API:

GET /admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025

No game-ID drilldown was added.

2. Backend Cloud Run Startup Failure Fixed

A Cloud Run deploy initially failed even though the Docker image built successfully.

The runtime logs showed:

TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'

Root cause:

Local environment was Python 3.12
Cloud Run container was Python 3.9
Python 3.9 does not support type hints like:
str | None

Fix:

Replaced the Python 3.10+ union type hint in the admin claim-health service with Python 3.9-compatible typing:
Optional[str]
Added future annotation safety to claim-language helper files where needed
Confirmed:
python -c "import app; print('app imported ok')"

Build result:

Cloud Build succeeded
Cloud Run deployed successfully
New revision served 100% of traffic
3. Admin Claim Health Endpoint Protected

Added backend admin protection for:

GET /admin/gamelens/claim-health

The endpoint now uses an admin-only auth guard.

Expected behavior:

Request Type	Expected Result
No token	401
Signed-in non-admin	403
Signed-in admin	200

Local tests passed:

curl -i http://127.0.0.1:8080/health

Returned:

200 OK
{"status":"ok"}

Admin endpoint without token:

curl -i "http://127.0.0.1:8080/admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025"

Returned:

401 UNAUTHORIZED
{"error":"unauthorized","message":"Missing Authorization Bearer token"}

Existing protected route check:

curl -i http://127.0.0.1:8080/games

Returned:

401 UNAUTHORIZED
{"error":"unauthorized","message":"Missing Authorization Bearer token"}

This confirmed the admin endpoint is no longer openly accessible and normal protected route behavior was not broken.

4. Frontend Blank Page Fixed

After backend admin auth was added, /admin/claim-health showed a blank black page.

Lovable hardened the frontend page.

Fixes included:

Added an AdminErrorBoundary
Added explicit loading/error/no-data states
Added clean 401 message:
Not signed in. Your session has expired. Please sign in again to view this page.
Added clean 403 message:
Admin access required.
Added network error handling
Added safer section reads so missing API sections do not crash the page
Confirmed the admin API helper was already attaching the Firebase Bearer token correctly
Removed nested <main> issue
Kept backend behavior and API response shape unchanged
5. Admin Navigation Work Started

Goal:

Admin users should see an admin tab in the main navigation instead of needing a hidden direct URL.

Desired final navigation:

Games | Matchup Lens | Admin | Settings

Lovable added frontend support for this:

Files changed:

src/lib/admin-api.ts
src/components/AppShell.tsx
src/pages/AdminClaimHealth.tsx

Frontend behavior:

Calls a future backend endpoint:
GET /me
Expects:
{
  "email": "...",
  "role": "admin",
  "active": true,
  "is_admin": true
}
Shows the Admin nav tab only when:
is_admin === true
Hides the Admin tab while loading, on error, or for non-admin users

Also fixed the Confidence by Core Area table so it pivots flat API rows into unique Core Area rows with High / Medium / Low columns.

6. Backend /me Endpoint Added Locally

Added a lightweight authenticated user info endpoint:

GET /me

Purpose:

Allow the frontend to know whether the signed-in user is an admin.

Expected response:

{
  "email": "user@example.com",
  "role": "admin",
  "active": true,
  "is_admin": true
}

Local test without token:

curl -i http://127.0.0.1:8080/me

Returned:

401 UNAUTHORIZED
{"error":"unauthorized","message":"Missing Authorization Bearer token"}

This confirms:

/me route is registered
/me is protected by Firebase auth
The route no longer returns 404
Current State
Working
Admin Claim Health API works
Admin Claim Health frontend dashboard loads
Backend Cloud Run startup issue fixed
Admin endpoint now blocks no-token requests
Frontend page has proper 401/403/error states
Frontend has role-aware Admin tab logic ready
/me endpoint is locally registered and protected
Still Needs Final Validation

Before considering this complete:

Commit/publish the Lovable frontend patch
Commit/deploy the backend /me endpoint
Confirm Cloud Run build succeeds
Open frontend while signed in as admin
Confirm Admin tab appears
Confirm /admin/claim-health loads from the Admin tab
Confirm non-admin users do not see the Admin tab
Confirm direct URL still returns proper 401/403 behavior when unauthorized
Important Product Guardrails

Keep this dashboard framed as:

Admin claim-validation health

Not:

Model accuracy
Winner prediction
Betting performance
Public scoreboard

The dashboard should help evaluate whether GameLens is making truthful pregame football claims, not whether it picked every game winner.

Next Step

Finish the role-aware admin navigation flow:

Commit/publish frontend patch from Lovable
Commit backend /me endpoint
Deploy backend
Confirm Admin tab appears for admin user
Confirm dashboard loads from nav

After that, this phase is basically done.


Tiny but important note: don’t call the admin tab fully done until `/me` is deployed and the tab actually appears for your signed-in admin account.

########----#######
# GameLens Daily Tracker — Admin Claim Health + Admin Tab Complete

## Date
2026-05-24 / 2026-05-25

## Main Goal

Finish the first usable version of the GameLens Admin Claim Health dashboard and make it accessible through the app navigation for admin users only.

The goal of this dashboard is to help review aggregate claim-validation performance:

> Did GameLens pregame claims get supported by postgame data?

This is not winner accuracy, betting performance, or a public scoreboard.

---

## Final Status

✅ Admin Claim Health API exists  
✅ Admin Claim Health dashboard exists  
✅ Cloud Run backend deploy issue fixed  
✅ Admin endpoint is protected by backend admin auth  
✅ Frontend no longer renders a blank page on auth/render errors  
✅ `/me` endpoint added and deployed  
✅ Admin tab appears in the main navigation for admin users  
✅ Admin tab routes to `/admin/claim-health`  
✅ Dashboard loads from the Admin tab  

This phase is now functionally complete.

---

## What Was Built

### 1. Admin Claim Health Dashboard

Frontend route:

```text
/admin/claim-health

Dashboard sections:

Coverage summary
No-claim games
Baseline validation rate
Claim rows
Neutral/mixed rate
Core Area Health chart
Offensive Efficiency Feature Scorecard chart
Category Health table
Confidence by Core Area table
Claim Surface Health table

The dashboard is aggregate-level only.

No game-ID drilldown was added.

2. Backend Claim Health API

Backend endpoint:

GET /admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025

The API returns:

coverage
baseline
section_metadata
sections.core_area_matrix
sections.category_matrix
sections.confidence_core_area_matrix
sections.feature_scorecard
sections.surface_matrix

This gives the frontend graph-ready data for aggregate claim-health review.

3. Cloud Run Startup Issue Fixed

A backend deploy initially failed even though the Docker image built successfully.

Root cause:

Local dev was running Python 3.12
Cloud Run was running Python 3.9
New backend code used Python 3.10+ style type hints:
str | None

Cloud Run crashed during app import.

Fix:

Replaced the incompatible union type hint with Python 3.9-compatible typing:
Optional[str]
Added annotation safety where needed
Confirmed app import worked
Redeployed successfully

Result:

Cloud Build and Cloud Run deploy succeeded, and the new revision served traffic.

4. Admin Security Added

The Admin Claim Health API is now protected by backend admin auth.

Expected behavior:

Request	Result
No token	401
Signed-in non-admin	403
Signed-in admin	200

Local tests confirmed:

/health → 200
/admin/gamelens/claim-health without token → 401
/games without token → 401

This confirmed the admin endpoint is no longer open and existing protected routes still behave correctly.

Important reminder:

Frontend tab hiding is UX only.
Backend require_admin_auth is the real security.

5. Frontend Hardening

After the backend endpoint became protected, the Admin Claim Health page initially rendered as a blank black page.

Lovable hardened the page with:

Explicit loading state
401 unauthenticated message
403 admin-required message
Network error message
Generic error fallback
No-data fallback
Admin error boundary
Safe section reads
Confidence table pivot fix

The page now fails visibly instead of silently blanking.

6. /me Endpoint Added

Backend endpoint added:

GET /me

Purpose:

Let the frontend know whether the signed-in user is an admin.

Expected response:

{
  "email": "user@example.com",
  "role": "admin",
  "active": true,
  "is_admin": true
}

Local no-token test confirmed:

/me → 401 Missing Authorization Bearer token

This showed the route was registered and protected.

7. Admin Tab Added

Frontend now calls /me and conditionally shows the Admin tab only when:

is_admin === true

Final navigation now includes:

Games | Matchup Lens | Settings | Admin

The Admin tab routes to:

/admin/claim-health

Screenshot validation confirmed the Admin tab appears for the admin account and routes correctly.

Current Interpretation

This was a full-stack admin feature loop:

Built aggregate admin API
Built frontend dashboard
Fixed backend deployment compatibility
Added backend admin security
Hardened frontend auth/error states
Added /me user context endpoint
Added role-aware Admin navigation

The dashboard is now discoverable for admin users without being exposed to normal users.

Product Guardrails Preserved

The dashboard still avoids:

public scoreboard framing
winner accuracy framing
betting performance framing
game-ID drilldown
overconfident “model success” language

Correct framing remains:

Aggregate claim-validation health

The dashboard helps answer whether GameLens is telling truthful pregame football stories.

Final Validation Completed

Confirmed:

Cloud Run build successful
Frontend published
Admin tab appears in main navigation
Admin tab routes to /admin/claim-health
Claim Health dashboard loads
Backend endpoint is protected
Direct URL behavior remains safe
Tomorrow’s Starting Point

Possible next work:

Review the Admin Claim Health dashboard visually and clean up any readability issues.
Validate the Confidence by Core Area table now that pivoting is fixed.
Decide whether to improve chart labels/tooltips.
Consider adding lightweight filters:
season
run_id
minimum claim rows
Begin interpreting the actual results:
strongest claim areas
weakest/noisiest areas
feature scorecard usefulness
which surfaces deserve stronger or softer language
Decide whether this dashboard should drive the next Level 4 calibration improvement.

Huge win today. This went from **hidden experimental endpoint** to **secured admin dashboard with real navigation**. That is not small.

##
## Final Frontend Polish — Feature Scorecard Labels

Before declaring the Admin Claim Health dashboard complete, noticed that the Offensive Efficiency Feature Scorecard chart rendered bars correctly but the left-side bucket labels appeared as tiny dashes/blank labels. This made the chart hard to interpret because the user could see the performance bars but not which feature bucket each bar represented.

Lovable fixed this by updating the frontend chart handling:

- Added `bucket` and `strength` to the admin API row type
- Added readable bucket labels such as:
  - `repeat_positive_strong` → `Repeat Positive Strong`
  - `repeat_positive_supportive` → `Repeat Positive Supportive`
  - `opposing_efficiency_signal` → `Opposing Efficiency Signal`
- Added graceful truncation for long Y-axis labels
- Increased Y-axis width for the Feature Scorecard chart
- Added percent labels at the end of bars
- Preserved the raw bucket value in tooltips
- Reused the defensive label behavior for Core Area Health as well

This was a frontend readability fix only. It did not change backend behavior, API response shape, admin auth, routing, or add drilldown.

####

Admin Claim Health is now functionally complete as an MVP. It is not visually polished yet, but the core loop works: admin users see the Admin tab, the tab routes to /admin/claim-health, the protected API returns real aggregate claim-health data, and the dashboard renders the major sections. Further work should focus on readability, interpretation, and small UX polish rather than new backend scope.