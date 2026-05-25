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