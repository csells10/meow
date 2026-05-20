## Definition of Done

Today’s goal is complete when `/game` responses expose claim-strength metadata inside `language_support`:

- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`

This should be metadata-only.

Do not change:

- frontend display
- user-facing summaries
- matchup lean
- outcome confidence
- Model Trust
- winner/prediction logic

## Validation Games

Use the same smoke-test games first:

- `20251009_PHI@NYG`
- `20251013_BUF@ATL`

Expected behavior:

- `yards_per_pass` and `yards_per_rush` should remain unblocked but gated
- `red_zone_efficiency`, `td_rate`, `turnover_margin_per_game`, and `yards_per_play` should remain blocked
- claim-strength fields should no longer be `null` where the data exists
- no wording should become louder yet

## Stop Point

If the metadata appears correctly and existing guardrails still work, stop and commit before touching frontend or copy generation.


--starting working--


# GameLens Daily Notes — Claim Strength Metadata Exposure

## Work Completed

Today I updated the `/game` response claim-language metadata layer so `language_support` now exposes the new Level 4 v0.2 claim-strength fields:

- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`

This was added as response-only metadata.

## Files Updated

- `services/claim_language_support_registry.py`
- `services/claim_language_response.py`

## What Changed

### `claim_language_support_registry.py`

Updated `build_language_support()` so the payload can include:

- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`

This keeps the registry compatible with the Level 3 / Level 4 claim-strength calibration work.

### `claim_language_response.py`

Added runtime claim-strength metadata helpers that classify response rows using percentile gap and football-area grouping.

Updated language support annotation for:

- Team Comparison rows
- Matchup Breakdown metric highlights
- Matchup Breakdown category summaries and drivers

Core Area summaries and context notes were intentionally left untouched for now.

## Validation Completed

Compiled successfully:

```bash
python -m py_compile services/claim_language_response.py services/claim_language_support_registry.py

Smoke-tested local /game responses:

20251009_PHI@NYG
20251013_BUF@ATL

Confirmed:

New claim-strength fields appear in language_support
yards_per_pass and yards_per_rush show measured/trusted metadata where appropriate
Boosting remains gated by two_way_context
Blocked metrics stay blocked:
red_zone_efficiency
td_rate
turnover_margin_per_game
yards_per_play
No frontend wording, matchup lean, Model Trust, outcome confidence, or winner logic changed
Notes

One local startup error occurred:

ModuleNotFoundError: No module named 'flask_cors'

This appeared to be a virtual environment issue, not a code bug. After switching into the correct venv and restarting app.py, the API worked and the new metadata appeared correctly.

Current Status

Backend metadata exposure for Level 4 v0.2 claim-strength fields is complete and smoke-tested.

Next step: commit the two changed service files.

--api has been patched--

What the new API feature does

GameLens already says things like:

“PHI shows a clear advantage in Red Zone Efficiency.”

The new feature answers the follow-up question:

How loudly should GameLens be allowed to say that?

That is the key. This does not change the pick. It does not change confidence. It does not change Model Trust. It gives each claim a little “language permission slip.”

Your response now has this inside language_support:

{
  "claim_strength_bucket": "real_edge",
  "claim_strength_context": "caution_area_edge",
  "claim_strength_language_signal": "caution_only",
  "language_boost_allowed": false,
  "reason": "two_way_context_not_supportive"
}

That means:

“The edge is numerically real, but this is a volatile/noisy area or the broader support is not strong enough, so don’t let the UI oversell it.”

That is a big deal. That is GameLens getting smarter about tone, not just data.

The main fields
claim_strength_bucket

This is the size of the edge based mostly on percentile gap.

Think of it like:

Bucket	Meaning
near_even	Basically too close
thin_edge	Small edge, soften language
usable_edge	There is some separation
real_edge	Big separation
missing	Not enough gap data

Example from PHI/NYG: PHI had a real_edge in Red Zone Efficiency because the percentile gap was huge, but that alone does not mean we should speak strongly.

claim_strength_context

This is the football meaning of that edge.

Examples:

Context	Meaning
trusted_real_edge	Big edge in a more trusted football area
trusted_measured_edge	Useful edge, but measured
watch_area_edge	Interesting, but still watch-list quality
caution_area_edge	Big edge, but volatile/noisy area
thin_gap	Skinny edge
near_even_gap	Too close
unclassified_edge	Gap exists, but row lacks enough category/core area context

This is why Red Zone Efficiency and Turnover Margin / Game can have real_edge but still get caution_area_edge. They are real signals, but volatile. Your system is saying: “Yes, this matters — but don’t act like it’s ironclad.” 🧯

claim_strength_language_signal

This is the product-facing language hint.

Signal	How to use it
boost_candidate	Candidate for stronger wording, only if other gates agree
measured	Useful support, but don’t hype it
caution_only	Treat as volatile/swing-factor language
soften	Use softer wording
normal	No special treatment
no_boost	Missing/insufficient support

In the PHI/NYG response, a lot of PHI’s flashy advantages are caution_only, especially Red Zone Efficiency, Points Per Play, Turnover Margin, and TD Rate. That’s valuable because the model liked PHI, but NYG won 34–17. The new metadata helps explain why some pregame claims should have sounded less certain, even when the raw edge looked big.

The most important field: language_boost_allowed

This is the final “can we talk louder?” switch.

For PHI/NYG, it is basically saying:

"language_boost_allowed": false

Why? Because both sides had:

"two_way_context": "available_mixed"

not:

"supportive"

Your own feature work showed two_way_context = supportive was the repeatable lift signal. It should be treated as claim-language support, not winner confidence. Supportive two-way context validated better than mixed/unavailable by roughly 7–8 points in prior testing, but it should not become an automatic pick-confidence boost.

So the API is doing the right thing:

“The stat edge exists, but the broader claim support is not strong enough to boost the language.”

That’s mature. That’s not a toy model anymore.

How we use it in the product
1. Better frontend copy later

Instead of blindly showing:

“PHI shows a clear advantage in Turnover Margin Per Game.”

The UI could eventually show:

“PHI has the turnover edge, but this is a volatile swing factor.”

Because that row says:

"claim_strength_context": "caution_area_edge",
"claim_strength_language_signal": "caution_only",
"support_level": "blocked"

That’s exactly the kind of nuance GameLens needs.

2. Better “Why?” tooltips

For a metric like Yards Per Rush, the PHI/NYG response says:

"claim_strength_bucket": "usable_edge",
"claim_strength_context": "trusted_measured_edge",
"claim_strength_language_signal": "measured"

But it also says:

"language_boost_allowed": false
"reason": "two_way_context_not_supportive"

So a tooltip could say:

“This is a useful rushing-efficiency signal, but broader two-way support was mixed, so GameLens keeps the language measured.”

That is excellent user education. It explains the model without overpromising.

3. Cleaner internal QA

This feature lets you filter claims after the fact:

Which boost_candidate claims actually validated?
Which caution_only claims caused model overconfidence?
Which soften claims should have suppressed summary language?
Which trusted_measured_edge claims are earning promotion later?

That connects directly to your claim-learning system: the goal is not just “did we pick the winner?” It is “did the pregame claim hold up in postgame data?”

PHI vs NYG in plain English

Here is what GameLens is now able to say internally:

PHI had several real pregame edges. Red Zone Efficiency, Points Per Play, TD Rate, Turnover Margin, and Third Down % all showed meaningful separation.

But the new claim-language metadata says:

“Careful. Many of those are caution/watch/blocked areas, and the broader two-way context is only available_mixed, not supportive.”

That is exactly the lesson from this game. PHI looked good in a lot of pregame signals, but the model should not necessarily have sounded as strong as the raw edges looked. NYG had an Offensive Output/Rushing Game path showing up too, especially Yards Per Rush and First Down Rate, but those also stayed measured because the two-way context was mixed.

My recommended use rule

For now, use this hierarchy:

1. language_boost_allowed
   If true, the claim may be eligible for stronger/supportive wording.

2. claim_strength_language_signal
   Determines tone:
   - boost_candidate = maybe stronger later
   - measured = useful but careful
   - caution_only = volatile/swing factor
   - soften = reduce confidence in wording

3. rule_status / reason
   Explains why the system blocked or allowed the language.

4. two_way_context
   Final safety gate. supportive matters. available_mixed means don’t boost.
The win

You just gave GameLens a new brain layer:

It can now separate “this edge exists” from “we should speak confidently about this edge.”

That is the difference between a stats page and an intelligence product. 🔥


--finished API integration--

Today’s Definition of Done

You wanted to expose the Level 4 v0.2 claim-strength metadata inside /game without changing product behavior.

That happened.

Completed
Added claim_strength_bucket
Added claim_strength_context
Added claim_strength_language_signal
Exposed them inside language_support
Added them to:
Team Comparison rows
Metric Highlights
Category Summary drivers
Category Summary aggregate support
Confirmed blocked metrics stayed blocked
Confirmed two_way_context still gates language boosts
Confirmed no changes to:
winner logic
matchup_lean
model_trust
outcome confidence
frontend copy
Ran compile successfully
Smoke-tested:
20251009_PHI@NYG
20251013_BUF@ATL
Created notes + deep-dive documentation

Finalized it into the API then ran the 'devil's advocate' test to collect responses of the API and asked CHATGPT to poke holes and find areas that didn't make sense and would make it difficult for a user to understand.

## Team Comparison Metadata Alignment Cleanup

Completed the first cleanup item from the Devil’s Advocate API smoke test.

The issue was that `team_comparison` rows were missing `category` and `core_area`, which caused claim-strength metadata to fall back to `unclassified_edge`. This made Team Comparison less aligned with Metric Highlights and Category Summaries, even when they were describing the same metric.

Updated `services/game_service.py` so visible Team Comparison rows now include `category` and `core_area`.

Also updated `agg/gamelens_training/build_claim_training_examples.py` so future claim-training rows preserve the same Team Comparison metadata instead of dropping it.

Validation completed:
- `services/game_service.py` compiled successfully
- `agg/gamelens_training/build_claim_training_examples.py` compiled successfully
- Local `/game` spot checks confirmed Team Comparison rows now include category/core_area
- Re-ran the same 30-game API smoke sample
- `team_comparison_unclassified_claim_strength` warning dropped from 30 to 0

Result: Team Comparison now speaks the same metadata language as Metric Highlights, Category Summaries, and claim-language support.

## Number 2 — Category Summary No-Boost Reason Cleanup

Completed the second Devil’s Advocate cleanup item. Category summary aggregate rows no longer return `reason: null` when no drivers qualify for boosted language. They now return:

`no_category_drivers_allowed_language_boost`

This improves QA clarity without changing scoring, matchup lean, outcome confidence, Model Trust, or frontend behavior.

| Cleanup item | Before | After | Status |
|---|---:|---:|---|
| Team Comparison unclassified claim strength | 30 | 0 | ✅ Done |
| Category aggregate missing no-boost reason | 218 | 0 | ✅ Done |
| Confidence field mismatch | 10 | 10 | ✅ Done |
| Core Area layer leader mismatch | 32 | 32 | Later |
| Model Trust tooltip mismatch | 6 | 6 | Later |

## Number 3 — Confidence Contract Cleanup

Completed the third Devil’s Advocate cleanup item.

The API now preserves `matchup_lean.confidence` as a legacy/raw signal-confidence field while adding clearer fields for frontend use:

- `raw_signal_confidence`
- `confidence_role`
- `user_facing_confidence`

The frontend now has one clear confidence mouthpiece: `matchup_lean.user_facing_confidence.label`, sourced from `outcome_confidence`.

Also updated the API smoke collector so it no longer treats raw signal confidence differing from outcome confidence as a mismatch. It now validates the new contract instead.

Validation completed:
- `services/game_service.py` compiled successfully
- `qa/collect_gamelens_api_smoke_payloads.py` compiled successfully
- Local API spot checks confirmed the new fields
- Re-ran the same 30-game sample
- `confidence_field_mismatch` dropped from 10 to 0

Result: confidence fields now communicate their roles clearly without changing model scoring, matchup lean logic, or frontend behavior.

-----Break in work then a re focus----

## Devil’s Advocate Cleanup — Updated Direction

After reviewing the updated API output, the remaining work should shift slightly.

The first three cleanup items improved metadata alignment and API clarity. The remaining work is less about raw data correctness and more about making the `/game` response feel coherent to users.

The key product rule moving forward:

> One user-facing Core Area should have one displayed direction.

Backend layers can preserve diagnostic nuance, but the frontend should not force users to reconcile competing leaders for the same Core Area.

| Priority | Item | Current Warning Count | Updated Interpretation | Recommended Action | Definition of Done |
|---:|---|---:|---|---|---|
| 1 | Model Trust tooltip mismatch | 6 | Small wording bug. Tooltip language sometimes says “several even areas” when the visible neutral count is only one. | Fix tooltip wording in `services/model_trust_service.py`, likely inside `build_edge()`, so text depends on actual neutral count. | `model_trust_tooltip_even_area_mismatch` drops from `6 → 0`. |
| 2 | Core Area layer leader mismatch | 32 | Product/API consistency issue. `core_area_comparison` and `matchup_breakdown.core_area_summaries` can expose competing visible leaders for the same Core Area. | Make `core_area_comparison` own the user-facing Core Area leader. Use `core_area_summaries` to describe driver support quality, not to create a competing directional verdict. | `core_area_layer_leader_mismatch` drops from `32 → 0`, or becomes a non-user-facing diagnostic field. |
| 3 | Core Area display strength language | N/A | Current language can make moderate broad-score gaps sound too decisive. | Add/adjust display strength language based on broad Core Area gap: `<0.08 = Near Even`, `0.08–0.18 = Lean`, `0.18–0.30 = Edge`, `>=0.30 = Strong Edge`. | Frontend/API says “Lean” when the gap is real but not clean enough to call an “Edge.” |
| 4 | Frontend QA case | N/A | `20251013_BUF@ATL` is a useful stress test because it shows broad BUF support but ATL resistance in turnovers/defense. | QA this game after backend cleanup to confirm the page reads as a measured lean, not a clean contradiction. | User-facing read feels coherent: “BUF had the broader lean, but ATL had defensive/turnover resistance.” |
| 5 | Smoke-test rerun | N/A | Same seeded sample gives clean before/after comparison. | Re-run `python qa/collect_gamelens_api_smoke_payloads.py --sample-size 30 --seed 20260520`. | Only acceptable remaining warnings are intentional diagnostics, not user-facing contradictions. |

### Updated Work Order

1. Fix Model Trust tooltip wording first because it is small, isolated, and easy to validate.
2. Then fix Core Area direction consistency because it affects the coherence of the matchup page.
3. Re-run the same 30-game smoke sample.
4. QA `20251013_BUF@ATL` manually in the frontend before calling the Core Area issue resolved.

## Model Trust Tooltip Cleanup

Fixed the Model Trust edge tooltip so wording now reflects the actual neutral/even row count. Previously, some responses said “several even areas” even when only one neutral Team Comparison row existed.

Updated `services/model_trust_service.py` so tooltip language changes based on neutral count:
- `0` neutral rows: clear separation wording
- `1` neutral row: one-neutral-row wording
- `2+` neutral rows: multiple-neutral-rows wording

Validation completed:
- `services/model_trust_service.py` compiled successfully
- Local API spot check confirmed corrected tooltip wording
- Re-ran the same 30-game smoke sample
- `model_trust_tooltip_even_area_mismatch` dropped from `6 → 0`

Result: Model Trust tooltip language now matches the visible Team Comparison counts without changing scoring or model logic.

--final step in the clean up done (this is my note before my commit)--

## Core Area Direction Consistency Cleanup

Completed the final Devil’s Advocate API cleanup item.

The issue was that `core_area_comparison` and `matchup_breakdown.core_area_summaries` could expose different visible leaders for the same Core Area. That made the `/game` response feel like it had competing directional reads.

Updated `services/game_service.py` so `core_area_comparison` is now the source of truth for user-facing Core Area direction. `core_area_summaries` now align their visible `leader` and `leader_team` to that broad Core Area read, while preserving the previous headline-driver result as diagnostic metadata.

New diagnostic fields include:

- `leader_source`
- `display_strength`
- `display_summary`
- `broad_score_gap`
- `headline_driver_leader`
- `headline_driver_summary`
- `driver_alignment`

Validation completed:

- `services/game_service.py` compiled successfully
- Local API spot checks confirmed Core Area summary leaders now match `core_area_comparison`
- `20251013_BUF@ATL` now shows BUF as the displayed Disruption and Turnovers leader while explaining headline-driver support was thin/neutral
- Re-ran the same 30-game smoke sample
- `core_area_layer_leader_mismatch` dropped from `32 → 0`
- Latest smoke sample returned `warning_counts: {}`

Result: the `/game` response now has one coherent user-facing Core Area direction while still preserving driver nuance for explanation/debugging.