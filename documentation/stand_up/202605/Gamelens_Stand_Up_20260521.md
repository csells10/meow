Tomorrow’s main goal
Update the frontend to consume the newer /game response fields

I would not start more feature engineering tomorrow.

That feels tempting, but I think it would be premature. You just improved the API contract. Now you need to make sure the product is actually showing that improvement.

Right now, the frontend may still be using older/simple fields like:

leader
summary
summary_label
confidence

when it should increasingly prefer:

display_strength
display_summary
leader_source
driver_alignment
user_facing_confidence
outcome_confidence
language_support

That is tomorrow’s highest-value work.

My proposed order of work
1. Frontend field audit

Start with the matchup page and identify where it still uses older fields.

Focus on these sections:

Frontend area	What to check
Core Area Advantage	Is it using display_strength and display_summary?
Matchup Lean	Is it using matchup_label, profile_strength, and user_facing_confidence?
Model Trust	Is it using the improved tooltip/reasoning fields?
Team Comparison	Is it respecting better, technical_better, comparison_strength, and maybe language_support?
Category/Metric Highlights	Is it showing “clear advantage” even when metadata says cautious?

The practical question is:

Does the website reflect the smarter API, or is it still making the API sound dumber than it is?

That’s a great tomorrow question.

2. Update Core Area Advantage first

This should be the first real frontend change.

Why? Because today’s best improvement was this:

One Core Area = one displayed direction

So the frontend should show something like:

Disruption and Turnovers
BUF Lean
BUF has a broad lean in Disruption and Turnovers. Headline-driver support is thin or close to even.

not just:

BUF Edge
Use these fields first
display_strength
display_summary
leader_team
leader_source
driver_alignment
broad_score_gap

I’d treat summary and summary_label as fallback fields now.

3. Confirm confidence display

Make sure the frontend does not use:

matchup_lean.confidence

as the main displayed confidence.

It should use:

matchup_lean.user_facing_confidence.label

or:

matchup_lean.outcome_confidence.label

This matters because confidence is now legacy/raw signal confidence.

This is one of those “small field, big product trust” items.

4. QA the same known game

Use:

20251013_BUF@ATL

That game is now your perfect QA goblin. 🧌

Before frontend update, it may still look too clean for BUF.

After frontend update, it should read more like:

BUF had the broader lean, but Disruption/Turnovers was thin, ATL had defensive/turnover resistance, and the miss was a useful calibration case.

That is the product voice you want.

What I would not do tomorrow
I would not add more feature engineering yet

You already added meaningful feature layers:

claim strength metadata
two_way_context
language_support
driver_alignment
display_strength

Before adding more, you need to see whether the current product can communicate those layers.

Otherwise you risk building more backend intelligence that the user never actually sees.

I would not start a big redesign

This is not “make the page prettier” time.

It is:

make the current page consume the correct fields

Small, surgical, high-leverage.

Tomorrow’s work plan
## Tomorrow Plan — Frontend Alignment With Updated `/game` Response

### Goal

Update the frontend so the Matchup page reflects the improved `/game` API response instead of relying on older/simple fields.

### Priority 1 — Core Area Advantage

Use the new Core Area summary fields:

- `display_strength`
- `display_summary`
- `leader_source`
- `driver_alignment`
- `broad_score_gap`
- `headline_driver_leader`

Frontend should display one clear Core Area direction, with driver disagreement shown as context.

### Priority 2 — Confidence Display

Confirm frontend uses:

- `matchup_lean.user_facing_confidence.label`
- `matchup_lean.outcome_confidence`
- `matchup_lean.profile_strength`

Avoid using legacy/raw:

- `matchup_lean.confidence`

as the main user-facing confidence.

### Priority 3 — Model Trust + Team Comparison Sanity Check

Confirm Model Trust and Team Comparison are still coherent after the API cleanup.

Check that:

- neutral rows do not create fake edge strength
- tooltips match the actual edge/neutral counts
- Team Comparison does not overstate near-even rows

### QA Game

Use:

```text
20251013_BUF@ATL

Expected frontend read:

BUF had the broader pregame lean, but ATL had defensive and turnover-related resistance. Disruption and Turnovers should read as a lean/mixed support area, not a clean edge.
Do Not Do Yet
Do not add more feature engineering yet.
Do not start a major frontend redesign.
Do not build new model layers until current API improvements are visible in the product.
Definition of Done
Core Area cards use the new display fields.
Confidence display uses user_facing_confidence.
BUF@ATL looks less overconfident on the page.
Existing matchup page still loads cleanly.
No major visual redesign required.

# My honest take

Tomorrow should be a **frontend contract day**, not a feature engineering day.

You’ve improved the brain. Now make sure the face is saying what the brain actually means. 😄

---todays proposed work---

| Priority | Major Theme                      | Sub-theme                              | Proposed Frontend Work                                                                                                                                                                             | Key Fields / Areas                                                                                                                                                       | Definition of Done                                                                                 |
| -------: | -------------------------------- | -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
|        1 | **Frontend Contract Audit**      | Find old field usage                   | Review the Matchup page and identify where it still uses older/simple fields instead of the newer API contract.                                                                                    | Old: `leader`, `summary`, `summary_label`, `confidence` → New: `display_strength`, `display_summary`, `user_facing_confidence`, `outcome_confidence`, `language_support` | We know exactly which components need updates before touching code.                                |
|        2 | **Core Area Advantage Update**   | Best first real change                 | Update Core Area cards so they still use `core_area_comparison` for score bars, but use `matchup_breakdown.core_area_summaries` for smarter display/caption text.                                  | `display_strength`, `display_summary`, `leader_team`, `driver_alignment`, `broad_score_gap`                                                                              | `20251013_BUF@ATL` shows **BUF Lean**, not **BUF Edge**, for Disruption and Turnovers.             |
|        3 | **Core Area Language Mapping**   | Softer labels                          | Map backend `display_strength` to user-facing labels.                                                                                                                                              | `near_even → Near Even`, `lean → Lean`, `edge → Edge`, `strong_edge → Strong Edge`                                                                                       | Core Area labels stop overstating moderate gaps.                                                   |
|        4 | **Confidence Display Cleanup**   | Use the right confidence mouthpiece    | Confirm Matchup Lean is not showing raw legacy `matchup_lean.confidence` as the main user-facing value.                                                                                            | Prefer `matchup_lean.user_facing_confidence.label`, `outcome_confidence`, `profile_strength`                                                                             | The page shows product-facing confidence, not raw signal confidence.                               |
|        5 | **Team Comparison Sanity Check** | Don’t create fake edges                | Confirm neutral/near-even rows do not visually imply a team edge. Keep “Fits matchup” subtle and avoid adding more pills.                                                                          | `better`, `technical_better`, `comparison_strength`, `language_support`                                                                                                  | Near-even rows feel like near-even rows, not hidden predictions.                                   |
|        6 | **Model Trust Sanity Check**     | Confirm wording coherence              | Make sure Model Trust tooltip/reasoning text still matches the updated API behavior after yesterday’s cleanup.                                                                                     | `model_trust.reasoning`, `edge.tooltip`, `matchup_advantage`, `signal_alignment`                                                                                         | Tooltips match visible edge/neutral counts and do not overstate certainty.                         |
|        7 | **Category / Metric Copy Audit** | Catch “clear advantage” overstatements | Look for places where the UI still says “clear advantage” even when metadata says cautious, blocked, mixed, or no boost. This may become a later copy cleanup, not necessarily today’s main build. | `claim_strength_language_signal`, `rule_status`, `reason`, `language_boost_allowed`                                                                                      | Any risky wording is documented; only fix today if it is small and obvious.                        |
|        8 | **Known-Game QA Pass**           | Use the QA goblin 🧌                   | QA with `20251013_BUF@ATL` because it exposes the exact problem: broad BUF lean, but thin/neutral driver support and ATL resistance.                                                               | Full Matchup page                                                                                                                                                        | Page reads as: “BUF had the broader lean, but this was not a clean edge.”                          |
|        9 | **Stop / Commit Point**          | Do not wander                          | Stop once the frontend reflects the improved API contract. Do not start feature engineering, new model logic, or a redesign.                                                                       | N/A                                                                                                                                                                      | Existing page loads cleanly, no new sections/badge clutter, frontend feels smarter but not busier. |

| Order | Work Block                                     | Why it goes here                                                                 |
| ----: | ---------------------------------------------- | -------------------------------------------------------------------------------- |
|     1 | **Audit current frontend usage**               | Prevents guessing. We want to know where old fields are still driving the UI.    |
|     2 | **Patch Core Area Advantage**                  | Highest-leverage visible fix; already backed by the new API fields.              |
|     3 | **Patch confidence display if needed**         | Small field change, big trust improvement.                                       |
|     4 | **Sanity check Team Comparison + Model Trust** | Make sure yesterday’s API cleanup is not being muted or contradicted by UI copy. |
|     5 | **QA `20251013_BUF@ATL`**                      | Confirms the page now tells the right story.                                     |
|     6 | **Commit**                                     | Lock the progress before the “one more thing” goblin appears. 😄                 |


| Order | Lovable Task                                                                                       |
| ----: | -------------------------------------------------------------------------------------------------- |
|     1 | Audit current Matchup page field usage                                                             |
|     2 | Update Core Area Advantage to use `matchup_breakdown.core_area_summaries` display fields           |
|     3 | Confirm confidence display uses `matchup_lean.user_facing_confidence.label` / `outcome_confidence` |
|     4 | Sanity check Team Comparison and Model Trust copy                                                  |
|     5 | QA `20251013_BUF@ATL`                                                                              |
|     6 | Stop and summarize changes                      

| Priority | Major Theme                    | Status                         | Why                                                                                                                                                      |
| -------: | ------------------------------ | ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
|        1 | **Frontend Contract Audit**    | ✅ Complete                     | Lovable audited old vs. new field usage and identified the risky outdated areas.                                                                         |
|        2 | **Core Area Advantage Update** | ✅ Complete                     | Core Area cards now use `core_area_summaries.display_*` fields for smarter labels/captions while preserving bars/percentages.                            |
|        3 | **Core Area Language Mapping** | ✅ Complete                     | `display_strength` is now mapped into labels like **Lean** and **Strong Edge**. BUF@ATL proved it with **BUF Lean** for Disruption and Turnovers.        |
|        4 | **Confidence Display Cleanup** | ✅ Mostly complete              | v1.7.13 patched the legacy fallback path to prefer `user_facing_confidence.label`. BUF@ATL looked unchanged because it already used the newer read path. |
|        8 | **Known-Game QA Pass**         | ✅ Complete for v1.7.13/v1.7.14 | BUF@ATL was tested before/after. The important Core Area target passed.                                                                                  |
                                                   |

| Priority | Major Theme                      | Status                                                    |
| -------: | -------------------------------- | --------------------------------------------------------- |
|        5 | **Team Comparison Sanity Check** | ⏳ Open                                                    |
|        6 | **Model Trust Sanity Check**     | ⏳ Open                                                    |
|        7 | **Category / Metric Copy Audit** | ⏳ Open                                                    |
|        9 | **Stop / Commit Point**          | ⏳ Open after you decide whether to continue or commit now |


Completed:
- Priority 1: Frontend Contract Audit
- Priority 2: Core Area Advantage Update
- Priority 3: Core Area Language Mapping
- Priority 4: Confidence Display Cleanup, narrow fallback patch
- Priority 8: BUF@ATL QA pass for v1.7.13/v1.7.14

Remaining:
- Team Comparison sanity check
- Model Trust sanity check
- Category / Metric copy audit
- Final commit/stop point

--psudo commit--
Align frontend confidence and core area display language

| Priority | Major Theme                      | Status                                                    |
| -------: | -------------------------------- | --------------------------------------------------------- |
|        5 | **Team Comparison Sanity Check** |   CLOSED                                                   |
|        6 | **Model Trust Sanity Check**     | CLOSED                                                     |
|        7 | **Category / Metric Copy Audit** | ⏳ Open                                                    |
|        9 | **Stop / Commit Point**          | ⏳ Open after you decide whether to continue or commit now |


--finished front end work here is a wrap up of my notes from the day below--

# GameLens Frontend Wrap-Up — v1.7.13 to v1.7.18

## Summary

Today’s frontend work focused on aligning the Matchup page with the smarter `/game` API response without redesigning the page or adding more UI clutter.

The main goal was:

> Make GameLens sound more accurate, more measured, and less like a forced pick machine.

This was not a feature-expansion day. It was a frontend contract-alignment and product-voice cleanup day.

---

## Work Completed

### v1.7.13 — Matchup Lean Confidence Fallback

Updated the legacy Matchup Lean confidence fallback so it prefers:

```ts
matchup_lean.user_facing_confidence.label

before falling back to:

matchup_lean.confidence

This keeps the frontend aligned with the backend’s product-safe confidence wording.

Impact:

Safer confidence display fallback
No layout change
No visible change expected on games already using the newer Matchup Read path
v1.7.14 — Core Area Advantage Display Strength

Updated Core Area Advantage tiles to prefer backend display fields from:

matchup_breakdown.core_area_summaries

Specifically:

display_strength
display_summary
leader_team

Core Area labels now support:

Near Even
Lean
Edge
Strong Edge

Important QA win:

For 20251013_BUF@ATL, Disruption and Turnovers changed from:

BUF Edge

to:

BUF Lean

This was the first major visible frontend improvement. The page now better communicates that BUF had a broad lean there, but driver support was thin or close.

v1.7.15 — Team Comparison Visual Restraint

Updated Team Comparison row rendering so near-even rows no longer look like decisive wins.

Rows with:

comparison_strength === "near_even"

now visually render as neutral:

no bold winner
no dimmed loser
no “Fits matchup” badge
no supported-row tint

row.better is not mutated, so model/counter logic remains untouched.

v1.7.16 — Model Trust Fallback Alignment

Aligned Model Trust fallback logic with the new Team Comparison restraint.

If backend Model Trust fields are missing, fallback counts now skip near-even Team Comparison rows when calculating:

fallback matchup advantage counts
fallback edge-strength labels

Backend-provided Model Trust values still take precedence.

This prevents fallback Model Trust from contradicting the visually restrained Team Comparison rows.

v1.7.17 — Copy Restraint + Fallback Language Cleanup

Cleaned up remaining frontend-owned copy that could sound too strong.

Changes included:

fallback Team Comparison bullets now skip near-even rows
fallback wording changed from “held the edge in” to “leaned in”
neutral Core Area fallback captions now use softer context wording
confidence tooltip wording removed “Clear advantage across multiple factors”
verified “Fits matchup” gate was already correctly suppressing cautious/softened/no-boost rows

Backend-owned reasoning text remains untouched.

v1.7.18 — Fits Matchup Measured-Signal Correction

After auditing badge reachability, the “Fits matchup” gate was slightly relaxed.

Previously suppressed:

caution_only
soften
no_boost
measured

Updated to suppress only:

caution_only
soften
no_boost

This allows measured rows to show “Fits matchup” when the backend explicitly sets:

language_boost_allowed === true

Reasoning:

measured means useful support, not hype. Since the badge says “Fits matchup” rather than “Strong edge,” allowing measured support is appropriate.

QA Games Reviewed
Game ID	QA Purpose	Result
20251013_BUF@ATL	Known calibration miss / frontend restraint goblin	Passed
20251123_MIN@GB	Strong correct read	Passed
20251222_SF@IND	High-profile miss / severe model miss	Passed with backend-copy note
20251109_ARI@SEA	No Pick / Low Confidence restraint	Passed
20251020_TB@DET	Correct medium-confidence mixed profile	Passed
20260125_NE@DEN	Close playoff / low-scoring correct lean	Passed
20251123_MIN@GB badge audit	Confirm “Fits matchup” remains reachable	Passed after v1.7.18
Product Impact

The frontend now better separates:

This team has an edge

from:

This edge is strong enough to speak confidently about

Key improvements:

Core Areas can now say Lean / Edge / Strong Edge instead of everything being “Edge”
near-even Team Comparison rows no longer look decisive
“Fits matchup” is rare but still reachable
Model Trust fallback now matches visible Team Comparison restraint
frontend fallback copy is softer and less absolute
no new sections, badges, or clutter were added
What Stayed Unchanged

No changes were made to:

backend model logic
winner/prediction logic
matchup lean calculation
outcome confidence logic
score math
Core Area math
routing/fetching
page layout
major component structure

This was a frontend language/contract alignment pass only.

Remaining Note

One future backend-copy improvement was observed:

For severe incorrect outcomes like 20251222_SF@IND, backend Model Trust wording such as:

held the broader matchup edge

may eventually need softer miss-severity-aware language.

That is not a frontend bug. It belongs in backend Model Trust copy calibration later.

Commit Message
Tighten frontend matchup language and support restraint

Alternative:

Align frontend matchup labels, confidence, and support badges
Final Status

Frontend alignment batch is complete and tested.

Safe to commit.

--randomly started testing features--

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

Feature Lab #3 result: useful, but not as “drive sustainability” yet 🏈🧪

This one is a mixed win. It found signal, but not exactly where we expected.

Coverage read

Your coverage output shows the available ingredients are mostly:

1st_down_rate
third_down_pct
points_per_play
yards_per_play
td_rate

But the actual SQL feature mostly used:

1st_down_rate
third_down_pct
points_per_play

The missing “true drive control” inputs — total_drives, time_of_possession, total_plays — did not really show up in the result. So this is not yet a true drive-sustainability feature. It is more like:

drive_conversion_plus_scoring_support_v0

The coverage also shows points_per_play is much stronger than 1st_down_rate and third_down_pct, while td_rate is weak/noisy.

Aggregated result
Broad offensive claims
Bucket	Rows	Validation rate
drive_supportive	712	59.4%
drive_conflict	351	51.0%
drive_mixed	204	50.0%

This is useful. Not perfect, but drive_supportive has a clear lift.

Scoring-related claims
Bucket	Rows	Validation rate
drive_supportive	1,888	54.5%
drive_conflict	796	36.6%
drive_mixed	416	35.6%

This is the strongest result. It says the feature helps scoring-related language more than pure drive language.

Narrow drive claims
Bucket	Rows	Validation rate
drive_conflict	378	52.6%
drive_supportive	1,493	51.6%
drive_mixed	410	43.4%

This is the warning sign. For true drive claims, drive_supportive is not clearly better than conflict.

So I would not call this a clean drive-sustainability feature.

Best combo

The best version is when it agrees with two_way_context.

Narrow drive claims
two_way_context = supportive
+
drive_support_bucket = drive_supportive
=
516 rows, 334 validated, ~64.7%

That is good.

Broad offensive claims
two_way_context = supportive
+
drive_support_bucket = drive_supportive
=
266 rows, 184 validated, ~69.2%

Also very good.

Scoring-related claims
two_way_context = supportive
+
drive_support_bucket = drive_supportive
=
676 rows, 399 validated, ~59.0%

Good, though not as strong as broad offense.

My decision
✅ Keep as candidate

But rename it.

Do not call it:

drive_sustainability_support_score

That name overpromises.

Better:

drive_conversion_scoring_support_v0

or:

offensive_sustainability_support_v0
Best use

Use it as claim-language support only when:

two_way_context = supportive
AND drive_support_bucket = drive_supportive

Best target areas:

broad_offensive_claim
scoring_related_claim
narrow_drive_claim, but only with two_way_context supportive
Do not use it for
automatic winner confidence
Model Trust
standalone drive-control warning
confidence cap

The drive_conflict bucket is not reliable as a warning. It validated too well in some places.

My plain-English verdict

This feature is useful, but not for the reason we thought.

It does not cleanly answer:

Can this team sustain drives?

It better answers:

When the broader two-way profile is already supportive, does the team also have enough conversion/scoring support to make offensive or scoring claims more trustworthy?

That is still useful. It just belongs in the support-language toolkit, not the confidence engine.

## GameLens Feature Testing Notes

Today we tested several SQL-only feature candidates before committing anything to code.

### 1. Rush Claim Support
Tested a `rushing_control_score` idea, then refined it after the first bucket logic marked everything unavailable. After fixing the availability gate, the signal looked useful only for narrow rushing claims. Best future name: `rush_claim_support_score` or `rushing_efficiency_support_score`. Use only for rushing-related claim support, not winner confidence or broad drive-control warnings.

### 2. Passing Efficiency Support
Tested `passing_efficiency_support_score`, which mostly behaved like a `yards_per_pass` support feature. This was the strongest candidate so far. `passing_supportive` showed useful lift for narrow passing claims and broad offensive claims, especially when paired with `two_way_context = supportive`. Best future name: `passing_efficiency_claim_support_v0`.

### 3. Drive / Conversion Support
Tested `drive_sustainability_support_score`, but the data showed it was not really true drive sustainability because volume/control inputs were limited. It worked better as a conversion/scoring support feature. Best future name: `drive_conversion_scoring_support_v0` or `offensive_sustainability_support_v0`. Use only as claim-language support, especially when `two_way_context = supportive`.

### Overall Takeaway
The SQL-first feature lab workflow worked well:
define a football idea, build a temporary SQL feature, bucket it, compare validation rates, and only then decide if it deserves code.

Current feature ranking:
1. `passing_efficiency_claim_support_v0` — strongest candidate.
2. `drive_conversion_scoring_support_v0` — useful with two-way support.
3. `rush_claim_support_score` — promising but narrow.
4. Broad `rushing_control_score` / confidence-cap use — not ready.

| Rank | Feature                               | Status                                      |
| ---: | ------------------------------------- | ------------------------------------------- |
|    1 | `passing_efficiency_claim_support_v0` | Strongest candidate                         |
|    2 | `scoring_claim_support_v0`            | Useful, especially with two-way support     |
|    3 | `drive_conversion_scoring_support_v0` | Useful but misnamed as drive sustainability |
|    4 | `rush_claim_support_score`            | Real but narrow                             |
|    5 | Broad `rushing_control_score`         | Not ready                                   |

--number 6--

| Rank | Feature                               | Status                             |
| ---: | ------------------------------------- | ---------------------------------- |
|    1 | `passing_efficiency_claim_support_v0` | Strongest clean support candidate  |
|    2 | `explosive_offense_warning_v0`        | Best warning / fragility candidate |
|    3 | `scoring_claim_support_v0`            | Useful scoring-language support    |
|    4 | `drive_conversion_scoring_support_v0` | Useful but conditional             |
|    5 | `rush_claim_support_score`            | Real but narrow                    |
|    6 | Broad `rushing_control_score`         | Not ready                          |

--number 7--
| Rank | Feature                               | Status                              |
| ---: | ------------------------------------- | ----------------------------------- |
|    1 | `passing_efficiency_claim_support_v0` | Strongest clean support candidate   |
|    2 | `explosive_offense_warning_v0`        | Best warning / fragility candidate  |
|    3 | `defensive_resistance_support_v0`     | Strong defensive-language candidate |
|    4 | `scoring_claim_support_v0`            | Useful scoring-language support     |
|    5 | `drive_conversion_scoring_support_v0` | Useful but conditional              |
|    6 | `rush_claim_support_score`            | Real but narrow                     |
|    7 | Broad `rushing_control_score`         | Not ready                           |

--number 8--

| Rank | Feature                               | Status                              |
| ---: | ------------------------------------- | ----------------------------------- |
|    1 | `offense_vs_defense_collision_v0`     | Best combo / matchup-path candidate |
|    2 | `passing_efficiency_claim_support_v0` | Strongest clean single-area support |
|    3 | `explosive_offense_warning_v0`        | Best warning / fragility candidate  |
|    4 | `defensive_resistance_support_v0`     | Strong defensive-language candidate |
|    5 | `scoring_claim_support_v0`            | Useful scoring-language support     |
|    6 | `drive_conversion_scoring_support_v0` | Useful but conditional              |
|    7 | `rush_claim_support_score`            | Real but narrow                     |
|    8 | Broad `rushing_control_score`         | Not ready                           |


--number 9--

| Rank | Feature                               | Status                                     |
| ---: | ------------------------------------- | ------------------------------------------ |
|    1 | `offense_vs_defense_collision_v0`     | Best core matchup-path feature             |
|    2 | `matchup_fragility_warning_v0`        | Best combo language/restraint feature      |
|    3 | `passing_efficiency_claim_support_v0` | Strong clean single-area support           |
|    4 | `explosive_offense_warning_v0`        | Good opponent-upside warning               |
|    5 | `defensive_resistance_support_v0`     | Strong defensive-language candidate        |
|    6 | `scoring_claim_support_v0`            | Useful scoring support                     |
|    7 | `drive_conversion_scoring_support_v0` | Useful but conditional                     |
|    8 | `rush_claim_support_score`            | Real but likely superseded by matchup path |
|    9 | Broad `rushing_control_score`         | Not ready                                  |

--number 10--
| Rank | Feature                               | Status                                        |
| ---: | ------------------------------------- | --------------------------------------------- |
|    1 | `offense_vs_defense_collision_v0`     | Best core matchup-path feature                |
|    2 | `matchup_fragility_warning_v0`        | Best combo language/restraint feature         |
|    3 | `passing_efficiency_claim_support_v0` | Strong clean single-area support              |
|    4 | `defensive_resistance_support_v0`     | Strong defensive-language candidate           |
|    5 | `defense_vs_dynamite_v0`              | Good narrow defensive caution/support feature |
|    6 | `explosive_offense_warning_v0`        | Good opponent-upside warning                  |
|    7 | `scoring_claim_support_v0`            | Useful scoring support                        |
|    8 | `drive_conversion_scoring_support_v0` | Useful but conditional                        |
|    9 | `rush_claim_support_score`            | Real but likely superseded by matchup path    |
|   10 | Broad `rushing_control_score`         | Not ready                                     |

GameLens Feature Lab Handoff — Claim-Language Feature Candidates
Purpose

We ran a SQL-first feature lab to test whether new pregame features help football claims validate more often.

This was intentionally not a winner-prediction test.

The core question was:

Does this feature help GameLens speak more truthfully about the matchup?

The best use case for these features is:

claim-language support
language restraint
“soften / no boost / caution” metadata
matchup-path explanation
future claim_strength_language_signal

The features should not be used yet for:

automatic winner confidence
Model Trust override
matchup lean override
automatic High Confidence logic
Current Feature Ranking
Rank	Feature	Status	Best Use
1	offense_vs_defense_collision_v0	Best core matchup-path feature	Offensive/rushing/scoring claim language
2	matchup_fragility_warning_v0	Best combo language/restraint feature	Clean path vs strength-on-strength vs stressed offense
3	passing_efficiency_claim_support_v0	Strongest clean single-area support	Passing and broad offensive claim support
4	defensive_resistance_support_v0	Strong defensive-language candidate	Defensive Control / suppression language
5	defense_vs_dynamite_v0	Good narrow defensive support/caution feature	Narrow defensive claim language
6	explosive_offense_warning_v0	Good opponent-upside warning	Fragility / caution metadata
7	scoring_claim_support_v0	Useful scoring support	Scoring Efficiency / points-per-play language
8	drive_conversion_scoring_support_v0	Useful but conditional	Conversion/scoring support with two-way context
9	rush_claim_support_score	Real but likely superseded by matchup-path logic	Narrow rushing claims only
10	broad rushing_control_score	Not ready	Do not promote yet
1. offense_vs_defense_collision_v0
What it tests

Whether a claimed offense has support and whether the opponent defense gives it a clean path or resistance.

Plain-English question:

Can this offense actually attack this defense?

Why it matters

This was the most “GameLens” feature because it combines offensive support with opponent defensive context instead of just saying “Team A is good.”

Result

This was the best overall feature candidate.

Strong buckets:

offense_clear_path
offense_supported

Caution buckets:

strength_on_strength
opponent_defense_warning
offense_stressed

The earlier collision test showed strong separation across broad offensive, passing, rushing, and scoring claims. offense_clear_path and offense_supported validated well, while offense_stressed was a very strong warning bucket.

Recommended use

Use for:

offensive claim support
passing/rushing/scoring matchup-path language
suppressing boost when the offense is stressed
softening strength-on-strength claims

Do not use for:

winner confidence
Model Trust
matchup lean override
Product language
The offensive claim has a clearer matchup path because the opponent defense does not show matching resistance.
The offense is supported, but this is more strength-on-strength than a clean advantage.
The offensive read should stay measured because the opponent defense shows resistance.
2. matchup_fragility_warning_v0
What it tests

This combined:

offensive path / collision
opponent explosive upside
low-fragility vs high-fragility matchup context

Plain-English question:

Even if the claimed side has a clean offensive path, is the profile fragile?

Result

The first row-level output looked noisy, but after aggregating buckets, this became much more useful.

Best all-runs findings:

Claim Scope	Strong Bucket	Validation	Lift
broad offensive	clean_path_low_fragility	67.9%	+16.2 pts
broad offensive	supported_path_low_fragility	65.4%	+13.7 pts
rushing	supported_path_low_fragility	80.0%	+26.0 pts
rushing	clean_path_low_fragility	72.1%	+18.1 pts
scoring	supported_path_low_fragility	64.3%	+17.1 pts
scoring	clean_path_low_fragility	58.6%	+11.4 pts

Worst/caution buckets were also meaningful:

Claim Scope	Caution Bucket	Validation	Lift
broad offensive	offense_stressed_fragility	23.6%	-28.0 pts
broad offensive	strength_on_strength_fragility	35.0%	-16.7 pts
scoring	offense_stressed_fragility	24.7%	-22.5 pts
rushing	strength_on_strength_fragility	41.6%	-12.3 pts

This deserves promotion as a language-control feature, not a winner feature.

Recommended use

Use for:

claim_strength_language_signal
language_boost_allowed
confidence_modifier
caution_only
soften
no_boost
Suggested mapping
Bucket	Behavior
clean_path_low_fragility	allow stronger support language
supported_path_low_fragility	allow support language
fragility_mixed	normal language
strength_on_strength_fragility	soften language
opponent_defense_fragility	caution / no boost
general_explosive_fragility	caution
offense_stressed_fragility	strong caution / suppress boost
3. passing_efficiency_claim_support_v0
What it tests

Whether pregame passing-efficiency support helps passing-related claims validate.

Result

This was the best single-area support feature.

It mostly behaved like a yards_per_pass feature, not a full passing bundle.

Strong result:

passing_supportive validated better than passing_mixed
especially useful for narrow passing claims and broad offensive claims
strongest when paired with two_way_context = supportive
Recommended use

Use for:

yards_per_pass
Passing Game claims
narrow passing claims
broad offensive claims when paired with supportive context

Do not use for:

passing volume claims
automatic winner confidence
Model Trust
Product language
The passing profile has claim-level support, especially with broader two-way context also backing the read.
4. defensive_resistance_support_v0
What it tests

Whether a defense has pregame support for suppressing opponent efficiency.

Result

This was a strong defensive-language candidate.

Best findings:

narrow defensive claims: defense_supportive validated much better than mixed
broad defensive claims also improved
strongest with two_way_context = supportive

The current available inputs were narrower than ideal, mostly around:

points_allowed_per_play
Defensive Control context

So the more honest current name might be:

points_allowed_defensive_support_v0

But defensive_resistance_support_v0 is still acceptable if documented carefully.

Recommended use

Use for:

Defensive Control claim support
points_allowed_per_play claims
defensive suppression language
opponent offensive caution

Do not use for:

disruption/turnover claims
sacks
winner confidence
5. defense_vs_dynamite_v0
What it tests

Whether a supported defense still becomes fragile when the opponent has explosive offensive upside.

Plain-English question:

Can this defense hold up if the opponent has dynamite?

Result

This worked well for narrow defensive claims, but was messier for broad defensive and disruption-adjacent claims.

Best all-runs narrow defensive findings:

Bucket	Validation	Lift
defense_clean_suppression_path	65.1%	+12.5 pts
defense_supported_normal_risk	57.7%	+5.0 pts
defense_vs_dynamite_warning	44.9%	-7.8 pts
defense_dynamite_mixed	42.2%	-10.5 pts
mixed_defense_vs_dynamite	34.8%	-17.9 pts
dynamite_unavailable	31.3%	-21.4 pts

This is a clean ladder for narrow defensive claims.

Recommended use

Use for:

narrow defensive claims
points_allowed_per_play
points_allowed_per_yard
defensive suppression language
defensive caution when opponent explosiveness is high

Do not use for:

disruption-adjacent claims
turnovers
sacks
broad Defensive Control suppression unless revalidated
Suggested mapping
Bucket	Behavior
defense_clean_suppression_path	allow stronger defensive suppression language
defense_supported_normal_risk	normal support language
defense_vs_dynamite_warning	caution / soften for narrow defensive claims
mixed_defense_vs_dynamite	no boost
weak_defense_explosive_opponent	strong caution if used later
dynamite_unavailable	no boost
6. explosive_offense_warning_v0
What it tests

Whether opponent explosive offensive upside makes claims more fragile.

Result

This was useful as a warning feature, not a normal support feature.

The important finding was that opponent explosive threat reduced validation for broad offensive and defensive-resistance claims.

Recommended use

Use for:

opponent-upside warning
broad offensive claim caution
defensive resistance caution
fragility metadata

Do not use for:

winner confidence
automatic high-confidence boost
Model Trust override
Product language
Confidence stays measured because the opponent carries explosive offensive upside despite the broader matchup read.
7. scoring_claim_support_v0
What it tests

Whether pregame scoring-conversion support helps scoring-related claims validate.

Result

Useful, but not a “wow” feature.

It mostly confirmed that points_per_play is a strong support metric.

Best finding:

scoring_supportive was clearly better than mixed/conflict for narrow scoring claims
broad offensive claims improved somewhat
strongest when paired with two_way_context = supportive
Recommended use

Use for:

Scoring Efficiency language
points_per_play
scoring-related claim support
broad offensive support only with supportive two-way context

Do not use for:

red-zone-specific boost
td_rate boost
drive-control warning
winner confidence
8. drive_conversion_scoring_support_v0
What it tests

Originally framed as drive_sustainability_support_score.

But the data did not really support true drive sustainability because true drive/volume inputs were limited.

It mostly tested:

1st_down_rate
third_down_pct
points_per_play
Result

Useful, but misnamed.

Better name:

drive_conversion_scoring_support_v0

or:

offensive_sustainability_support_v0

Strongest when paired with two_way_context = supportive.

Recommended use

Use for:

scoring-related claims
broad offensive claims
narrow drive claims only when two-way context is supportive

Do not use for:

standalone drive-control warning
confidence cap
Model Trust
9. rush_claim_support_score
What it tests

Whether pregame rushing support helps rushing-related claims validate.

Result

This was useful only for narrow rushing claims.

The first SQL test failed because everything bucketed as unavailable. After fixing the availability gate, the feature showed real lift for narrow rushing claims.

Recommended use

Use for:

yards_per_rush
Rushing Game
narrow rushing claims

Do not use for:

broad drive-control claims
winner confidence
matchup confidence
broad rushing-control warnings
Important note

This feature may be partially superseded by:

offense_vs_defense_collision_v0

and:

matchup_fragility_warning_v0

Those combo features performed better for rushing-claim support.

10. Broad rushing_control_score
What it tests

Whether rushing/control affects overall game shape.

Result

Not ready.

The narrow rushing signal was real, but the broader drive-control / game-control behavior was messy.

The feature should not be promoted as:

rushing_control_score

because that name overpromises.

Recommended use

Do not implement broad rushing control yet.

Maybe revisit later with better inputs:

rushing attempts
run play percentage
rushing success rate
time of possession
early-down rush efficiency
opponent run defense
game-script-adjusted rushing share
Revalidation Plan

Before coding anything, I would revalidate the list with a cleaner second pass.

Step 1 — Keep aggregated output as default

All future SQL tests should output:

all runs combined
by run
by two_way_context
validation rate
baseline validation rate
lift vs baseline
row count filter

The raw row explosion should only be used for debugging.

Step 2 — Revalidate top candidates only

Priority revalidation order:

offense_vs_defense_collision_v0
matchup_fragility_warning_v0
passing_efficiency_claim_support_v0
defensive_resistance_support_v0
defense_vs_dynamite_v0

Do not spend more time right now on:

broad rushing_control_score
disruption/turnover explanation features
red-zone standalone support
td_rate standalone support
Step 3 — Validate by claim type/layer

For each candidate, split by:

claim_type
claim_layer
metric
category
core_area
two_way_context
sample size bucket

This matters because some features work for:

metric_highlight / supporting

but may not work for:

core_area_summary / headline
Step 4 — Decide product behavior, not just feature rank

Each feature should map to one of these product behaviors:

Behavior	Meaning
allow_boost	stronger claim language allowed
normal	no special language change
soften	weaker wording
caution_only	warning copy allowed, no boost
no_boost	suppress “Fits matchup” / support badge
block	do not use for this claim type
Suggested Promotion Tiers
Tier 1 — Build around these
offense_vs_defense_collision_v0
matchup_fragility_warning_v0

These are the strongest GameLens-style matchup intelligence features.

Tier 2 — Add as support metadata
passing_efficiency_claim_support_v0
defensive_resistance_support_v0
defense_vs_dynamite_v0
scoring_claim_support_v0

These are useful, but should stay scoped.

Tier 3 — Keep as research/backlog
drive_conversion_scoring_support_v0
rush_claim_support_score

Useful, but not urgent.

Tier 4 — Do not promote
broad rushing_control_score

Not enough evidence yet.

Final Handoff Note

The biggest lesson from this feature lab is that combo features beat isolated stat support.

The best candidates were not simply:

Team A has better metric X.

They were:

Team A has a claim-level strength, the opponent profile either gives it a clean path or creates resistance, and the language should adjust accordingly.

That is the GameLens direction worth revalidating. 🏈