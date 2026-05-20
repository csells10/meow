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