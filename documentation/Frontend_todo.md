# GameLens Frontend Stopping Point — v1.7.x Review Notes

_Last updated: May 15, 2026_

## Current stopping point

Today was a major frontend/product translation session.

We took several backend/API ideas and turned them into clearer user-facing frontend concepts:

```text
two_way_context → Fits matchup
category_summaries → What’s shaping this matchup
drivers → Key inputs
parent signal grouping → Game Profile explanation layer

This is a good stopping point before adding more frontend features.

The next session should not start by coding a new feature immediately.

The next session should start by reviewing which /game API elements are still not being used in the frontend, deciding whether they belong on the page, and deciding where they should live.

What we accomplished
1. Team Comparison: two_way_context became “Fits matchup”

Backend concept:

two_way_context / language_support

Frontend concept:

Fits matchup

Meaning:

This stat edge lines up with the broader pregame matchup profile.
It does not mean the team is guaranteed to win.

Final frontend treatment:

Fits matchup badge appears next to the winning value.
Supported rows get a subtle persistent treatment.
Hover behavior was calmed down so normal rows do not look more important than supported rows.
Backend jargon is hidden from users.

Important product boundary:

Fits matchup is row-level evidence support.
It is not a winner prediction.
It is not a betting signal.
It is not a confidence upgrade by itself.
2. Game Profile: added “What’s shaping this matchup”

Backend concept:

matchup_breakdown.category_summaries

Frontend concept:

What’s shaping this matchup

Purpose:

Explain the context behind the Game Profile signals above.

This belongs inside the existing Game Profile card because Game Profile answers:

What kind of matchup is this?

The new subsection answers:

What is shaping that profile?
3. Driver labels became “Key inputs”

Backend concept:

category_summaries[].drivers

Frontend concept:

Key inputs: First Down Rate, Yards Per Play.

Why this matters:

The first version only showed summaries like:

Drive Conversion leans toward GB.

That was technically correct but too thin.

Adding Key inputs makes the section more useful by showing the actual metric labels driving the category read.

Current rule:

Render max 2 driver labels.
Keep the line muted.
Do not render raw backend slugs.
Do not render driver gaps, team IDs, or backend summaries yet.
Latest frontend direction: v1.7.11

The most recent frontend direction is to group matchup-shaping rows under their parent Game Profile signal.

This is better than relying on color alone.

Why the change was needed

Earlier versions tried to use color to connect detail rows back to Game Profile.

That became confusing when multiple Game Profile tiles had the same color.

Example:

Turnover Risk = Elevated
Scoring Efficiency = Elevated

If both are gold, the user still cannot tell which context row explains which signal.

So the lesson was:

Color alone is not enough.
The parent-child relationship needs to be explicit.
Preferred structure
What’s shaping this matchup
Context behind the signals above.

Scoring Efficiency
Elevated · TB more efficient scoring

Drive Conversion
Drive Conversion leans toward TB.
Key inputs: Third Down %.

Offensive Rhythm
Offensive Rhythm leans toward TB.
Key inputs: Yards Per Play, First Down Rate.

Red Zone Finish
Red Zone Finish leans toward TB.
Key inputs: Red Zone Efficiency.
Product meaning

This structure clearly says:

These rows explain this Game Profile signal.

That is the right mental model.

v1.7.11 frontend rules
Grouping behavior

Group rows by mapped parent Game Profile signal:

Pressure
Turnover Risk
Scoring Efficiency
Explosiveness
Defensive Stability
Tempo
Other matchup context
Parent header behavior

Parent headers may show:

Scoring Efficiency
Elevated · TB more efficient scoring

The parent header carries the color/accent.

Child row behavior

Child rows should be mostly neutral/slate.

They should show:

Category label
Summary sentence
Key inputs line, when available

Example:

Drive Conversion
Drive Conversion leans toward TB.
Key inputs: Third Down %.
Fallback behavior

If a row cannot map cleanly to a visible Game Profile signal:

Other matchup context

If there are no usable rows:

Hide the entire subsection.
Important product guardrails

Do not make GameLens feel like a forced pick machine.

Avoid language like:

lock
guaranteed
must pick
best bet
will win
should win

Use language like:

leans
points toward
fits the matchup
context
signal
profile
supporting read

Important distinction:

GameLens explains matchup shape.
It does not guarantee outcome.
Tomorrow’s recommended workflow

Tomorrow should be an audit + product-mapping session, not a coding sprint.

Step 1 — Review unused or underused /game API elements

Potential candidates:

ranking_context
matchup_breakdown.metric_highlights
matchup_breakdown.core_area_summaries
matchup_breakdown.context_notes
matchup_breakdown.freshness
claim_language_context
model_trust details
confidence guardrails
ranking metadata
lens_tags
Step 2 — Decide the role of each element

For each API element, decide:

Should this be user-facing?
Should it stay backend-only?
Should it be a tooltip/detail layer?
Should it be internal QA/admin only?
Should it be ignored for now?
Step 3 — Map useful fields into existing sections first

Before creating new sections, ask whether each item belongs inside:

Game Profile
Team Comparison
Matchup Lean
Model Trust & Outcome
Core Area Advantage
Final Score

Only create a new section if the data clearly does not fit anywhere else.

Step 4 — Avoid “label land”

Do not overload the page with badges and pills.

The page should not become:

badge badge badge
pill pill pill
signal label
support label
confidence label

The page should explain the matchup clearly.

Current product translation wins

These were good translations from backend to frontend:

Backend / API concept	Frontend translation
two_way_context	Fits matchup
language_boost_allowed	Subtle supported Team Comparison row
category_summaries	What’s shaping this matchup
drivers[].label	Key inputs
parent signal mapping	grouped Game Profile explanation layer
Next-session goal

The next session should answer:

What parts of the /game response are still unused?
Which ones deserve to be shown?
Where do they belong?
What should stay backend-only?

Only after that should we ask:

What frontend feature should we build next?

This keeps GameLens from becoming cluttered and protects the product identity:

A matchup intelligence and confidence-calibration tool.
Not a forced pick machine.
Suggested next Lovable prompt
Please stay in audit/planning mode only.

We have recently added:
- Team Comparison “Fits matchup”
- Game Profile “What’s shaping this matchup”
- Key inputs from category summary drivers
- v1.7.11 grouped matchup-shaping rows under parent Game Profile signals

Before adding more UI, I want to audit the remaining /game API response fields that the frontend is not using or is only partially using.

Please inspect the current frontend and provide a refreshed /game usage audit.

Specifically review:
- ranking_context
- matchup_breakdown.metric_highlights
- matchup_breakdown.core_area_summaries
- matchup_breakdown.context_notes
- matchup_breakdown.freshness
- claim_language_context
- model_trust details
- confidence guardrails
- ranking metadata
- lens_tags

For each field or section, classify it as:
1. currently used
2. partially used
3. unused but good user-facing candidate
4. backend-only / do not show
5. internal QA/admin candidate
6. ignore for now

For anything that may be user-facing, recommend where it belongs:
- Game Profile
- Team Comparison
- Matchup Lean
- Model Trust & Outcome
- Core Area Advantage
- a new section only if necessary

Guardrails:
- Do not implement anything yet.
- Do not expose backend jargon.
- Do not use claim_language_context directly.
- Do not turn the page into a pick machine.
- Avoid adding badges/pills unless they clearly improve understanding.
- Prefer improving existing sections before creating new ones.

Return:
- refreshed audit
- product recommendation
- safest next frontend candidate
- what should stay hidden
- risks of overloading the page