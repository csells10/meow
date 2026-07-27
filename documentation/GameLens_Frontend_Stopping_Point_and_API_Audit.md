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



/game API usage audit (post v1.7.11)
Field-by-field
ranking_context — 6. ignore for now. Not present in GameDetails type, not read anywhere. Without seeing payload shape it's unclear if it's percentile rank, league-wide context, or admin metadata. Defer until backend confirms intent.

matchup_breakdown.metric_highlights — 3. unused but good user-facing candidate. Not typed, not rendered. Likely the cleanest "specific stat that drove this" payload. Best home: inline expansion inside each grouped row in What's shaping this matchup, OR as the source of truth for the existing "Key inputs:" line (replacing the current driver-label scrape). Do not make a new section.

matchup_breakdown.core_area_summaries — 3. unused but good user-facing candidate. Natural fit under Core Area Advantage as a one-line caption per tile (mirrors what category_summaries did for Game Profile). Highest-leverage next addition because Core Area Advantage tiles currently show only % bars + a frontend-authored relationship label.

matchup_breakdown.context_notes — 3 or 5, depends on tone. If they're plain-English caveats ("short week", "weather", "key injury impact") → user-facing, belongs as a muted footnote inside Matchup Lean's cautions stack. If they're model-debug strings → 5, internal QA only. Need a sample before committing.

matchup_breakdown.freshness — 5. internal QA/admin candidate. Data-recency timestamps belong on a Settings/QA surface, not the matchup page. Surfacing it to analysts risks looking like a disclaimer.

claim_language_context — 4. backend-only / do not show. Already an explicit guardrail. It exists to govern language_support.language_boost_allowed, which the UI already consumes via the Fits matchup badge. Keep invisible.

model_trust details — 1. currently used (broadly). reasoning.headline/summary/drivers, matchup_advantage, edge.strength/description/tooltip, signal_alignment.signals/summary, learning_label are all rendered. Gap: edge.score (numeric) and signal_alignment.aligned_count/total_count are typed but not displayed — leave hidden, they'd add noise.

Confidence guardrails (confidence, confidence_context, profile_strength, outcome_confidence, matchup_label, matchup_cautions) — 1. currently used. All rendered in Matchup Lean. No action needed.

Ranking metadata — 6. ignore. Not in current payload type. If it shows up as team-level league ranks it could enrich Team Comparison rows (e.g. "12th in pressure rate"), but only if backend ships it as a structured per-row field. Defer.

lens_tags — 4 or 6. Sounds like internal taxonomy/telemetry. Don't surface as pills — that's exactly the badge-creep the guardrails warn against. Keep hidden unless backend explicitly defines them as user-facing.

Product recommendation
The page is already dense: Game Profile + grouped What's shaping this matchup + Team Comparison (with Fits matchup) + Core Area Advantage + Matchup Lean + Model Trust + Outcome. Adding more sections will dilute the existing hierarchy. The next moves should enrich existing tiles, not add surfaces.

Safest next frontend candidate
matchup_breakdown.core_area_summaries → one-line caption under each Core Area Advantage tile. Reasons:

Symmetrical with the v1.7.7–v1.7.11 work that gave Game Profile its explanation layer.
Replaces the frontend-authored RELATIONSHIP_LABEL map in CoreAreaAdvantage.tsx with backend-owned copy → fewer hardcoded strings, more consistent voice.
Zero new sections, no new badges, no new collapsibles.
Second-safest: wire metric_highlights into the existing "Key inputs:" line so it stops scraping drivers[].label and instead renders backend-curated stats.

What should stay hidden
claim_language_context, freshness, lens_tags, edge.score, signal_alignment.aligned_count/total_count, and any *_code / *_index fields. These are model internals.

Risks of overloading the page
Signal collision: every new caption competes with the grouped explainer we just built. Two layers of "why" under one tile = users stop reading both.
Badge creep: Fits matchup works because it's the only pill on a Team Comparison row. A second badge type would make both feel like decoration.
Mobile (797px viewport): Core Area Advantage already wraps to 1 column; adding sub-captions there is fine, but adding another full section below would push Matchup Lean off the first scroll.
Authority dilution: the more the page explains, the more it reads like a recommendation engine. Keep Matchup Lean as the single "what the model thinks" surface.
Recommend queueing v1.7.12 = Core Area summaries only.


# GameLens Frontend Language Guide

Do not expose backend feature names.

Core Areas should tell the football story.
Metrics should support the story.
Calibration features should decide how strongly the story is worded.

Avoid:
- two_way_context
- claim language support
- supportive / available_mixed
- feature buckets

Use:
- clean read
- supported read
- fragile edge
- split profile
- stronger matchup support
- not enough separation


--updates on 5-20-2026--

# Addendum — Core Area Advantage Now Has Better Backend Display Fields

### Backstory

During the Devil’s Advocate API audit, we found that the `/game` response could expose competing directional reads for the same Core Area.

Example issue:

- `core_area_comparison` said **BUF led Disruption and Turnovers**
- `matchup_breakdown.core_area_summaries` could say the same area was **neutral / near even**
- The frontend then had to decide which layer to believe

That created a product problem:

> The user should not have to reconcile multiple visible leaders for the same Core Area.

Even if those backend layers are technically answering slightly different analytical questions, the frontend needs one coherent matchup story.

### API/Product Decision

`core_area_comparison` now owns the user-facing Core Area direction.

`matchup_breakdown.core_area_summaries` should explain the quality of supporting drivers, not create a second competing visible verdict.

In plain English:

- Core Area cards should show one visible direction.
- Driver disagreement should be explained as context.
- Backend diagnostic nuance should still be preserved, but not forced onto the user.

### Backend/API Change Completed

Updated `services/game_service.py` so `matchup_breakdown.core_area_summaries` now align their visible leader fields to `core_area_comparison`.

The previous headline-driver result is still preserved as diagnostic metadata.

New useful fields now available inside:

```text
matchup_breakdown.core_area_summaries[]

Key fields:

leader
leader_team
leader_source
display_strength
display_summary
broad_score_gap
headline_driver_leader
headline_driver_summary
driver_alignment

Example from 20251013_BUF@ATL:

{
  "name": "Disruption and Turnovers",
  "leader": "away",
  "leader_team": "BUF",
  "leader_source": "core_area_comparison",
  "display_strength": "lean",
  "display_summary": "BUF has a broad lean in Disruption and Turnovers. Headline-driver support is thin or close to even.",
  "driver_alignment": "thin_or_neutral",
  "headline_driver_leader": "neutral",
  "headline_driver_summary": "Disruption and Turnovers looks close to even.",
  "broad_score_gap": 0.166
}
Why This Improves the API Response

Before this change, the API could technically be “correct” but product-confusing.

After this change, the API is more coherent:

Layer	Role
core_area_comparison	Owns broad user-facing Core Area direction
matchup_breakdown.core_area_summaries[].leader	Now mirrors the broad visible direction
display_strength	Gives frontend better language than always saying “Edge”
display_summary	Backend-owned sentence for the Core Area card
headline_driver_leader	Preserves the old driver-level read for diagnostics
driver_alignment	Explains whether headline drivers aligned, conflicted, or were thin/neutral

This keeps the API honest while making the frontend easier to understand.

Frontend Finding After Deployment

The deployed API now includes the new Core Area summary fields.

However, the frontend still visually shows:

Disruption and Turnovers
BUF Edge
BUF 58% / ATL 42%

For 20251013_BUF@ATL, the backend now says:

display_strength: lean
display_summary: BUF has a broad lean in Disruption and Turnovers. Headline-driver support is thin or close to even.

So the frontend is not blocked by backend/API anymore.

The next frontend issue is that CoreAreaAdvantage.tsx appears to still render from the older/simple core_area_comparison fields and likely hardcodes non-neutral leaders as Edge.

Recommended Frontend Update

Update CoreAreaAdvantage.tsx so Core Area cards can use matchup_breakdown.core_area_summaries as the display/caption source when available.

Recommended behavior:

Continue using core_area_comparison for the score bars and broad percentages.
Match each card to matchup_breakdown.core_area_summaries[] by Core Area name.
Use backend-owned fields when available:
display_strength
display_summary
driver_alignment
leader_source
Keep existing fallback behavior if the new fields are missing.
Suggested Display Mapping

Use display_strength for the visible label:

display_strength	Suggested frontend label
near_even	Near Even
lean	Lean
edge	Edge
strong_edge	Strong Edge

Example:

Disruption and Turnovers

BUF Lean

BUF has a broad lean in Disruption and Turnovers. Headline-driver support is thin or close to even.

BUF 58%
ATL 42%
12 metrics
Important Product Guardrails

Do not add a new section.

This should enrich the existing Core Area Advantage tiles, not create another surface.

Avoid exposing backend terms directly:

Avoid:

driver_alignment
headline_driver_leader
leader_source
broad_score_gap

Use their meaning instead:

“Headline support is thin”
“Drivers generally support this read”
“Drivers are mixed”
“Broad lean”
“Strong broad edge”
Definition of Done

Frontend work is done when:

20251013_BUF@ATL shows BUF Lean for Disruption and Turnovers, not BUF Edge
Core Area Advantage uses display_summary when available
Existing percentage bars still render from core_area_comparison
No new badges/pills are added
Backend jargon remains hidden
The page feels more coherent without becoming more cluttered
Updated Frontend Priority

This should now be the next safest frontend candidate:

Core Area Advantage → consume matchup_breakdown.core_area_summaries display fields

This follows the same product pattern as prior frontend improvements:

Previous translation	New parallel
category_summaries explained Game Profile	core_area_summaries should explain Core Area Advantage
drivers became Key Inputs	driver_alignment becomes plain-language support context
backend jargon stayed hidden	backend diagnostic fields stay hidden
no new section added	existing Core Area cards get smarter

Recommended next version:

v1.7.12 or v1.7.13 — Core Area Advantage display-summary integration

Tiny gut check: this is a good frontend task because it does **not** add more visual clutter. It makes an existing section smarter, which is exactly the right move here.


## Frontend Performance Findings

Chrome Performance showed that the GameLens matchup page is not suffering from major layout instability; CLS was 0, which is good. The main performance issue is LCP around 2.6s, with most of the delay coming from waiting to render the main matchup content after the document and scripts load.

The likely improvement path is not a full redesign. The better next step is to make the page render a useful shell immediately — header, teams, date, and loading/skeleton cards — while Firebase auth, account lookup, route chunks, and the game API response finish loading.

Team logos should also be optimized. Current 500px PNG logo assets are larger than needed for the UI and have short cache lifetimes. Smaller logo assets and longer cache headers would reduce repeat-load cost.

DevTools also showed browser extension activity, especially Aura, contributing main-thread time. Future measurements should be run in Incognito or a clean browser profile before treating results as app-owned performance problems.