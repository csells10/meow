# GameLens Backburner Cleanup / v1.6.3 Polish Queue

_Last updated: May 13, 2026_

## Purpose

This file captures small cleanup ideas discovered during v1.6.2 QA.

These are **not urgent** and should not interrupt mission-critical backend work, builder automation planning, or larger v1.7/v2 feature planning.

Use this file when there is time for a focused polish pass.

---

# Product Rule to Preserve

```text
Matchup Lean owns pregame confidence.
Model Trust & Outcome owns postgame validation.
The diagnostic area explains what supported or challenged the read.
```

Do not reintroduce duplicate confidence language.

---

# Best Candidate for v1.6.3

## 1. Improve Game Profile Signal Summary Wording

Status:

```text
Backburner / likely small frontend or backend wording cleanup
```

Current issue found during QA:

```text
Game Profile Signals can say “All signals agreed” even when one signal is neutral.
```

Example:

```text
Pressure was neutral
Turnover Risk favored ATL
Scoring Efficiency favored ATL
Game Profile Signals: All signals agreed
```

Why this is slightly misleading:

```text
A neutral signal did not agree. It simply did not oppose the lean.
```

Better wording options:

```text
No opposing Game Profile signals
Directional signals agreed
No Game Profile signals pushed back
No opposing signals
```

Recommended wording:

```text
No opposing Game Profile signals
```

Reason:

```text
It is honest when one or more signals are neutral, but the remaining directional signals point with the lean.
```

Possible implementation locations:

```text
services/model_trust_service.py
src/pages/Matchup.tsx
```

Preferred long-term fix:

```text
Backend should own the summary wording because model_trust.signal_alignment.summary_label / summary is backend-owned reasoning.
```

Lowest-risk frontend-only fix:

```text
If summary says “All signals agreed” and at least one signal row has favored_side neutral or aligns neutral, display “No opposing Game Profile signals.”
```

Do not implement until reviewed.

---

## 2. Make Matchup Lean Confidence Summaries More Context-Aware

Status:

```text
Backburner / useful polish
```

Current issue:

Some confidence summaries are technically true but generic.

Current examples:

```text
Confidence: This should be treated cautiously rather than as a strong outcome read.
Confidence: The profile is strong, but outcome confidence is kept measured.
```

These are okay, but they do not always explain *why* confidence is low or measured.

Better examples:

```text
Confidence: Kept low because Game Profile signals were mixed.
Confidence: Kept low because Core Areas were close.
Confidence: Kept measured because the profile was useful, but not overwhelming.
Confidence: Kept measured because the visible comparison metrics supported the lean, but not every football signal agreed.
```

Useful trigger logic:

```text
core_area_gap_is_small → mention Core Areas are close
core_areas_are_split → mention Core Areas are split
core_areas_do_not_fully_confirm_lean → mention broader profile is mixed
mixed Game Profile signals → mention Game Profile signals were mixed
strong profile + medium confidence → mention profile was useful but not overwhelming
strong profile + low confidence → mention the clean comparison profile had signal pushback or caution flags
```

Possible implementation locations:

```text
services/game_service.py
services/model_trust_service.py
src/pages/Matchup.tsx
```

Preferred long-term fix:

```text
Backend should generate sharper outcome_confidence.summary text because it knows the reason for the confidence cap.
```

Frontend should avoid inventing reasoning if the backend already provides it.

---

## 3. Review `confidence_context` Placement and Redundancy

Status:

```text
Backburner / not urgent
```

Current state:

```text
confidence_context still renders in Model Trust & Outcome below Predicted vs Actual.
```

This was intentionally left in place during v1.6.1.

Why it may need review later:

```text
Matchup Lean now displays confidence summaries.
Model Trust also displays confidence_context as postgame context.
There may be mild wording overlap.
```

Current interpretation:

```text
Acceptable for now.
```

Possible future options:

- keep it as postgame caution context
- rename it visually to `Pregame caution`
- only show it when it adds information not already present in Matchup Lean
- move it into the diagnostic area

Do not change yet without a quick audit.

---

## 4. Keep `Why cautious` Useful and Non-Debuggy

Status:

```text
Mostly complete / continue watching
```

Current completed behavior:

```text
Raw backend caution codes are mapped to user-facing labels.
```

Known mapping:

```text
core_area_gap_is_small → Core areas are close
core_areas_are_split → Core areas are split
core_areas_do_not_fully_confirm_lean → Broader profile is mixed
supporting_context_is_mixed_or_limited → Supporting context is limited
strong_profile_does_not_guarantee_outcome → hidden
```

Backburner watch item:

```text
If more backend caution codes appear, add them to the map or hide them until there is a clean user-facing phrase.
```

Do not show raw snake_case backend codes to users.

---

## 5. Light/Dark Visual QA for Matchup Read and Model Trust

Status:

```text
Backburner / visual polish
```

Screenshots looked generally good in both dark and light mode.

Still worth checking later:

- chip contrast in light mode
- caution chip visibility
- diagnostic row alignment
- tooltip readability
- mobile wrapping for `Medium Confidence` / `High Confidence`
- spacing when `Why cautious` is absent
- spacing when `Why cautious` has multiple chips

Do not spend backend time on this.

---

## 6. Better No Pick Language

Status:

```text
Backburner / wording polish
```

Current No Pick behavior is much cleaner after duplicate confidence removal.

Potential future polish:

```text
No Pick / Low Confidence
Profile: The available signals do not create a clean matchup lean.
Confidence: This should be treated cautiously rather than as a strong outcome read.
```

Possible improved wording:

```text
Profile: The available signals did not create enough separation for a lean.
Confidence: Low because the matchup stayed too balanced to force a call.
```

Do not change unless the wording repeatedly feels awkward during QA.

---

## 7. Model Trust Diagnostic Defaults

Status:

```text
Backburner / watch item
```

Current diagnostic title is improved:

```text
What supported or challenged the read?
```

Current rows are improved:

```text
Team Comparison Support
Game Profile Signals
```

Future optional improvement:

```text
Add a one-line plain-English bridge when Team Comparison Support and Game Profile Signals conflict.
```

Example:

```text
Visible comparison metrics supported the lean, but Game Profile signals were mixed.
```

This may be useful later, but it is not necessary right now because the new labels already carry most of the meaning.

Potential implementation location:

```text
services/model_trust_service.py
```

---

# Possible v1.6.3 Scope

A small v1.6.3 should be limited to wording and polish only.

Good v1.6.3 scope:

- improve “All signals agreed” wording when neutral signals exist
- sharpen confidence summary wording if backend reason is already available
- add/adjust caution label mappings
- minor light/dark/mobile visual polish

Bad v1.6.3 scope:

- backend scoring changes
- new API fields
- matchup_breakdown UI
- dynamic drivers
- League Discovery
- injury context
- Field Control repair
- builder automation
- full redesign

---

# Suggested v1.6.3 Planning Prompt Later

```text
We need a planning-only pass for small v1.6.3 wording polish.

Do not implement yet.

Please audit Model Trust signal summary wording and Matchup Lean confidence summaries.

Specific issues:

1. “All signals agreed” may be misleading when one Game Profile signal is neutral. A better phrase may be “No opposing Game Profile signals” or “Directional signals agreed.”

2. Matchup Lean confidence summaries can be generic. When confidence is low or measured because Game Profile signals are mixed, Core Areas are close, or Core Areas are split, the summary should say that directly if the backend already knows the reason.

Please identify whether these strings are backend-owned or frontend-owned, recommend the smallest safe fix, and list the files likely touched.

No backend scoring changes. No layout changes. No new UI sections.
```

---

# Priority Recommendation

If only one item is done later, do this:

```text
Replace “All signals agreed” with “No opposing Game Profile signals” when neutral signals are present.
```

That is the cleanest small trust improvement found during v1.6.2 QA.
