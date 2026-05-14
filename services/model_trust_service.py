import random


def build_model_trust(
    game_profile: list,
    team_comparison: list,
    matchup_lean: dict,
    model_outcome: dict,
    header: dict,
) -> dict:
    """
    Builds backend-owned model trust fields for the matchup page.

    This keeps reasoning logic out of the frontend.
    """

    matchup_advantage = build_matchup_advantage(team_comparison)
    edge = build_edge(matchup_advantage)
    learning_label = build_learning_label(model_outcome)

    signal_alignment = build_signal_alignment(
        game_profile=game_profile,
        matchup_lean=matchup_lean,
        model_outcome=model_outcome,
        header=header,
    )

    reasoning = build_reasoning(
        team_comparison=team_comparison,
        matchup_advantage=matchup_advantage,
        matchup_lean=matchup_lean,
        model_outcome=model_outcome,
        edge=edge,
        signal_alignment=signal_alignment,
        header=header,
    )

    return {
        "reasoning": reasoning,
        "matchup_advantage": matchup_advantage,
        "edge": edge,
        "signal_alignment": signal_alignment,
        "learning_label": learning_label,
    }


def build_matchup_advantage(team_comparison: list) -> dict:
    """
    Count visible Team Comparison advantages.

    Important:
    - away/home rows count as directional advantages.
    - neutral rows are preserved as useful matchup context.
    - neutral rows should not create fake team advantage.
    """

    away_count = 0
    home_count = 0
    neutral_count = 0

    for row in team_comparison or []:
        better = row.get("better")

        if better == "away":
            away_count += 1
        elif better == "home":
            home_count += 1
        elif better == "neutral":
            neutral_count += 1

    decisive_count = away_count + home_count
    total_visible = decisive_count + neutral_count

    if away_count > home_count:
        leader = "away"
    elif home_count > away_count:
        leader = "home"
    else:
        leader = "tie"

    return {
        "visible": bool(team_comparison),
        "away": away_count,
        "home": home_count,
        "neutral": neutral_count,
        "decisive": decisive_count,
        "total_visible": total_visible,
        "leader": leader,
        "tooltip": (
            "Counts how many visible Team Comparison metrics favored each team. "
            "Neutral rows mean the metric was even and did not create separation. "
            "This is a directional count, not the full model score."
        ),
    }


def build_edge(matchup_advantage: dict) -> dict:
    away = matchup_advantage.get("away", 0)
    home = matchup_advantage.get("home", 0)
    neutral = matchup_advantage.get("neutral", 0)

    diff = abs(away - home)
    decisive_total = away + home
    total_visible = matchup_advantage.get("total_visible", decisive_total + neutral)

    # Use total_visible so neutral rows soften the displayed edge score.
    # Example: 1 away edge, 0 home edges, 4 neutral rows = 0.20, not 1.00.
    edge_score = round(diff / total_visible, 2) if total_visible else 0

    if diff >= 3 and neutral == 0:
        strength = "strong"
        tooltip = (
            "The visible Team Comparison metrics show clear separation. Core Area context and Game Profile signals may still add nuance."
        )
    elif diff >= 3:
        strength = "moderate"
        tooltip = ("Some Team Comparison metrics lean one way, but several even areas keep the edge from looking clean."
)
    elif diff >= 2:
        strength = "moderate"
        tooltip = (
            "The visible Team Comparison metrics show a noticeable lean, but other matchup signals still matter."
        )
    elif diff >= 1:
        strength = "low"
        tooltip = (
            "The visible Team Comparison metrics show only a small edge. Use this as supporting context, not a standalone conclusion."
        )
    else:
        strength = "none"
        tooltip = (
            "The visible Team Comparison metrics are evenly split, neutral, or unavailable."
        )

    return {
        "strength": strength,
        "score": edge_score,
        "tooltip": tooltip,
        "has_content": total_visible > 0,
    }


def build_learning_label(model_outcome: dict) -> str:
    """
    Returns a user-facing learning label based on model outcome.
    Backend owns all messaging — frontend should display as-is.
    """

    if not model_outcome:
        return "Outcome not available yet"

    result = str(model_outcome.get("result") or "").lower()

    if result == "correct":
        return "Model aligned with outcome"

    if result == "incorrect":
        return "Model miss — learning opportunity logged"

    if result == "no pick":
        return "Neutral — model avoided low-confidence scenario"
    
    if result in {"tie", "push", "no decision", "no_decision"}:
        return "Tie game — model accuracy not graded"

    return "Outcome not available yet"


def build_signal_alignment(
    game_profile: list,
    matchup_lean: dict,
    model_outcome: dict,
    header: dict,
) -> dict:
    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    predicted_team = None
    result = str((model_outcome or {}).get("result") or "").lower()

    if model_outcome:
        raw_predicted_team = model_outcome.get("predicted_team")

        if raw_predicted_team and raw_predicted_team not in {"None", "No Pick"}:
            predicted_team = raw_predicted_team

    if not predicted_team and matchup_lean:
        raw_target = str(matchup_lean.get("target_team", "")).replace(" edge", "").strip()

        if raw_target and raw_target not in {"None", "No strong directional edge", "No Pick"}:
            predicted_team = raw_target

    if predicted_team == away:
        predicted_side = "away"
        opponent_side = "home"
    elif predicted_team == home:
        predicted_side = "home"
        opponent_side = "away"
    else:
        predicted_side = None
        opponent_side = None

    signals = []
    aligned_count = 0
    total_count = 0

    for row in game_profile or []:
        category = row.get("category")

        favored_side = row.get("tilt_team")

        # Legacy fallback only
        if favored_side not in {"away", "home"}:
            tilt = row.get("tilt") or ""
            if away in tilt:
                favored_side = "away"
            elif home in tilt:
                favored_side = "home"
            else:
                favored_side = None

        if result == "no pick":
            aligns = "not_applicable"
        elif not predicted_side or not favored_side:
            aligns = "neutral"
        elif favored_side == predicted_side:
            aligns = "yes"
            aligned_count += 1
            total_count += 1
        elif favored_side == opponent_side:
            aligns = "no"
            total_count += 1
        else:
            aligns = "neutral"

        favored_team = None
        if favored_side == "away":
            favored_team = away
        elif favored_side == "home":
            favored_team = home

        if favored_team:
            sentence = f"{category} favored {favored_team}"
        else:
            sentence = f"{category} was neutral"

        signals.append({
            "category": category,
            "aligns": aligns,
            "favored_side": favored_side,
            "sentence": sentence,
        })

    # 🔥 Updated messaging here
    if result == "no pick":
        summary_code = "not_applicable"
        summary_label = "No pick — signals were mixed and did not support a confident prediction"
    elif total_count == 0:
        summary_code = "unknown"
        summary_label = "Signal alignment not available"
    elif aligned_count == total_count:
        summary_code = "strong"
        summary_label = "All signals agreed"
    elif aligned_count == 0:
        summary_code = "weak"
        summary_label = "Signals disagreed"
    else:
        summary_code = "mixed"
        summary_label = "Mixed signals — not all signals agreed"

    return {
        "summary_code": summary_code,
        "summary_label": summary_label,
        "aligned_count": aligned_count,
        "total_count": total_count,
        "tooltip": (
            "Shows whether each Game Profile signal agreed with the model's predicted or leaned team."
        ),
        "signals": signals,
    }


def resolve_reasoning_team(matchup_lean: dict, model_outcome: dict, leader_team=None):
    """
    Resolve the team that the reasoning text should discuss.

    Preference order:
    1. model_outcome.predicted_team
    2. matchup_lean.target_team
    3. matchup_advantage leader_team fallback
    """

    predicted_team = None

    if model_outcome:
        raw_team = model_outcome.get("predicted_team")

        if raw_team and raw_team not in {"None", "No Pick"}:
            predicted_team = raw_team

    if not predicted_team and matchup_lean:
        raw_target = str(matchup_lean.get("target_team", "")).replace(" edge", "").strip()

        if raw_target and raw_target not in {
            "None",
            "No strong directional edge",
            "No Pick",
        }:
            predicted_team = raw_target

    return predicted_team or leader_team

def build_reasoning(
    team_comparison: list,
    matchup_advantage: dict,
    matchup_lean: dict,
    model_outcome: dict,
    edge: dict,
    signal_alignment: dict,
    header: dict,
) -> dict:
    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]
    game_id = header.get("game_id")

    leader = matchup_advantage.get("leader")

    if leader == "away":
        leader_team = away
    elif leader == "home":
        leader_team = home
    else:
        leader_team = None

    drivers = []

    for row in team_comparison or []:
        better = row.get("better")

        if better not in {"away", "home"}:
            continue

        team = away if better == "away" else home
        label = row.get("label")

        drivers.append({
            "team": better,
            "category": label,
            "label": label,
            "sentence": f"{team} held the edge in {str(label).lower()}",
            "gap": f"{row.get('away')} vs {row.get('home')}",
        })

    result = str((model_outcome or {}).get("result") or "").lower()

    reasoning_team = resolve_reasoning_team(
        matchup_lean=matchup_lean,
        model_outcome=model_outcome,
        leader_team=leader_team,
    )

    profile_type = (matchup_lean or {}).get("profile_type")
    core_area_context = (matchup_lean or {}).get("core_area_context") or {}

    if not profile_type:
        profile_type = core_area_context.get("profile_type")

    # ---------------------------------
    # Headline aligned to matchup_lean
    # ---------------------------------
    if result == "no pick" or not reasoning_team:
        headline = "No clear matchup profile edge"

    elif profile_type == "coin_flip_profile":
        headline = f"{reasoning_team} had a slight signal lean in a balanced matchup"

    elif profile_type == "split_profile":
        headline = f"{reasoning_team} had a slight signal lean in a mixed matchup profile"

    elif profile_type == "conflicting_profile":
        headline = f"{reasoning_team} had signal support, but the broader profile was conflicted"

    elif profile_type == "confirmed_edge":
        headline = f"{reasoning_team} held the broader matchup edge"

    elif profile_type == "no_clear_edge":
        headline = "No clear matchup profile edge"

    elif reasoning_team:
        headline = f"{reasoning_team} showed a directional signal lean"

    else:
        headline = "No clear matchup profile edge"

    summary = build_reasoning_summary(
        leader_team=leader_team,
        matchup_lean=matchup_lean,
        model_outcome=model_outcome,
        edge=edge,
        signal_alignment=signal_alignment,
        game_id=game_id,
    )

    return {
        "headline": headline,
        "summary": summary,
        "has_content": len(drivers) > 0 or bool(summary),
        "drivers": drivers[:3],
    }


def build_reasoning_summary(
    leader_team,
    matchup_lean,
    model_outcome,
    edge,
    signal_alignment,
    game_id=None,
):
    """
    Builds a short human-readable reasoning summary.

    This is now aligned with matchup_lean.profile_type so Model Trust
    does not overstate the edge when Core Areas are mixed or coin-flippy.
    """

    edge_strength = (edge or {}).get("strength") or "unclear"
    alignment_code = (signal_alignment or {}).get("summary_code") or "unknown"

    result = None

    if model_outcome:
        result = str(model_outcome.get("result") or "").lower()

    team = resolve_reasoning_team(
        matchup_lean=matchup_lean,
        model_outcome=model_outcome,
        leader_team=leader_team,
    )

    profile_type = (matchup_lean or {}).get("profile_type")
    lean_summary = (matchup_lean or {}).get("lean_summary")
    focus_summary = (matchup_lean or {}).get("focus_summary")
    confidence_context = (matchup_lean or {}).get("confidence_context")
    core_area_context = (matchup_lean or {}).get("core_area_context") or {}

    if not profile_type:
        profile_type = core_area_context.get("profile_type")

    core_split = core_area_context.get("core_area_split")
    core_gap = core_area_context.get("core_gap")

    # ---------------------------------
    # No pick / no edge
    # ---------------------------------
    if result == "no pick" or profile_type == "no_clear_edge":
        templates = [
            "The model avoided a strong call because the signals were too balanced.",
            "This game did not create enough separation for a confident directional edge.",
            "The matchup profile stayed mixed enough that the model did not force a lean.",
        ]

    # ---------------------------------
    # Coin flip profile
    # ---------------------------------
    elif profile_type == "coin_flip_profile":
        if result == "incorrect":
            templates = [
                "{team} had the stronger signal score, but Core Areas were nearly even. The miss is a calibration note, not a broken read.",
                "The model leaned {team}, but the broader profile was close enough that the final result exposed a useful calibration miss.",
                "{team} showed a slight signal lean, but the Core Area gap was tiny, so this result should be treated as a balanced-profile miss.",
            ]
        elif result == "correct":
            templates = [
                "{team} had only a slight signal lean in a balanced matchup, and the final result still backed it up.",
                "The model leaned {team} despite a near-even Core Area profile, and that small edge held through the outcome.",
                "{team}'s signal lean was modest, but it was enough to align with the final result.",
            ]
        else:
            templates = [
                "{team} has a slight signal lean, but Core Areas are nearly even overall.",
                "The matchup is close overall, with only a slight lean toward {team}.",
                "{team} leads the signal read, but the broader Core Area profile is nearly balanced.",
            ]

    # ---------------------------------
    # Split profile
    # ---------------------------------
    elif profile_type == "split_profile":
        if result == "incorrect":
            templates = [
                "{team} had signal support, but Core Areas were split. The miss points to a mixed-profile calibration issue.",
                "The model leaned {team}, but the broader matchup was divided across Core Areas, making this a useful learning spot.",
                "{team}'s signals were stronger, but the split Core Area profile limited confidence and the result went the other way.",
            ]
        elif result == "correct":
            templates = [
                "{team} had signal support in a split Core Area profile, and the final result backed the lean.",
                "The matchup was mixed overall, but {team}'s signal edge still aligned with the outcome.",
                "{team}'s lean came from stronger signals, even though Core Areas were split.",
            ]
        else:
            templates = [
                "{team} has signal support, but the broader Core Area profile is split.",
                "The matchup profile is mixed, with a slight signal lean toward {team}.",
                "{team} leads the signal read, but Core Areas are divided enough to limit confidence.",
            ]

    # ---------------------------------
    # Conflicting profile
    # ---------------------------------
    elif profile_type == "conflicting_profile":
        if result == "incorrect":
            templates = [
                "{team} had signal support, but Core Areas did not confirm the edge. The miss reinforces that conflict warning.",
                "The model leaned {team}, but broader Core Area context pointed against a clean edge.",
                "{team}'s signal edge was not fully supported by the wider profile, and the final result exposed that risk.",
            ]
        elif result == "correct":
            templates = [
                "{team}'s signal support overcame a conflicting Core Area profile, but this still deserves cautious grading.",
                "The model leaned {team} despite broader profile conflict, and the result backed the signal read.",
                "{team}'s signals proved more useful than the broader Core Area warning in this game.",
            ]
        else:
            templates = [
                "{team} has signal support, but Core Areas do not fully confirm the edge.",
                "The model leans {team}, though the broader profile is conflicting.",
                "{team} leads the signal read, but Core Area context argues for caution.",
            ]

    # ---------------------------------
    # Confirmed edge
    # ---------------------------------
    elif profile_type == "confirmed_edge":
        if result == "correct":
            templates = [
                "{team} had support from both signal scoring and Core Area context, and the final result backed that up.",
                "The model leaned {team}, and the broader matchup profile confirmed the edge.",
                "{team}'s advantage was supported across the profile and aligned with the outcome.",
            ]
        elif result == "incorrect":
            templates = [
                "{team} had support from the broader matchup profile, but the final result went the other way.",
                "The model saw a confirmed edge for {team}, making this a stronger calibration miss.",
                "{team}'s profile looked cleaner pregame, but the outcome exposed a miss worth reviewing.",
            ]
        else:
            templates = [
                "{team} is supported by both signal scoring and Core Area context.",
                "{team} holds the broader matchup edge based on the current profile.",
                "The matchup profile gives {team} a {edge_strength} edge with Core Area support.",
            ]

    # ---------------------------------
    # Fallback
    # ---------------------------------
    else:
        if result == "incorrect":
            templates = [
                "The model saw an edge for {team}, though the outcome exposed a useful calibration miss.",
                "{team} showed some matchup support, but the final result went the other way.",
                "The model leaned {team}, but this game became a learning spot for calibration.",
            ]
        elif result == "correct":
            templates = [
                "The model leaned toward {team}, and the final result backed that up.",
                "{team}'s matchup support aligned with the outcome.",
                "The model's lean toward {team} held through the final result.",
            ]
        elif team:
            templates = [
                "{team} carries a {edge_strength} matchup lean based on the current profile.",
                "The matchup profile leans toward {team}, though confidence depends on signal agreement.",
                "{team} shows the cleaner setup, though the edge grades as {edge_strength}.",
            ]
        else:
            templates = [
                "The matchup profile does not show a clear enough edge yet.",
                "The available signals are too balanced to create a strong matchup story.",
                "No clean matchup advantage stands out from the current profile.",
            ]

    # Stable randomness: same game/profile/result = same wording
    seed = str(
        game_id
        or f"{profile_type}-{result}-{team}-{edge_strength}-{alignment_code}-{core_split}-{core_gap}"
    )
    rng = random.Random(seed)
    template = rng.choice(templates)

    return template.format(
        team=team or "the selected team",
        edge_strength=edge_strength,
        core_split=core_split or "unknown",
        core_gap=core_gap if core_gap is not None else "unknown",
        lean_summary=lean_summary or "",
        focus_summary=focus_summary or "",
        confidence_context=confidence_context or "",
    )