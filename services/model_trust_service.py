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
    away_count = 0
    home_count = 0

    for row in team_comparison or []:
        better = row.get("better")

        if better == "away":
            away_count += 1
        elif better == "home":
            home_count += 1

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
        "leader": leader,
        "tooltip": "Shows how many matchup comparison factors favored each team.",
    }


def build_edge(matchup_advantage: dict) -> dict:
    away = matchup_advantage.get("away", 0)
    home = matchup_advantage.get("home", 0)

    diff = abs(away - home)
    total = away + home

    edge_score = round(diff / total, 2) if total else 0

    if diff >= 3:
        strength = "strong"
    elif diff >= 2:
        strength = "moderate"
    elif diff >= 1:
        strength = "low"
    else:
        strength = "none"

    return {
        "strength": strength,
        "score": edge_score,
        "tooltip": "Overall strength of the matchup advantage.",
        "has_content": total > 0,
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
        "tooltip": "Whether each key signal favored the predicted team or opponent.",
        "signals": signals,
    }


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

    if result == "no pick":
        headline = "No clear matchup profile edge"
    elif leader_team:
        headline = f"{leader_team} holds the cleaner matchup profile"
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
    """

    edge_strength = (edge or {}).get("strength") or "unclear"
    alignment_code = (signal_alignment or {}).get("summary_code") or "unknown"

    predicted_team = None
    result = None

    if model_outcome:
        raw_team = model_outcome.get("predicted_team")
        result = str(model_outcome.get("result") or "").lower()

        if raw_team and raw_team not in {"None", "No Pick"}:
            predicted_team = raw_team

    if not predicted_team and matchup_lean:
        raw_target = str(matchup_lean.get("target_team", "")).replace(" edge", "").strip()

        if raw_target and raw_target not in {"None", "No strong directional edge"}:
            predicted_team = raw_target

    team = predicted_team or leader_team

    # Determine summary type
    if result == "correct":
        summary_type = "correct"
    elif result == "incorrect":
        summary_type = "incorrect"
    elif result == "no pick":
        summary_type = "no_pick"
    elif team:
        summary_type = "pregame_edge"
    else:
        summary_type = "neutral"

    templates = {
        "correct": [
            "{team} had the cleaner pregame profile, and the final result backed that up.",
            "The model leaned toward {team}, and that matchup edge held through the outcome.",
            "{team}'s advantage showed up clearly enough for the model to align with the result.",
        ],
        "incorrect": [
            "{team} showed the better pregame profile, but the final result went the other way.",
            "The model saw an edge for {team}, though the outcome exposed a useful calibration miss.",
            "{team} had the cleaner matchup signals, but this game became a learning spot for the model.",
        ],
        "no_pick": [
            "The matchup profile did not create a clean enough edge to force a lean.",
            "The model avoided a strong call because the signals were too balanced.",
            "This game stayed too mixed pregame for a confident directional edge.",
        ],
        "pregame_edge": [
            "{team} carries a {edge_strength} matchup edge based on the current profile.",
            "The matchup profile leans toward {team}, with a {edge_strength} overall edge.",
            "{team} owns the cleaner pregame setup, though the edge grades as {edge_strength}.",
        ],
        "neutral": [
            "The matchup profile does not show a clear enough edge yet.",
            "The available signals are too balanced to create a strong matchup story.",
            "No clean matchup advantage stands out from the current profile.",
        ],
    }

    # Stable randomness (same game = same wording)
    seed = str(game_id or f"{summary_type}-{team}-{edge_strength}-{alignment_code}")
    rng = random.Random(seed)
    template = rng.choice(templates[summary_type])

    return template.format(
        team=team or "the selected team",
        edge_strength=edge_strength,
    )