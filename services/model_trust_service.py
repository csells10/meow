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
    if not model_outcome:
        return "Outcome not available yet"

    result = str(model_outcome.get("result") or "").lower()

    if result == "correct":
        return "Model aligned with outcome"

    if result == "incorrect":
        return "Model miss — review signals"

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

    if model_outcome:
        predicted_team = model_outcome.get("predicted_team")

    if not predicted_team and matchup_lean:
        predicted_team = str(matchup_lean.get("target_team", "")).replace(" edge", "")

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
        tilt = row.get("tilt") or ""

        favored_side = None

        if away in tilt:
            favored_side = "away"
        elif home in tilt:
            favored_side = "home"

        if not predicted_side or not favored_side:
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

        if favored_side == "away":
            favored_team = away
        elif favored_side == "home":
            favored_team = home
        else:
            favored_team = None

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

    if total_count == 0:
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
    header: dict,
) -> dict:
    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

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

    if leader_team:
        headline = f"{leader_team} holds the cleaner matchup profile"
    else:
        headline = "No clear matchup profile edge"

    return {
        "headline": headline,
        "has_content": len(drivers) > 0,
        "drivers": drivers[:3],
    }