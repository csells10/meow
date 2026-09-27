"""Independent synthetic verification for the frozen Matchup Lens endpoint.

All game, team, metric, ranking, and window values in this file are synthetic.
No test invokes BigQuery, a scheduled job, a learning flow, or a write path.
"""

from __future__ import annotations

import ast
import copy
import inspect
import json
import math
from unittest.mock import Mock, patch

import pytest
from flask import Flask
from google.api_core.exceptions import DeadlineExceeded


# Importing the production query module constructs a BigQuery client.  Replacing
# only that constructor keeps collection local and read-only; every endpoint
# dependency is replaced with the synthetic functions below before a test runs.
with patch("google.cloud.bigquery.Client"):
    from auth import firebase_auth
    from queries import game_queries
    from routes import game_routes as game_routes_module
    from routes import games as games_routes_module
    from services import matchup_lens_service as lens_service


SYNTHETIC_GAME_ID = "20990101_AAA@BBB"
SYNTHETIC_AS_OF_DATE = "2098-12-29"
SYNTHETIC_SOURCE_DATE = "2098-12-28"
SYNTHETIC_WINDOW = "regular_season_to_date"
TOP_LEVEL_ORDER = [
    "schema_version",
    "available",
    "reason",
    "game",
    "display",
    "basis",
    "metric_catalog",
    "teams",
    "coverage",
    "league_context",
    "method",
]
GAME_ORDER = [
    "game_id",
    "game_date",
    "game_time",
    "game_status",
    "season",
    "game_week",
    "season_type",
    "away_team",
    "home_team",
]
TEAM_ORDER = [
    "team_id",
    "team_abv",
    "games_in_window",
    "latest_included_game_id",
    "latest_source_date",
    "data_lag_days",
    "metrics",
]
METRIC_ORDER = [
    "metric",
    "value",
    "label",
    "definition",
    "category",
    "core_area",
    "comparison_direction",
    "higher_is_better",
    "raw_or_derived",
    "aggregation_method",
    "format",
    "decimals",
    "notes",
    "ranking_usage",
    "signal_strength",
    "edge_language_allowed",
    "include_in_core_area_advantage",
    "confidence_eligible",
    "data_quality_status",
    "lens_tags",
    "league_rank",
    "league_percentile",
    "tier",
    "tier_label",
    "teams_ranked",
    "ranking_kind",
    "rank_direction",
    "rank_interpretation",
    "rank_tie_method",
    "source_data_date",
    "data_lag_days",
]
LENS_ORDER = [
    "explosiveness",
    "drive-control",
    "scoring-finish",
    "defensive-resistance",
    "disruption-protection",
    "turnover-balance",
]
NAMED_CONSUMERS = [
    "sacks_taken",
    "sack_yards_lost",
    "sacks",
    "interceptions_thrown",
    "fumbles_lost",
    "turnovers",
    "defensive_interceptions",
    "fumbles_recovered",
    "points_per_play",
    "td_rate",
    "red_zone_efficiency",
    "points_allowed_per_play",
]


def _synthetic_definitions() -> dict[str, tuple[str, list[str]]]:
    """Return a catalog covering every frozen consumer and readiness row."""
    return {
        "explosive_metric": ("strong", ["explosiveness"]),
        "third_down_pct": ("strong", ["third-down"]),
        "fourth_down_pct": ("supporting", ["fourth-down"]),
        "scoring_metric": ("strong", ["scoring"]),
        "defense_metric": ("strong", ["defense"]),
        "sacks": ("strong", ["disruption"]),
        "turnovers": ("strong", ["turnovers"]),
        "sacks_taken": ("supporting", ["context"]),
        "sack_yards_lost": ("supporting", ["context"]),
        "interceptions_thrown": ("supporting", ["context"]),
        "fumbles_lost": ("supporting", ["context"]),
        "defensive_interceptions": ("supporting", ["context"]),
        "fumbles_recovered": ("supporting", ["context"]),
        "points_per_play": ("supporting", ["context"]),
        "td_rate": ("supporting", ["context"]),
        "red_zone_efficiency": ("supporting", ["context"]),
        "points_allowed_per_play": ("supporting", ["context"]),
        # Known transport evidence that must not enter lens readiness/scoring.
        "context_drive_metric": ("context", ["third-down"]),
        # Deliberately outside the two-team payload and old sample catalogs.
        "runtime_metric_beyond_old_catalog": ("supporting", ["context"]),
    }


def _metric_payload(
    metric: str,
    signal_strength: str,
    tags: list[str],
    percentile: float | None,
) -> dict:
    return {
        "metric": metric,
        "value": 1.25,
        "label": metric.replace("_", " ").title(),
        "definition": None,
        "category": None,
        "core_area": None,
        "comparison_direction": None,
        "higher_is_better": True,
        "raw_or_derived": "synthetic",
        "aggregation_method": None,
        # Production uses provenance names here; the amended endpoint ignores them.
        "numerator": "synthetic_numerator_source",
        "denominator": "synthetic_denominator_source",
        "format": "number",
        "decimals": 2,
        "notes": None,
        "ranking_usage": "synthetic",
        "signal_strength": signal_strength,
        "edge_language_allowed": True,
        "include_in_core_area_advantage": True,
        "confidence_eligible": True,
        "data_quality_status": "synthetic",
        "lens_tags": tags,
        "league_rank": 1,
        "league_percentile": percentile,
        "tier": None,
        "tier_label": None,
        "teams_ranked": 32,
        "ranking_kind": "synthetic",
        "rank_direction": "descending",
        "rank_interpretation": "synthetic",
        "rank_tie_method": "synthetic",
        "source_data_date": SYNTHETIC_SOURCE_DATE,
        "data_lag_days": 1,
    }


def _synthetic_state() -> dict:
    """Build one complete, schema-valid, entirely synthetic source state."""
    definitions = _synthetic_definitions()
    header = {
        "game_id": SYNTHETIC_GAME_ID,
        "game_date": "2099-01-01",
        "game_time": "1:00p",
        "game_status": "Scheduled",
        "season": "2098",
        "game_week": "Week 18",
        "season_type": "Regular Season",
        "away_team": {"id": 101, "abbreviation": "AAA", "logo": None},
        "home_team": {"id": 202, "abbreviation": "BBB", "logo": None},
    }
    raw_rows: list[dict] = []
    helpers = {"away": {}, "home": {}}
    windows: list[dict] = []
    runtime_only = "runtime_metric_beyond_old_catalog"

    for side, team_id, team_abv, percentile, latest_game in (
        ("away", "101", "AAA", 60.0, "20981228_AAA@CCC"),
        ("home", "202", "BBB", 55.0, "20981228_DDD@BBB"),
    ):
        for metric, (signal, tags) in definitions.items():
            if metric == runtime_only:
                continue
            payload = _metric_payload(metric, signal, tags, percentile)
            helpers[side][metric] = payload
            raw_rows.append(
                {
                    "season": "2098",
                    "as_of_date": SYNTHETIC_AS_OF_DATE,
                    "source_data_date": SYNTHETIC_SOURCE_DATE,
                    "data_lag_days": 1,
                    "window_type": SYNTHETIC_WINDOW,
                    "team_id": team_id,
                    "team_abv": team_abv,
                    "metric": metric,
                    "label": payload["label"],
                    "signal_strength": signal,
                    "lens_tags": list(tags),
                }
            )
            windows.append(
                {
                    "team_id": team_id,
                    "metric": metric,
                    "data_date": SYNTHETIC_SOURCE_DATE,
                    "games_in_window": 1,
                    "latest_included_game_id": latest_game,
                }
            )

    signal, tags = definitions[runtime_only]
    raw_rows.append(
        {
            "season": "2098",
            "as_of_date": SYNTHETIC_AS_OF_DATE,
            "source_data_date": SYNTHETIC_SOURCE_DATE,
            "data_lag_days": 1,
            "window_type": SYNTHETIC_WINDOW,
            "team_id": "303",
            "team_abv": "CCC",
            "metric": runtime_only,
            "label": runtime_only.replace("_", " ").title(),
            "signal_strength": signal,
            "lens_tags": list(tags),
        }
    )
    return {
        "header": header,
        "raw_rows": raw_rows,
        "helpers": helpers,
        "windows": windows,
        "meta": {
            "available": True,
            "window_type": SYNTHETIC_WINDOW,
            "as_of_date": SYNTHETIC_AS_OF_DATE,
        },
    }


def _install_state(monkeypatch: pytest.MonkeyPatch, state: dict) -> dict[str, Mock]:
    mocks = {
        "header": Mock(return_value=state["header"]),
        "window": Mock(return_value=SYNTHETIC_WINDOW),
        "boundary": Mock(return_value=state["raw_rows"]),
        "helper": Mock(
            return_value=(
                state["helpers"]["away"],
                state["helpers"]["home"],
                state["meta"],
            )
        ),
        "aligned": Mock(return_value=state["windows"]),
    }
    monkeypatch.setattr(lens_service, "get_game_header", mocks["header"])
    monkeypatch.setattr(lens_service, "select_window_type", mocks["window"])
    monkeypatch.setattr(
        lens_service,
        "get_matchup_lens_ranking_boundary",
        mocks["boundary"],
    )
    monkeypatch.setattr(
        lens_service,
        "get_team_rankings_for_game",
        mocks["helper"],
    )
    monkeypatch.setattr(
        lens_service,
        "get_matchup_lens_source_aligned_windows",
        mocks["aligned"],
    )
    return mocks


def _reason(payload: dict) -> str | None:
    return None if payload["reason"] is None else payload["reason"]["code"]


def _drop_team_metric(state: dict, side: str, metric: str) -> None:
    team_id = "101" if side == "away" else "202"
    state["helpers"][side].pop(metric)
    state["raw_rows"] = [
        row
        for row in state["raw_rows"]
        if not (str(row["team_id"]) == team_id and row["metric"] == metric)
    ]
    state["windows"] = [
        row
        for row in state["windows"]
        if not (str(row["team_id"]) == team_id and row["metric"] == metric)
    ]


@pytest.fixture
def flask_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(game_routes_module.game_routes)
    app.register_blueprint(games_routes_module.games_bp)
    return app


def _allow_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(firebase_auth, "init_firebase_admin", lambda: None)
    monkeypatch.setattr(
        firebase_auth.firebase_admin_auth,
        "verify_id_token",
        lambda token: {"email": "allowed@example.test"},
    )
    monkeypatch.setattr(
        firebase_auth,
        "is_user_allowed",
        lambda email: (True, {"active": True, "role": "admin"}),
    )


def test_nested_route_dispatch_and_existing_routes_remain_separate(
    monkeypatch: pytest.MonkeyPatch,
    flask_app: Flask,
) -> None:
    _allow_auth(monkeypatch)
    lens_builder = Mock(return_value=('{"route":"lens"}\n', 200))
    game_builder = Mock(return_value={"route": "game"})
    games_builder = Mock(return_value=[{"game_id": SYNTHETIC_GAME_ID}])
    monkeypatch.setattr(
        game_routes_module,
        "serialize_matchup_lens_context",
        lens_builder,
    )
    monkeypatch.setattr(game_routes_module, "get_game_details", game_builder)
    monkeypatch.setattr(games_routes_module, "fetch_games_by_date", games_builder)
    monkeypatch.setattr(games_routes_module, "log_event", lambda *a, **k: None)
    headers = {"Authorization": "Bearer synthetic-token"}

    with flask_app.test_client() as client:
        lens_response = client.get(
            f"/game/{SYNTHETIC_GAME_ID}/lens-context",
            headers=headers,
        )
        game_response = client.get(f"/game/{SYNTHETIC_GAME_ID}", headers=headers)
        games_response = client.get(
            "/games?date=2099-01-01",
            headers=headers,
        )

    assert lens_response.status_code == 200
    assert lens_response.data == b'{"route":"lens"}\n'
    assert game_response.get_json() == {"route": "game"}
    assert games_response.get_json() == {
        "date": "2099-01-01",
        "games": [{"game_id": SYNTHETIC_GAME_ID}],
    }
    lens_builder.assert_called_once_with(SYNTHETIC_GAME_ID)
    game_builder.assert_called_once_with(SYNTHETIC_GAME_ID)
    games_builder.assert_called_once()


@pytest.mark.parametrize(
    ("case", "expected_status", "expected_body"),
    [
        (
            "missing",
            401,
            b'{"error":"unauthorized","message":"Missing Authorization Bearer token"}\n',
        ),
        (
            "invalid",
            401,
            b'{"error":"unauthorized","message":"Invalid or expired Firebase token"}\n',
        ),
        (
            "no_email",
            403,
            b'{"error":"forbidden","message":"Firebase token does not include an email address"}\n',
        ),
        (
            "not_allowed",
            403,
            b'{"error":"forbidden","message":"This Google account is not allowed to access GameLens"}\n',
        ),
    ],
)
def test_auth_failure_bodies_are_unchanged_and_builder_is_not_called(
    monkeypatch: pytest.MonkeyPatch,
    flask_app: Flask,
    case: str,
    expected_status: int,
    expected_body: bytes,
) -> None:
    monkeypatch.setattr(firebase_auth, "init_firebase_admin", lambda: None)
    builder = Mock(return_value=('{"unexpected":true}\n', 200))
    monkeypatch.setattr(
        game_routes_module,
        "serialize_matchup_lens_context",
        builder,
    )
    headers = {}
    if case != "missing":
        headers["Authorization"] = "Bearer synthetic-token"
    if case == "invalid":
        monkeypatch.setattr(
            firebase_auth.firebase_admin_auth,
            "verify_id_token",
            Mock(side_effect=ValueError("synthetic invalid token")),
        )
    else:
        token = {} if case == "no_email" else {"email": "blocked@example.test"}
        monkeypatch.setattr(
            firebase_auth.firebase_admin_auth,
            "verify_id_token",
            Mock(return_value=token),
        )
    monkeypatch.setattr(
        firebase_auth,
        "is_user_allowed",
        Mock(return_value=(False, None)),
    )

    with flask_app.test_client() as client:
        response = client.get(
            f"/game/{SYNTHETIC_GAME_ID}/lens-context",
            headers=headers,
        )

    assert response.status_code == expected_status
    assert response.data == expected_body
    builder.assert_not_called()


def test_success_envelope_order_bytes_dynamic_catalog_and_two_teams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    _install_state(monkeypatch, state)

    first, first_status = lens_service.serialize_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )
    second, second_status = lens_service.serialize_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )
    payload = json.loads(first)

    assert first_status == second_status == 200
    assert first == second
    assert first.endswith("\n")
    assert "\n" not in first[:-1]
    assert first == (
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    )
    assert "NaN" not in first and "Infinity" not in first
    assert list(payload) == TOP_LEVEL_ORDER
    assert list(payload["game"]) == GAME_ORDER
    assert payload["available"] is True
    assert list(payload["teams"]) == ["away", "home"]
    assert list(payload["teams"]["away"]) == TEAM_ORDER
    assert payload["teams"]["away"]["team_id"] == "101"
    assert payload["teams"]["home"]["team_id"] == "202"
    assert "runtime_metric_beyond_old_catalog" in payload["metric_catalog"]
    assert payload["coverage"]["catalog_metric_count"] == len(
        payload["metric_catalog"]
    )
    assert payload["coverage"]["catalog_metric_count"] not in {59, 63, 70, 73}
    assert list(payload["teams"]["away"]["metrics"]) == sorted(
        payload["teams"]["away"]["metrics"]
    )
    first_metric = next(iter(payload["teams"]["away"]["metrics"].values()))
    assert list(first_metric) == METRIC_ORDER
    assert "numerator" not in first_metric
    assert "denominator" not in first_metric
    assert "allow_nan=False" in inspect.getsource(
        lens_service.serialize_matchup_lens_context
    )


def test_canonical_header_teams_override_url_abbreviations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    mocks = _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        "20990101_ZZZ@YYY"
    )

    assert status == 200
    assert payload["game"]["game_id"] == SYNTHETIC_GAME_ID
    assert payload["game"]["away_team"]["team_abv"] == "AAA"
    assert payload["game"]["home_team"]["team_abv"] == "BBB"
    mocks["header"].assert_called_once_with("20990101_ZZZ@YYY")


@pytest.mark.parametrize(
    "game_id",
    [
        "",
        "not-a-game",
        "20990101_A@BBB",
        "20990101_AAAAA@BBB",
        "2099-01-01_AAA@BBB",
        "20990101_aaa@BBB",
        "2" * 33,
    ],
)
def test_structurally_invalid_ids_stop_before_reads(
    monkeypatch: pytest.MonkeyPatch,
    game_id: str,
) -> None:
    header = Mock(side_effect=AssertionError("header must not be called"))
    monkeypatch.setattr(lens_service, "get_game_header", header)

    payload, status = lens_service.build_matchup_lens_context(game_id)

    assert status == 400
    assert _reason(payload) == "INVALID_GAME_ID"
    assert payload["game"] is None
    header.assert_not_called()


def test_raw_duplicate_is_rejected_before_helper_dictionary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    duplicate = next(
        row
        for row in state["raw_rows"]
        if row["team_id"] == "101" and row["metric"] == "third_down_pct"
    )
    state["raw_rows"].append(copy.deepcopy(duplicate))
    mocks = _install_state(monkeypatch, state)
    mocks["helper"].side_effect = AssertionError("helper must not be trusted")

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )

    assert status == 409
    assert _reason(payload) == "DUPLICATE_RANKING_ROWS"
    mocks["helper"].assert_not_called()


def test_missing_exact_source_alignment_is_expected_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    state["windows"].pop()
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )

    assert status == 200
    assert _reason(payload) == "SOURCE_ALIGNMENT_UNAVAILABLE"
    assert payload["game"]["game_id"] == SYNTHETIC_GAME_ID
    assert payload["basis"] is None
    assert payload["metric_catalog"] == []
    assert payload["teams"] == {"away": None, "home": None}
    assert payload["coverage"] is None


@pytest.mark.parametrize("mode", ["duplicate", "count_disagreement"])
def test_source_alignment_duplicates_or_disagreement_are_conflicts(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    state = _synthetic_state()
    away_rows = [
        row for row in state["windows"] if str(row["team_id"]) == "101"
    ]
    if mode == "duplicate":
        state["windows"].append(copy.deepcopy(away_rows[0]))
    else:
        away_rows[0]["games_in_window"] = 2
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )

    assert status == 409
    assert _reason(payload) == "SOURCE_ALIGNMENT_CONFLICT"


def test_newer_pregame_window_row_is_never_attached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    exact = next(
        row
        for row in state["windows"]
        if str(row["team_id"]) == "101" and row["metric"] == "third_down_pct"
    )
    newer = copy.deepcopy(exact)
    newer.update(
        {
            "data_date": SYNTHETIC_AS_OF_DATE,
            "games_in_window": 99,
            "latest_included_game_id": "20981229_NEWER@AAA",
        }
    )
    state["windows"].append(newer)
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )

    assert status == 200
    assert payload["teams"]["away"]["games_in_window"] == 1
    assert (
        payload["teams"]["away"]["latest_included_game_id"]
        == "20981228_AAA@CCC"
    )
    assert payload["teams"]["away"]["latest_source_date"] == SYNTHETIC_SOURCE_DATE


def test_zero_games_requires_null_latest_included_game_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Frozen invariant: zero games cannot retain a latest included game."""
    state = _synthetic_state()
    for row in state["windows"]:
        if str(row["team_id"]) == "101":
            row["games_in_window"] = 0
            # Deliberately retain the synthetic game ID to reproduce the defect.
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )

    assert status == 409
    assert _reason(payload) == "SOURCE_ALIGNMENT_CONFLICT"


def test_lag_is_derived_from_dates_instead_of_trusting_helper_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Frozen freshness is as_of_date minus source_data_date: exactly one day."""
    state = _synthetic_state()
    for payload in state["helpers"]["away"].values():
        payload["data_lag_days"] = 7
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )

    assert status == 200
    assert payload["teams"]["away"]["data_lag_days"] == 1
    assert payload["basis"]["max_data_lag_days"] == 1


def test_both_canonical_teams_are_required_for_top_level_availability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    state["raw_rows"] = [
        row for row in state["raw_rows"] if str(row["team_id"]) != "202"
    ]
    mocks = _install_state(monkeypatch, state)
    mocks["helper"].side_effect = AssertionError("helper must not be called")

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )

    assert status == 200
    assert _reason(payload) == "MISSING_TEAM_EVIDENCE"
    mocks["helper"].assert_not_called()


def test_partial_lens_remains_available_and_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    _drop_team_metric(state, "away", "fourth_down_pct")
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )
    readiness = {
        row["lens_key"]: row for row in payload["coverage"]["lens_readiness"]
    }

    assert status == 200
    assert payload["available"] is True
    assert payload["reason"] is None
    drive = readiness["drive-control"]
    assert drive["away"]["status"] == "partial"
    assert drive["home"]["status"] == "complete"
    assert drive["comparison_status"] == "partial"
    assert drive["away"]["missing_metrics"] == ["fourth_down_pct"]
    assert all(
        row["comparison_status"] == "complete"
        for key, row in readiness.items()
        if key != "drive-control"
    )


def test_null_percentile_is_valid_and_changes_only_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    state["helpers"]["away"]["third_down_pct"]["league_percentile"] = None
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )
    drive = next(
        row
        for row in payload["coverage"]["lens_readiness"]
        if row["lens_key"] == "drive-control"
    )

    assert status == 200
    assert payload["available"] is True
    assert (
        payload["teams"]["away"]["metrics"]["third_down_pct"][
            "league_percentile"
        ]
        is None
    )
    assert drive["away"]["status"] == "partial"
    assert "third_down_pct" in drive["away"]["missing_metrics"]


def test_all_six_rows_named_coverage_warning_order_and_rank_suppression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )
    coverage = payload["coverage"]
    warnings = coverage["warnings"]
    sort_keys = [
        (
            item["code"],
            item["lens_key"] or "",
            item["team_side"] or "",
            ",".join(item["metrics"]),
        )
        for item in warnings
    ]

    assert status == 200
    assert [row["lens_key"] for row in coverage["lens_readiness"]] == LENS_ORDER
    assert coverage["named_metric_coverage"] == {
        "consumers": NAMED_CONSUMERS,
        "available_away_metrics": NAMED_CONSUMERS,
        "available_home_metrics": NAMED_CONSUMERS,
        "missing_away_metrics": [],
        "missing_home_metrics": [],
    }
    assert sort_keys == sorted(sort_keys)
    assert payload["league_context"]["mode"] == "suppressed"
    assert any(
        item["code"] == "LEAGUE_RANK_OUTPUT_SUPPRESSED" for item in warnings
    )


def test_context_signal_is_transported_counted_and_excluded_from_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _synthetic_state()
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )
    coverage = payload["coverage"]
    drive = next(
        row
        for row in coverage["lens_readiness"]
        if row["lens_key"] == "drive-control"
    )

    assert status == 200
    assert payload["available"] is True
    assert "context_drive_metric" in payload["metric_catalog"]
    assert coverage["away_metric_count"] == len(state["helpers"]["away"])
    assert coverage["home_metric_count"] == len(state["helpers"]["home"])
    for side in ("away", "home"):
        transported = payload["teams"][side]["metrics"][
            "context_drive_metric"
        ]
        assert transported["signal_strength"] == "context"
        assert drive[side]["catalog_eligible_metric_count"] == 2
        assert drive[side]["eligible_numeric_metric_count"] == 2
        assert "context_drive_metric" not in drive[side]["missing_metrics"]



@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("nan", "INVALID_METRIC_EVIDENCE"),
        ("positive_infinity", "INVALID_METRIC_EVIDENCE"),
        ("negative_percentile", "INVALID_METRIC_EVIDENCE"),
        ("high_percentile", "INVALID_METRIC_EVIDENCE"),
        ("unknown_signal", "INVALID_METRIC_EVIDENCE"),
        ("null_signal", "INVALID_METRIC_EVIDENCE"),
        ("cased_signal", "INVALID_METRIC_EVIDENCE"),
        ("unsafe_source_date", "UNSAFE_EVIDENCE_DATES"),
        ("unsafe_as_of_date", "UNSAFE_EVIDENCE_DATES"),
        ("window_mismatch", "WINDOW_MISMATCH"),
    ],
)
def test_invalid_metric_date_and_window_states_fail_safely(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    expected_reason: str,
) -> None:
    state = _synthetic_state()
    target = state["helpers"]["away"]["third_down_pct"]
    if mutation == "nan":
        target["league_percentile"] = math.nan
    elif mutation == "positive_infinity":
        target["league_percentile"] = math.inf
    elif mutation == "negative_percentile":
        target["league_percentile"] = -0.01
    elif mutation == "high_percentile":
        target["league_percentile"] = 100.01
    elif mutation == "unknown_signal":
        target["signal_strength"] = "weak"
    elif mutation == "null_signal":
        target["signal_strength"] = None
    elif mutation == "cased_signal":
        target["signal_strength"] = "Context"
    elif mutation == "unsafe_source_date":
        target["source_data_date"] = "2099-01-01"
    elif mutation == "unsafe_as_of_date":
        for row in state["raw_rows"]:
            row["as_of_date"] = "2099-01-01"
    elif mutation == "window_mismatch":
        state["raw_rows"][0]["window_type"] = "preseason_to_date"
    _install_state(monkeypatch, state)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )

    assert status == 409
    assert _reason(payload) == expected_reason
    assert payload["display"] is None
    assert payload["basis"] is None
    assert payload["metric_catalog"] == []
    assert payload["coverage"] is None


def test_unknown_game_maps_to_safe_404(monkeypatch: pytest.MonkeyPatch) -> None:
    header = Mock(return_value={})
    monkeypatch.setattr(lens_service, "get_game_header", header)

    payload, status = lens_service.build_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )

    assert status == 404
    assert _reason(payload) == "GAME_NOT_FOUND"
    assert payload["game"] is None


def test_deadline_maps_to_504_without_internal_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        lens_service,
        "get_game_header",
        Mock(side_effect=DeadlineExceeded("synthetic upstream detail")),
    )

    serialized, status = lens_service.serialize_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )
    payload = json.loads(serialized)

    assert status == 504
    assert _reason(payload) == "UPSTREAM_TIMEOUT"
    assert payload["game"] is None
    assert "synthetic upstream detail" not in serialized


def test_unexpected_exception_maps_to_safe_500_without_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        lens_service,
        "get_game_header",
        Mock(side_effect=RuntimeError("synthetic secret SQL detail")),
    )

    serialized, status = lens_service.serialize_matchup_lens_context(
        SYNTHETIC_GAME_ID
    )
    payload = json.loads(serialized)

    assert status == 500
    assert _reason(payload) == "UNEXPECTED_SERVER_ERROR"
    assert payload["game"] is None
    assert "synthetic secret SQL detail" not in serialized


def test_no_endpoint_write_path_is_present() -> None:
    sources = [
        inspect.getsource(lens_service),
        inspect.getsource(game_queries.get_matchup_lens_ranking_boundary),
        inspect.getsource(game_queries.get_matchup_lens_source_aligned_windows),
    ]
    forbidden_call_names = {
        "insert_rows",
        "insert_rows_json",
        "load_table_from_file",
        "load_table_from_json",
        "load_table_from_uri",
        "delete_table",
        "update_table",
    }
    for source in sources:
        tree = ast.parse(source)
        called = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assert not called & forbidden_call_names
        assert not any(
            token in source.upper()
            for token in (
                "INSERT INTO ",
                "UPDATE SET ",
                "DELETE FROM ",
                "MERGE INTO ",
                "CREATE TABLE ",
                "DROP TABLE ",
                "TRUNCATE TABLE ",
            )
        )
