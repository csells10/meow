import copy
import unittest

from api_calls.api_utils.parse_nfl_stats import parse_game_stats


APPROVED_METRICS = {
    "blocked_fg",
    "blocked_punt",
    "blocked_xp",
    "defensive_or_special_teams_tds",
    "defensive_tds",
    "defensive_two_point_returns",
    "first_downs_from_penalties",
    "passing_first_downs",
    "penalty_count",
    "penalty_yards",
    "rushing_first_downs",
    "safeties",
    "turnovers",
    "two_point_conversions",
}

SNAP_METRICS = {
    "total_offensive_snaps",
    "total_defensive_snaps",
    "total_special_teams_snaps",
    "total_snaps",
    "offensive_snap_load",
    "defensive_snap_load",
    "special_teams_snap_pct",
}

OPTIONAL_TEAM_FIELDS = {
    "blockedFG",
    "blockedPunt",
    "blockedXP",
    "defensiveOrSpecialTeamsTds",
    "defensiveTwoPointConversionReturns",
    "firstDownsFromPenalties",
    "passingFirstDowns",
    "rushingFirstDowns",
    "turnovers",
    "twoPointConversions",
}

OPTIONAL_DST_FIELDS = {"defTD", "safeties"}


def make_payload() -> dict:
    return {
        "statusCode": 200,
        "body": {
            "gameID": "synthetic_parser_fixture",
            "gameDate": "20260910",
            "gameStatus": "Completed",
            "gameStatusCode": "2",
            "homePts": "40",
            "awayPts": "7",
            "teamStats": {
                "home": {
                    "teamID": "32",
                    "teamAbv": "WSH",
                    "passingYards": "241",
                    "rushingYards": "180",
                    "rushingAttempts": "35",
                    "totalYards": "421",
                    "totalPlays": "69",
                    "firstDowns": "26",
                    "passTD": "2",
                    "rushTD": "3",
                    "passCompletionsAndAttempts": "21-29",
                    "sacksAndYardsLost": "2-11",
                    "thirdDownEfficiency": "7-13",
                    "fourthDownEfficiency": "1-1",
                    "redZoneScoredAndAttempted": "4-5",
                    "possession": "35:05",
                    "penalties": "8-55",
                    "blockedFG": "1",
                    "blockedPunt": "2",
                    "blockedXP": "0",
                    "turnovers": "0",
                    "passingFirstDowns": "14",
                    "rushingFirstDowns": "9",
                    "firstDownsFromPenalties": "3",
                    "twoPointConversions": "1",
                    "defensiveTwoPointConversionReturns": "0",
                    "defensiveOrSpecialTeamsTds": "2",
                    "snapCounts": {
                        "totalOffensive": "69",
                        "totalDefensive": "43",
                        "totalSpecialTeams": "26",
                    },
                },
                "away": {
                    "teamID": "5",
                    "teamAbv": "CAR",
                    "passingYards": "85",
                    "rushingYards": "95",
                    "rushingAttempts": "23",
                    "totalYards": "180",
                    "totalPlays": "43",
                    "firstDowns": "10",
                    "passTD": "1",
                    "rushTD": "0",
                    "passCompletionsAndAttempts": "13-18",
                    "sacksAndYardsLost": "2-14",
                    "thirdDownEfficiency": "2-10",
                    "fourthDownEfficiency": "0-1",
                    "redZoneScoredAndAttempted": "1-1",
                    "possession": "24:55",
                    "penalties": "6-59",
                    "blockedFG": "0",
                    "blockedPunt": "0",
                    "blockedXP": "0",
                    "turnovers": "2",
                    "passingFirstDowns": "5",
                    "rushingFirstDowns": "4",
                    "firstDownsFromPenalties": "1",
                    "twoPointConversions": "0",
                    "defensiveTwoPointConversionReturns": "0",
                    "defensiveOrSpecialTeamsTds": "0",
                    "snapCounts": {
                        "totalOffensive": "43",
                        "totalDefensive": "69",
                        "totalSpecialTeams": "26",
                    },
                },
            },
            "DST": {
                "home": {
                    "teamID": "32",
                    "teamAbv": "WSH",
                    "ptsAllowed": "7",
                    "ydsAllowed": "180",
                    "sacks": "2",
                    "defTD": "1",
                    "defensiveInterceptions": "2",
                    "fumblesRecovered": "0",
                    "safeties": "1",
                },
                "away": {
                    "teamID": "5",
                    "teamAbv": "CAR",
                    "ptsAllowed": "40",
                    "ydsAllowed": "421",
                    "sacks": "2",
                    "defTD": "0",
                    "defensiveInterceptions": "0",
                    "fumblesRecovered": "0",
                    "safeties": "0",
                },
            },
        },
    }


def rows_for_team(rows: list[dict], team_abv: str) -> dict[str, dict]:
    return {
        row["metric"]: row
        for row in rows
        if row["team_abv"] == team_abv
    }


class ParseNflStatsTests(unittest.TestCase):
    def test_captures_all_approved_metrics_with_source_values(self):
        rows = parse_game_stats(make_payload())
        home = rows_for_team(rows, "WSH")

        expected = {
            "blocked_fg": 1.0,
            "blocked_punt": 2.0,
            "blocked_xp": 0.0,
            "defensive_or_special_teams_tds": 2.0,
            "defensive_tds": 1.0,
            "defensive_two_point_returns": 0.0,
            "first_downs_from_penalties": 3.0,
            "passing_first_downs": 14.0,
            "penalty_count": 8.0,
            "penalty_yards": 55.0,
            "rushing_first_downs": 9.0,
            "safeties": 1.0,
            "turnovers": 0.0,
            "two_point_conversions": 1.0,
        }

        self.assertEqual(set(expected), APPROVED_METRICS)
        self.assertEqual(
            {metric: home[metric]["value"] for metric in APPROVED_METRICS},
            expected,
        )
        self.assertEqual(home["penalty_count"]["category"], "Team Discipline")
        self.assertEqual(home["penalty_count"]["core_area"], "Offensive Output")
        self.assertEqual(home["penalty_yards"]["category"], "Team Discipline")
        self.assertEqual(home["safeties"]["core_area"], "Disruption and Turnovers")
        self.assertEqual(home["turnovers"]["category"], "Offense")

    def test_preserves_explicit_zero_optional_metrics(self):
        payload = make_payload()
        for side in ("home", "away"):
            team_stats = payload["body"]["teamStats"][side]
            for field in OPTIONAL_TEAM_FIELDS:
                team_stats[field] = "0"
            team_stats["penalties"] = "0-0"
            dst_stats = payload["body"]["DST"][side]
            for field in OPTIONAL_DST_FIELDS:
                dst_stats[field] = "0"

        rows = parse_game_stats(payload)

        for team_abv in ("WSH", "CAR"):
            team_rows = rows_for_team(rows, team_abv)
            self.assertTrue(APPROVED_METRICS.issubset(team_rows))
            self.assertEqual(
                {team_rows[metric]["value"] for metric in APPROVED_METRICS},
                {0.0},
            )

    def test_omits_absent_optional_metrics_without_fabricating_zeroes(self):
        payload = make_payload()
        for side in ("home", "away"):
            team_stats = payload["body"]["teamStats"][side]
            for field in OPTIONAL_TEAM_FIELDS | {"penalties"}:
                team_stats.pop(field, None)
            dst_stats = payload["body"]["DST"][side]
            for field in OPTIONAL_DST_FIELDS:
                dst_stats.pop(field, None)

        rows = parse_game_stats(payload)

        for team_abv in ("WSH", "CAR"):
            self.assertTrue(
                APPROVED_METRICS.isdisjoint(rows_for_team(rows, team_abv))
            )

    def test_omits_all_snap_metrics_when_snap_group_is_absent(self):
        payload = make_payload()
        for side in ("home", "away"):
            payload["body"]["teamStats"][side].pop("snapCounts")

        rows = parse_game_stats(payload)

        for team_abv in ("WSH", "CAR"):
            self.assertTrue(SNAP_METRICS.isdisjoint(rows_for_team(rows, team_abv)))

    def test_captures_observed_historical_regular_season_snap_counts(self):
        payload = make_payload()
        payload["body"]["gameID"] = "20250907_CAR@JAX"
        payload["body"]["gameDate"] = "20250907"
        payload["body"]["seasonType"] = "Regular Season"

        payload["body"]["teamStats"]["away"].update({
            "teamID": "5",
            "teamAbv": "CAR",
            "snapCounts": {
                "totalOffensive": "64",
                "totalDefensive": "66",
                "totalSpecialTeams": "22",
            },
        })
        payload["body"]["teamStats"]["home"].update({
            "teamID": "15",
            "teamAbv": "JAX",
            "snapCounts": {
                "totalOffensive": "66",
                "totalDefensive": "64",
                "totalSpecialTeams": "22",
            },
        })

        rows = parse_game_stats(payload)
        car = rows_for_team(rows, "CAR")
        jax = rows_for_team(rows, "JAX")

        self.assertEqual(car["total_offensive_snaps"]["value"], 64.0)
        self.assertEqual(car["total_defensive_snaps"]["value"], 66.0)
        self.assertEqual(car["total_special_teams_snaps"]["value"], 22.0)
        self.assertEqual(car["total_snaps"]["value"], 152.0)
        self.assertEqual(car["offensive_snap_load"]["value"], 0.421)
        self.assertEqual(car["defensive_snap_load"]["value"], 0.434)
        self.assertEqual(car["special_teams_snap_pct"]["value"], 0.145)

        self.assertEqual(jax["total_offensive_snaps"]["value"], 66.0)
        self.assertEqual(jax["total_defensive_snaps"]["value"], 64.0)
        self.assertEqual(jax["total_special_teams_snaps"]["value"], 22.0)
        self.assertEqual(jax["total_snaps"]["value"], 152.0)
        self.assertEqual(jax["offensive_snap_load"]["value"], 0.434)
        self.assertEqual(jax["defensive_snap_load"]["value"], 0.421)
        self.assertEqual(jax["special_teams_snap_pct"]["value"], 0.145)

    def test_partial_snap_group_keeps_independent_counts_only(self):
        payload = make_payload()
        payload["body"]["teamStats"]["home"]["snapCounts"].pop(
            "totalSpecialTeams"
        )

        rows = parse_game_stats(payload)
        home = rows_for_team(rows, "WSH")
        away = rows_for_team(rows, "CAR")

        self.assertEqual(home["total_offensive_snaps"]["value"], 69.0)
        self.assertEqual(home["total_defensive_snaps"]["value"], 43.0)
        self.assertTrue(
            {
                "total_special_teams_snaps",
                "total_snaps",
                "offensive_snap_load",
                "defensive_snap_load",
                "special_teams_snap_pct",
            }.isdisjoint(home)
        )
        self.assertTrue(SNAP_METRICS.issubset(away))

    def test_malformed_optional_metric_is_omitted_not_converted_to_zero(self):
        payload = copy.deepcopy(make_payload())
        payload["body"]["teamStats"]["home"]["blockedFG"] = "not-a-number"

        rows = parse_game_stats(payload)
        home = rows_for_team(rows, "WSH")
        away = rows_for_team(rows, "CAR")

        self.assertNotIn("blocked_fg", home)
        self.assertEqual(away["blocked_fg"]["value"], 0.0)


if __name__ == "__main__":
    unittest.main()
