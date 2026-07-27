import copy
import unittest

from api_calls.api_utils.validate_nfl_boxscore import validate_nfl_boxscore


def make_valid_payload() -> dict:
    return {
        "statusCode": 200,
        "body": {
            "gameID": "20260910_BUF@KC",
            "gameDate": "20260910",
            "gameStatus": "Completed",
            "gameStatusCode": "2",
            "homePts": "27",
            "awayPts": "20",
            "teamStats": {
                "home": {
                    "teamID": "12",
                    "teamAbv": "KC",
                    "passingYards": "289",
                    "rushingYards": "121",
                    "totalYards": "410",
                    "totalPlays": "64",
                    "firstDowns": "23",
                    "passCompletionsAndAttempts": "25-34",
                    "thirdDownEfficiency": "6-12",
                    "possession": "31:15",
                    "snapCounts": {
                        "totalOffensive": "64",
                        "totalDefensive": "59",
                        "totalSpecialTeams": "21",
                    },
                },
                "away": {
                    "teamID": "2",
                    "teamAbv": "BUF",
                    "passingYards": "251",
                    "rushingYards": "104",
                    "totalYards": "355",
                    "totalPlays": "59",
                    "firstDowns": "20",
                    "passCompletionsAndAttempts": "22-33",
                    "thirdDownEfficiency": "4-11",
                    "possession": "28:45",
                    "snapCounts": {
                        "totalOffensive": "59",
                        "totalDefensive": "64",
                        "totalSpecialTeams": "19",
                    },
                },
            },
            "DST": {
                "home": {
                    "teamID": "12",
                    "teamAbv": "KC",
                    "ptsAllowed": "20",
                    "ydsAllowed": "355",
                    "sacks": "3",
                },
                "away": {
                    "teamID": "2",
                    "teamAbv": "BUF",
                    "ptsAllowed": "27",
                    "ydsAllowed": "410",
                    "sacks": "2",
                },
            },
        },
    }


class ValidateNflBoxscoreTests(unittest.TestCase):
    def test_accepts_valid_completed_game(self):
        result = validate_nfl_boxscore(make_valid_payload())

        self.assertTrue(result.accepted)
        self.assertEqual(result.code, "accepted")
        self.assertEqual(result.game_id, "20260910_BUF@KC")
        self.assertEqual(result.team_ids, ("12", "2"))

    def test_accepts_final_status_for_compatibility(self):
        payload = make_valid_payload()
        payload["body"]["gameStatus"] = "Final"

        result = validate_nfl_boxscore(payload)

        self.assertTrue(result.accepted)
        self.assertEqual(result.code, "accepted")

    def test_rejects_non_final_game(self):
        payload = make_valid_payload()
        payload["body"]["gameStatus"] = "In Progress"
        payload["body"]["gameStatusCode"] = "1"

        result = validate_nfl_boxscore(payload)

        self.assertFalse(result.accepted)
        self.assertEqual(result.code, "game_not_final")

    def test_rejects_final_game_with_skeleton_body(self):
        payload = {
            "body": {
                "gameID": "20260910_BUF@KC",
                "gameDate": "20260910",
                "gameStatus": "Final",
                "gameStatusCode": "2",
                "homePts": "0",
                "awayPts": "0",
            }
        }

        result = validate_nfl_boxscore(payload)

        self.assertFalse(result.accepted)
        self.assertEqual(result.code, "missing_team")

    def test_rejects_payload_with_only_one_team(self):
        payload = make_valid_payload()
        del payload["body"]["teamStats"]["away"]
        del payload["body"]["DST"]["away"]

        result = validate_nfl_boxscore(payload)

        self.assertFalse(result.accepted)
        self.assertEqual(result.code, "missing_team")
        self.assertIn("away", result.reason)

    def test_rejects_missing_game_identity(self):
        payload = make_valid_payload()
        del payload["body"]["gameID"]

        result = validate_nfl_boxscore(payload)

        self.assertFalse(result.accepted)
        self.assertEqual(result.code, "missing_game_identity")

    def test_rejects_missing_team_identity(self):
        payload = make_valid_payload()
        del payload["body"]["teamStats"]["home"]["teamID"]
        del payload["body"]["DST"]["home"]["teamID"]

        result = validate_nfl_boxscore(payload)

        self.assertFalse(result.accepted)
        self.assertEqual(result.code, "missing_team_identity")
        self.assertIn("home", result.reason)

    def test_rejects_malformed_stats(self):
        payload = make_valid_payload()
        payload["body"]["teamStats"]["home"]["totalYards"] = "not-a-number"

        result = validate_nfl_boxscore(payload)

        self.assertFalse(result.accepted)
        self.assertEqual(result.code, "malformed_stats")
        self.assertIn("home.totalYards", result.reason)

    def test_accepts_legitimate_zero_valued_stats(self):
        payload = make_valid_payload()
        payload["body"]["homePts"] = "0"
        payload["body"]["awayPts"] = "0"

        for side in ("home", "away"):
            team_stats = payload["body"]["teamStats"][side]
            for field in (
                "passingYards",
                "rushingYards",
                "totalYards",
                "totalPlays",
                "firstDowns",
            ):
                team_stats[field] = "0"
            team_stats["passCompletionsAndAttempts"] = "0-0"
            team_stats["thirdDownEfficiency"] = "0-0"
            team_stats["possession"] = "00:00"
            for field in team_stats["snapCounts"]:
                team_stats["snapCounts"][field] = "0"

            dst_stats = payload["body"]["DST"][side]
            for field in ("ptsAllowed", "ydsAllowed", "sacks"):
                dst_stats[field] = "0"

        result = validate_nfl_boxscore(payload)

        self.assertTrue(result.accepted)
        self.assertEqual(result.code, "accepted")

    def test_accepts_requested_game_id_when_body_omits_it(self):
        payload = make_valid_payload()
        del payload["body"]["gameID"]

        result = validate_nfl_boxscore(
            payload,
            expected_game_id="20260910_BUF@KC",
        )

        self.assertTrue(result.accepted)
        self.assertEqual(result.game_id, "20260910_BUF@KC")

    def test_rejects_invalid_game_date_before_parser_use(self):
        payload = copy.deepcopy(make_valid_payload())
        payload["body"]["gameDate"] = "not-a-date"

        result = validate_nfl_boxscore(payload)

        self.assertFalse(result.accepted)
        self.assertEqual(result.code, "invalid_game_date")


if __name__ == "__main__":
    unittest.main()