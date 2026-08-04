import unittest
from unittest.mock import MagicMock, patch

from utils.helper import fetch_and_validate_api_data


class FetchAndValidateApiDataTests(unittest.TestCase):
    @staticmethod
    def _response(payload):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = payload
        response.text = ""
        return response

    @patch("utils.helper.requests.get")
    def test_empty_list_body_is_rejected_by_default(self, get):
        get.return_value = self._response({"body": []})

        result = fetch_and_validate_api_data("url", {}, {})

        self.assertIsNone(result)

    @patch("utils.helper.requests.get")
    def test_empty_list_body_is_allowed_only_when_requested(self, get):
        payload = {"body": []}
        get.return_value = self._response(payload)

        result = fetch_and_validate_api_data(
            "url",
            {},
            {},
            allow_empty_body=True,
        )

        self.assertEqual(result, payload)

    @patch("utils.helper.requests.get")
    def test_missing_body_remains_invalid_when_empty_is_allowed(self, get):
        get.return_value = self._response({})

        result = fetch_and_validate_api_data(
            "url",
            {},
            {},
            allow_empty_body=True,
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
