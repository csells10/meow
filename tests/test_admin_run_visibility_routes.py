from __future__ import annotations

import unittest
import subprocess
import sys
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

import auth.firebase_auth as firebase_auth
import qa_gamelens_packet5_admin_route as route_review
import routes.admin_run_visibility_routes as routes
from services.gamelens_admin_run_visibility_service import (
    DevelopmentRunVisibilityUnavailable,
    GameVisibilityNotFound,
)


class AdminRunVisibilityRouteTests(unittest.TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.register_blueprint(routes.admin_run_visibility_routes)
        app.testing = True
        self.client = app.test_client()
        self.runtime = SimpleNamespace(
            is_dev=True,
            active_season="2026",
            project_id="nfl-stream-406420",
        )

    @staticmethod
    def _query(**overrides):
        query = {
            "season": "2026",
            "season_type": "Preseason",
            "learning_run_id": "gamelens_2026_preseason_v1",
            "start_date": "2026-08-15",
            "end_date": "2026-08-15",
        }
        query.update(overrides)
        return query

    @staticmethod
    def _admin_token():
        return {
            "email": "admin@example.com",
            "gamelens_user": {
                "email": "admin@example.com",
                "role": "admin",
                "active": True,
            },
        }

    def _route_dependencies(self, *, service_result=None, service_error=None):
        stack = ExitStack()
        stack.enter_context(
            patch.object(
                firebase_auth,
                "verify_firebase_request",
                return_value=(self._admin_token(), None),
            )
        )
        stack.enter_context(
            patch.object(routes, "load_runtime_config", return_value=self.runtime)
        )
        client = stack.enter_context(
            patch.object(routes, "_create_bigquery_client", return_value="client")
        )
        module = stack.enter_context(
            patch.object(routes, "_bigquery_module", return_value="bigquery")
        )
        service = stack.enter_context(
            patch.object(
                routes,
                "get_admin_run_visibility",
                return_value=service_result,
                side_effect=service_error,
            )
        )
        return stack, client, module, service

    def test_route_requires_existing_admin_auth(self):
        def deny_request():
            return None, firebase_auth.unauthorized_response(
                "Missing Authorization Bearer token"
            )

        with patch.object(
            firebase_auth,
            "verify_firebase_request",
            side_effect=deny_request,
        ):
            response = self.client.get(
                "/admin/gamelens/run-visibility",
                query_string=self._query(),
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "unauthorized")

    def test_route_rejects_authenticated_non_admin(self):
        token = {
            "gamelens_user": {
                "email": "user@example.com",
                "role": "user",
                "active": True,
            }
        }
        with patch.object(
            firebase_auth,
            "verify_firebase_request",
            return_value=(token, None),
        ):
            response = self.client.get(
                "/admin/gamelens/run-visibility",
                query_string=self._query(),
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["message"], "Admin access required")

    def test_route_passes_validated_bounded_filters_to_service(self):
        expected = {
            "available": True,
            "scope": "admin_gamelens_run_visibility",
            "games": [],
        }
        stack, client, module, service = self._route_dependencies(
            service_result=expected
        )
        with stack:
            response = self.client.get(
                "/admin/gamelens/run-visibility",
                query_string=self._query(
                    game_id="20260815_DAL@SEA",
                    limit="7",
                ),
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), expected)
        client.assert_called_once_with("nfl-stream-406420")
        module.assert_called_once_with()
        service.assert_called_once_with(
            client="client",
            bigquery="bigquery",
            runtime_config=self.runtime,
            season="2026",
            season_type="Preseason",
            learning_run_id="gamelens_2026_preseason_v1",
            start_date=date(2026, 8, 15),
            end_date=date(2026, 8, 15),
            game_id="20260815_DAL@SEA",
            game_limit=7,
        )

    def test_missing_or_malformed_filters_return_safe_400(self):
        cases = (
            ({"learning_run_id": ""}, "learning_run_id"),
            ({"start_date": "08/15/2026"}, "YYYY-MM-DD"),
            ({"limit": "many"}, "limit must be an integer"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides):
                stack, client, module, service = self._route_dependencies()
                with stack:
                    response = self.client.get(
                        "/admin/gamelens/run-visibility",
                        query_string=self._query(**overrides),
                    )
                self.assertEqual(response.status_code, 400)
                self.assertEqual(
                    response.get_json()["error"],
                    "invalid_run_visibility_request",
                )
                self.assertIn(message, response.get_json()["message"])
                client.assert_not_called()
                module.assert_not_called()
                service.assert_not_called()

    def test_unknown_selected_game_returns_safe_404(self):
        stack, _, _, _ = self._route_dependencies(
            service_error=GameVisibilityNotFound("private detail")
        )
        with stack:
            response = self.client.get(
                "/admin/gamelens/run-visibility",
                query_string=self._query(game_id="20260815_BUF@CAR"),
            )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json()["error"], "game_not_found")
        self.assertNotIn("private detail", response.get_data(as_text=True))

    def test_development_source_fails_closed_outside_dev(self):
        production = SimpleNamespace(
            is_dev=False,
            active_season="2026",
            project_id="nfl-stream-406420",
        )
        with (
            patch.object(
                firebase_auth,
                "verify_firebase_request",
                return_value=(self._admin_token(), None),
            ),
            patch.object(routes, "load_runtime_config", return_value=production),
            patch.object(routes, "_create_bigquery_client") as client,
            patch.object(routes, "get_admin_run_visibility") as service,
        ):
            response = self.client.get(
                "/admin/gamelens/run-visibility",
                query_string=self._query(),
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(
            response.get_json()["error"], "development_source_unavailable"
        )
        client.assert_not_called()
        service.assert_not_called()

    def test_service_dev_guard_is_also_mapped_to_safe_403(self):
        stack, _, _, _ = self._route_dependencies(
            service_error=DevelopmentRunVisibilityUnavailable("private detail")
        )
        with stack:
            response = self.client.get(
                "/admin/gamelens/run-visibility",
                query_string=self._query(),
            )

        self.assertEqual(response.status_code, 403)
        self.assertNotIn("private detail", response.get_data(as_text=True))

    def test_unexpected_query_failure_returns_generic_500(self):
        stack, _, _, _ = self._route_dependencies(
            service_error=RuntimeError("secret table detail")
        )
        with stack:
            response = self.client.get(
                "/admin/gamelens/run-visibility",
                query_string=self._query(),
            )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.get_json()["error"], "run_visibility_query_failed"
        )
        self.assertNotIn("secret table detail", response.get_data(as_text=True))

    def test_route_is_get_only_and_cors_preflight_can_pass(self):
        post = self.client.post("/admin/gamelens/run-visibility")
        options = self.client.options("/admin/gamelens/run-visibility")

        self.assertEqual(post.status_code, 405)
        self.assertEqual(options.status_code, 204)

    def test_visual_route_checkpoint_entrypoint_is_executable(self):
        result = subprocess.run(
            [sys.executable, str(Path(route_review.__file__)), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("protected Packet 5 Admin route", result.stdout)

    def test_application_registers_the_protected_blueprint(self):
        import app as app_module

        self.assertIn("admin_run_visibility_routes", app_module.app.blueprints)


if __name__ == "__main__":
    unittest.main()
