import unittest

import run_gamelens_packet4_coordinator as runner


class Packet4CoordinatorCliTests(unittest.TestCase):
    def test_cli_requires_explicit_confirmation(self):
        with self.assertRaisesRegex(ValueError, "--confirm-dev-write"):
            runner.main(["--game-id", "game-1", "--attempt-id", "attempt-1"])


if __name__ == "__main__":
    unittest.main()
