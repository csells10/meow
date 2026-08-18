import unittest

import setup_gamelens_level3_columns as setup


class Level3SchemaSetupTests(unittest.TestCase):
    def test_cli_requires_explicit_confirmation_before_cloud_imports(self):
        with self.assertRaisesRegex(ValueError, "--confirm-dev-schema-update"):
            setup.main([])


if __name__ == "__main__":
    unittest.main()
