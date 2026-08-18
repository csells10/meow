import unittest

import setup_gamelens_packet4_receipts as setup


class Packet4ReceiptSetupCliTests(unittest.TestCase):
    def test_cli_requires_explicit_confirmation(self):
        with self.assertRaisesRegex(ValueError, "--confirm-dev-setup"):
            setup.main([])


if __name__ == "__main__":
    unittest.main()
