from __future__ import annotations

import unittest

from quant_core.execution.exit_sentinel import _breakout_action, _valid_open_price


class ExitSentinelRuleTests(unittest.TestCase):
    def test_valid_open_price_prefers_true_open_over_current_like_auction_price(self) -> None:
        quote = {
            "open": 15.13,
            "auction_price": 15.41,
            "current_price": 15.41,
        }

        self.assertAlmostEqual(_valid_open_price(quote), 15.13)

    def test_breakout_negative_open_premium_uses_nuke_action(self) -> None:
        action = _breakout_action(-0.13)

        self.assertEqual(action["action"], "核按钮")
        self.assertEqual(action["level"], "danger")


if __name__ == "__main__":
    unittest.main()
