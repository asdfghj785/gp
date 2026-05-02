from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from live_sentinel import (
    build_push_content,
    calculate_realtime_vwap,
    close_settlement_context,
    is_t_plus_3_timeout,
    should_verify_fake_dump,
    supports_t_plus_3_timeout,
    t_plus_3_date,
    verify_fake_dump,
)


class LiveSentinelRuleTests(unittest.TestCase):
    def test_breakout_does_not_use_t3_timeout_even_on_target_date(self) -> None:
        position = {
            "strategy_type": "尾盘突破",
            "buy_date": "2026-04-29",
            "target_date": "2026-04-30",
            "source": "daily_picks_1450",
        }

        self.assertFalse(supports_t_plus_3_timeout(position))
        self.assertFalse(is_t_plus_3_timeout(position, today=date(2026, 4, 30)))

    def test_swing_strategy_uses_t3_timeout(self) -> None:
        position = {
            "strategy_type": "右侧主升浪",
            "buy_date": "2026-04-27",
            "target_date": "2026-04-30",
            "source": "daily_picks_1450",
        }

        self.assertTrue(supports_t_plus_3_timeout(position))
        self.assertTrue(is_t_plus_3_timeout(position, today=date(2026, 4, 30)))

    def test_legacy_manual_position_without_strategy_keeps_t3_timeout(self) -> None:
        position = {
            "buy_date": "2026-04-27",
            "target_date": "2026-04-30",
        }

        self.assertTrue(supports_t_plus_3_timeout(position))
        self.assertTrue(is_t_plus_3_timeout(position, today=date(2026, 4, 30)))

    def test_breakout_push_content_uses_target_date_label(self) -> None:
        content = build_push_content(
            position={
                "code": "603305",
                "name": "旭升集团",
                "strategy_type": "尾盘突破",
                "buy_date": "2026-04-29",
                "target_date": "2026-04-30",
                "source": "daily_picks_1450",
            },
            reason="initial_stop",
            message="测试",
            current_price=15.13,
            highest_price=15.15,
            buy_price=15.15,
            current_gain_pct=-0.13,
            highest_gain_pct=0.0,
            drawdown_pct=-0.13,
            quote={"date": "2026-04-30", "time": "09:15:05"},
            checked_at="2026-04-30T09:15:05",
        )

        self.assertIn("- 命中策略：尾盘突破", content)
        self.assertIn("- 目标日期：2026-04-30", content)
        self.assertNotIn("T+3 日期", content)

    def test_morning_auction_close_settles_at_open_price(self) -> None:
        settlement = close_settlement_context(
            quote={"date": "2026-04-30", "time": "09:19:39", "open": 48.23, "current_price": 51.88},
            checked_at="2026-04-30T09:19:42",
            trigger_price=51.88,
            buy_price=49.64,
        )

        self.assertEqual(settlement["settlement_basis"], "morning_auction_open")
        self.assertAlmostEqual(settlement["settlement_price"], 48.23)
        self.assertAlmostEqual(settlement["settlement_gain_pct"], -2.84045125)

    def test_continuous_session_close_settles_at_trigger_price(self) -> None:
        settlement = close_settlement_context(
            quote={"date": "2026-04-30", "time": "09:31:00", "open": 48.23, "current_price": 51.88},
            checked_at="2026-04-30T09:31:01",
            trigger_price=51.88,
            buy_price=49.64,
        )

        self.assertEqual(settlement["settlement_basis"], "realtime")
        self.assertAlmostEqual(settlement["settlement_price"], 51.88)
        self.assertAlmostEqual(settlement["settlement_gain_pct"], 4.51248993)

    def test_t_plus_3_date_uses_market_trading_calendar(self) -> None:
        self.assertEqual(t_plus_3_date("2026-04-30"), "2026-05-08")

    def test_realtime_vwap_uses_price_volume_weighting(self) -> None:
        df = pd.DataFrame(
            [
                {"close": 10.0, "volume": 100.0},
                {"close": 11.0, "volume": 300.0},
            ]
        )

        self.assertAlmostEqual(calculate_realtime_vwap(df), 10.75)

    def test_verify_fake_dump_flags_price_holding_vwap(self) -> None:
        result = verify_fake_dump({"current_price": 10.0, "vwap": 10.04, "weibi": -90})

        self.assertTrue(result["fake_dump"])
        self.assertEqual(result["status"], "FAKE_DUMP")

    def test_verify_fake_dump_rejects_vwap_breakdown(self) -> None:
        result = verify_fake_dump({"current_price": 9.90, "vwap": 10.04, "weibi": -90})

        self.assertFalse(result["fake_dump"])
        self.assertEqual(result["status"], "REAL_DUMP")

    def test_fake_dump_verification_does_not_intercept_hard_stop(self) -> None:
        self.assertFalse(should_verify_fake_dump({"buy_price": 10.0}, 9.7))
        self.assertTrue(should_verify_fake_dump({"buy_price": 10.0}, 9.71))


if __name__ == "__main__":
    unittest.main()
