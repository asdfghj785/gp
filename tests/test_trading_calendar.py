from __future__ import annotations

import unittest
from datetime import date

from quant_core.daily_pick import next_weekday, nth_weekday
from quant_core.data_pipeline.trading_calendar import (
    is_trading_day,
    next_trading_day,
    nth_trading_day,
    trading_day_count_after,
)


class TradingCalendarTests(unittest.TestCase):
    def test_2026_labor_day_holiday_is_not_trading_day(self) -> None:
        self.assertTrue(is_trading_day(date(2026, 4, 30)))
        for day in range(1, 6):
            self.assertFalse(is_trading_day(date(2026, 5, day)))
        self.assertTrue(is_trading_day(date(2026, 5, 6)))

    def test_next_and_t3_trading_day_skip_labor_day_holiday(self) -> None:
        self.assertEqual(next_trading_day(date(2026, 4, 30)), date(2026, 5, 6))
        self.assertEqual(nth_trading_day(date(2026, 4, 30), 3), date(2026, 5, 8))
        self.assertEqual(trading_day_count_after(date(2026, 4, 30), date(2026, 5, 8)), 3)

    def test_daily_pick_compat_wrappers_use_trading_calendar(self) -> None:
        self.assertEqual(next_weekday(date(2026, 4, 30)), date(2026, 5, 6))
        self.assertEqual(nth_weekday(date(2026, 4, 30), 3), date(2026, 5, 8))


if __name__ == "__main__":
    unittest.main()
