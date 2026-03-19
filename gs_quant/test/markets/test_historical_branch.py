"""
Branch coverage tests for gs_quant/markets/historical.py
"""

import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from gs_quant.base import InstrumentBase, RiskKey
from gs_quant.risk.results import HistoricalPricingFuture, PricingFuture


class TestHistoricalPricingContextInit:
    """Test HistoricalPricingContext __init__."""

    def test_init_with_start_and_end(self):
        """Init with start and end dates."""
        from gs_quant.markets.historical import HistoricalPricingContext

        start = dt.date.today() - dt.timedelta(days=10)
        end = dt.date.today() - dt.timedelta(days=1)
        hpc = HistoricalPricingContext(start=start, end=end)
        assert hpc.date_range is not None
        assert len(hpc.date_range) > 0

    def test_init_with_start_no_end(self):
        """Init with start only; end defaults to today."""
        from gs_quant.markets.historical import HistoricalPricingContext

        # Use int start (business day offset) to avoid weekend issues
        hpc = HistoricalPricingContext(start=3)
        assert hpc.date_range is not None

    def test_init_with_int_start(self):
        """Init with int start (business day offset)."""
        from gs_quant.markets.historical import HistoricalPricingContext

        hpc = HistoricalPricingContext(start=5)
        assert hpc.date_range is not None

    def test_init_with_dates(self):
        """Init with explicit dates iterable."""
        from gs_quant.markets.historical import HistoricalPricingContext

        dates = [dt.date.today() - dt.timedelta(days=i) for i in range(5, 0, -1)]
        hpc = HistoricalPricingContext(dates=dates)
        assert hpc.date_range == tuple(dates)

    def test_init_start_and_dates_raises(self):
        """Init with both start and dates raises ValueError."""
        from gs_quant.markets.historical import HistoricalPricingContext

        with pytest.raises(ValueError, match='Must supply start or dates, not both'):
            HistoricalPricingContext(
                start=dt.date.today() - dt.timedelta(days=5),
                dates=[dt.date.today() - dt.timedelta(days=1)]
            )

    def test_init_neither_start_nor_dates_raises(self):
        """Init with neither start nor dates raises ValueError."""
        from gs_quant.markets.historical import HistoricalPricingContext

        with pytest.raises(ValueError, match='Must supply start or dates'):
            HistoricalPricingContext()

    def test_calc(self):
        """calc returns a HistoricalPricingFuture."""
        from gs_quant.markets.historical import HistoricalPricingContext
        from gs_quant.datetime.date import business_day_offset

        d1 = business_day_offset(dt.date.today(), -3, roll='preceding')
        d2 = business_day_offset(dt.date.today(), -2, roll='preceding')
        dates = [d1, d2]
        hpc = HistoricalPricingContext(dates=dates)
        mock_inst = MagicMock(spec=InstrumentBase)
        mock_inst.provider = MagicMock()
        mock_rm = MagicMock()

        hpc._on_enter()
        with patch.object(hpc, '_calc', return_value=MagicMock(spec=PricingFuture)):
            result = hpc.calc(mock_inst, mock_rm)
        assert isinstance(result, HistoricalPricingFuture)

    def test_calc_uses_context_provider(self):
        """calc uses context provider when set."""
        from gs_quant.markets.historical import HistoricalPricingContext
        from gs_quant.datetime.date import business_day_offset

        mock_provider = MagicMock()
        d1 = business_day_offset(dt.date.today(), -3, roll='preceding')
        dates = [d1]
        hpc = HistoricalPricingContext(dates=dates, provider=mock_provider)

        mock_inst = MagicMock(spec=InstrumentBase)
        mock_inst.provider = MagicMock()
        mock_rm = MagicMock()

        hpc._on_enter()
        with patch.object(hpc, '_calc', return_value=MagicMock(spec=PricingFuture)):
            result = hpc.calc(mock_inst, mock_rm)
        assert isinstance(result, HistoricalPricingFuture)


class TestBackToTheFuturePricingContext:
    """Test BackToTheFuturePricingContext."""

    def test_init_with_dates(self):
        """Init with explicit dates."""
        from gs_quant.markets.historical import BackToTheFuturePricingContext

        dates = [dt.date.today() - dt.timedelta(days=3), dt.date.today() - dt.timedelta(days=2)]
        bttf = BackToTheFuturePricingContext(dates=dates)
        assert bttf.date_range is not None

    def test_init_with_start_end(self):
        """Init with start and end."""
        from gs_quant.markets.historical import BackToTheFuturePricingContext

        start = dt.date.today() - dt.timedelta(days=10)
        end = dt.date.today() - dt.timedelta(days=1)
        bttf = BackToTheFuturePricingContext(start=start, end=end)
        assert bttf.date_range is not None

    def test_init_with_start_no_end(self):
        """Init with start only; end defaults to today."""
        from gs_quant.markets.historical import BackToTheFuturePricingContext

        bttf = BackToTheFuturePricingContext(start=3)
        assert bttf.date_range is not None

    def test_init_start_and_dates_raises(self):
        """Init with both start and dates raises ValueError."""
        from gs_quant.markets.historical import BackToTheFuturePricingContext

        with pytest.raises(ValueError, match='Must supply start or dates, not both'):
            BackToTheFuturePricingContext(
                start=dt.date.today() - dt.timedelta(days=5),
                dates=[dt.date.today() - dt.timedelta(days=1)]
            )

    def test_init_neither_start_nor_dates_raises(self):
        """Init with neither start nor dates raises ValueError."""
        from gs_quant.markets.historical import BackToTheFuturePricingContext

        with pytest.raises(ValueError, match='Must supply start or dates'):
            BackToTheFuturePricingContext()

    def test_calc_with_past_dates(self):
        """calc with past dates uses normal pricing path."""
        from gs_quant.markets.historical import BackToTheFuturePricingContext
        from gs_quant.datetime.date import business_day_offset

        past_date = business_day_offset(dt.date.today(), -5, roll='preceding')
        dates = [past_date]
        bttf = BackToTheFuturePricingContext(dates=dates)

        mock_inst = MagicMock(spec=InstrumentBase)
        mock_inst.provider = MagicMock()
        mock_rm = MagicMock()

        bttf._on_enter()
        with patch.object(bttf, '_calc', return_value=MagicMock(spec=PricingFuture)):
            result = bttf.calc(mock_inst, mock_rm)
        assert isinstance(result, HistoricalPricingFuture)

    def test_calc_with_future_dates(self):
        """calc with future dates uses RollFwd scenario."""
        from gs_quant.markets.historical import BackToTheFuturePricingContext
        from gs_quant.datetime.date import business_day_offset

        future_date = dt.date.today() + dt.timedelta(days=30)
        past_date = business_day_offset(dt.date.today(), -5, roll='preceding')
        dates = [past_date, future_date]
        bttf = BackToTheFuturePricingContext(dates=dates)

        mock_inst = MagicMock(spec=InstrumentBase)
        mock_inst.provider = MagicMock()
        mock_rm = MagicMock()

        bttf._on_enter()
        with patch.object(bttf, '_calc', return_value=MagicMock(spec=PricingFuture)):
            result = bttf.calc(mock_inst, mock_rm)
        assert isinstance(result, HistoricalPricingFuture)

    def test_calc_uses_context_provider(self):
        """calc uses context provider when set."""
        from gs_quant.markets.historical import BackToTheFuturePricingContext
        from gs_quant.datetime.date import business_day_offset

        mock_provider = MagicMock()
        past_date = business_day_offset(dt.date.today(), -3, roll='preceding')
        dates = [past_date]
        bttf = BackToTheFuturePricingContext(dates=dates, provider=mock_provider)

        mock_inst = MagicMock(spec=InstrumentBase)
        mock_inst.provider = MagicMock()
        mock_rm = MagicMock()

        bttf._on_enter()
        with patch.object(bttf, '_calc', return_value=MagicMock(spec=PricingFuture)):
            result = bttf.calc(mock_inst, mock_rm)
        assert isinstance(result, HistoricalPricingFuture)
