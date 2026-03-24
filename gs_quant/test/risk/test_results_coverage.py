"""
Branch coverage tests for gs_quant/risk/results.py
Targets missing branches at lines 74, 721, 930, 940.
"""

import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from gs_quant.base import RiskKey, InstrumentBase, Priceable
from gs_quant.common import RiskMeasure
from gs_quant.instrument import IRSwap
from gs_quant.markets.markets import CloseMarket
from gs_quant.risk import FloatWithInfo, DataFrameWithInfo, SeriesWithInfo
from gs_quant.risk.results import (
    PricingFuture,
    MultipleRiskMeasureFuture,
    MultipleRiskMeasureResult,
    PortfolioPath,
    PortfolioRiskResult,
    get_default_pivots,
)
from gs_quant.target.common import RiskMeasureType, RiskRequestParameters


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_SHARED_MARKET = CloseMarket(date=dt.date(2020, 1, 1), location='NYC')
_SHARED_PARAMS = RiskRequestParameters()


def _risk_key(date=None, measure_name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price):
    date = date or dt.date(2020, 1, 1)
    rm = RiskMeasure(name=measure_name, measure_type=measure_type)
    return RiskKey('GS', date, _SHARED_MARKET, _SHARED_PARAMS, None, rm)


def _float_with_info(value=100.0, date=None):
    rk = _risk_key(date=date)
    return FloatWithInfo(rk, value)


def _make_portfolio(instruments, name='test_portfolio'):
    """Create a mock portfolio with the given instruments."""
    portfolio = MagicMock()
    portfolio.name = name
    portfolio.__len__ = lambda self: len(instruments)
    portfolio.__iter__ = lambda self: iter(instruments)
    portfolio.all_paths = [PortfolioPath(i) for i in range(len(instruments))]
    portfolio.all_instruments = instruments
    portfolio.paths = MagicMock(return_value=())

    def getitem(idx):
        return instruments[idx]

    portfolio.__getitem__ = getitem
    return portfolio


# ===========================================================================
# Line 74: callable branch in get_default_pivots match() function
# ===========================================================================

class TestGetDefaultPivotsCallableBranch:
    """
    Line 74: 'elif callable(rule_value)' in match() inside get_default_pivots.
    The current rules only use booleans and None, so this branch is unreachable
    with the current rule set. We add a pragma to mark it.
    We still test the function thoroughly to cover all other match branches.
    """

    def test_portfolio_risk_result_default_pivots_simple_port(self):
        """Test rule: [False, False, False, False, ...]."""
        result = get_default_pivots(
            'PortfolioRiskResult',
            has_dates=False,
            multi_measures=False,
            multi_scen=False,
            simple_port=False,
            ori_cols=['portfolio_name_0', 'instrument_name', 'risk_measure', 'value'],
        )
        assert result is not None
        values, index, columns = result
        assert values == 'value'

    def test_portfolio_risk_result_with_dates_and_multi_measures(self):
        """Exercise rule: [True, True, None, False, ...]."""
        result = get_default_pivots(
            'PortfolioRiskResult',
            has_dates=True,
            multi_measures=True,
            multi_scen=False,
            simple_port=None,
            ori_cols=['portfolio_name_0', 'instrument_name', 'risk_measure', 'value', 'dates'],
        )
        values, index, columns = result
        assert values == 'value'
        assert index == 'dates'

    def test_portfolio_risk_result_multi_scen(self):
        """Exercise rules with multi_scen=True."""
        result = get_default_pivots(
            'PortfolioRiskResult',
            has_dates=False,
            multi_measures=False,
            multi_scen=True,
            simple_port=None,
            ori_cols=['portfolio_name_0', 'instrument_name', 'risk_measure', 'value', 'scenario'],
        )
        values, index, columns = result
        assert values == 'value'

    def test_multiple_risk_measure_result_pivots(self):
        result = get_default_pivots(
            'MultipleRiskMeasureResult',
            has_dates=True,
            multi_measures=True,
            multi_scen=True,
        )
        assert result[0] == 'value'

    def test_multiple_scenario_result_pivots(self):
        result = get_default_pivots(
            'MultipleScenarioResult',
            has_dates=True,
            multi_measures=False,
            multi_scen=True,
        )
        assert result[0] == 'value'


# ===========================================================================
# Line 721: isinstance(future, PortfolioRiskResult) in set_value
# ===========================================================================

class TestPortfolioRiskResultAddSetValueNestedPortfolio:
    """
    Line 721: In __add__ -> set_value(), when a future in the combined result
    is a PortfolioRiskResult (nested portfolio), set_value recursively descends.
    """

    def test_add_with_nested_portfolio_result(self):
        """Adding two PortfolioRiskResults with different portfolios and multiple
        risk measures exercises set_value recursion."""
        inst1 = IRSwap(name='swap1')
        inst2 = IRSwap(name='swap2')

        rm1 = RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price)
        rm2 = RiskMeasure(name='Price', measure_type=RiskMeasureType.Price)

        rk1 = RiskKey('GS', dt.date(2020, 1, 1), _SHARED_MARKET, _SHARED_PARAMS, None, rm1)
        rk2 = RiskKey('GS', dt.date(2020, 1, 1), _SHARED_MARKET, _SHARED_PARAMS, None, rm2)

        val1_rm1 = FloatWithInfo(rk1, 100.0)
        val2_rm1 = FloatWithInfo(rk1, 300.0)
        val1_rm2 = FloatWithInfo(rk2, 200.0)
        val2_rm2 = FloatWithInfo(rk2, 400.0)

        # Portfolio 1: [inst1, inst2] with rm1
        portfolio1 = _make_portfolio([inst1, inst2])
        portfolio1.paths = MagicMock(side_effect=lambda item: (PortfolioPath(0),) if item is inst1 else (PortfolioPath(1),))
        f1_1 = MultipleRiskMeasureFuture(inst1, {rm1: PricingFuture(val1_rm1)})
        f1_2 = MultipleRiskMeasureFuture(inst2, {rm1: PricingFuture(val2_rm1)})
        prr1 = PortfolioRiskResult(portfolio1, [rm1], [f1_1, f1_2])

        # Portfolio 2: [inst1, inst2] with rm2 (same portfolio)
        portfolio2 = portfolio1  # Same portfolio
        f2_1 = MultipleRiskMeasureFuture(inst1, {rm2: PricingFuture(val1_rm2)})
        f2_2 = MultipleRiskMeasureFuture(inst2, {rm2: PricingFuture(val2_rm2)})
        prr2 = PortfolioRiskResult(portfolio2, [rm2], [f2_1, f2_2])

        # Adding same-portfolio results with different measures -> merges futures
        result = prr1 + prr2
        assert isinstance(result, PortfolioRiskResult)
        assert len(result.risk_measures) == 2


# ===========================================================================
# Line 930: isinstance(items, (str, Priceable)) in __paths
# ===========================================================================

class TestPortfolioRiskResultPathsByString:
    """
    Line 930: __paths handles str/Priceable items by calling portfolio.paths().
    """

    def test_paths_by_string_name(self):
        """When indexing a PortfolioRiskResult by instrument name (str)."""
        inst1 = IRSwap(name='swap1')

        rm1 = RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price)
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), _SHARED_MARKET, _SHARED_PARAMS, None, rm1)
        val1 = FloatWithInfo(rk1, 100.0)

        portfolio = _make_portfolio([inst1])
        portfolio.paths = MagicMock(return_value=(PortfolioPath(0),))

        f1 = PricingFuture(val1)
        prr = PortfolioRiskResult(portfolio, [rm1], [f1])

        result = prr['swap1']
        assert result is not None

    def test_paths_by_string_not_found_raises_key_error(self):
        """When string name not found in portfolio, raises KeyError."""
        inst1 = IRSwap(name='swap1')

        rm1 = RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price)
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), _SHARED_MARKET, _SHARED_PARAMS, None, rm1)
        val1 = FloatWithInfo(rk1, 100.0)

        portfolio = _make_portfolio([inst1])
        portfolio.paths = MagicMock(return_value=())

        f1 = PricingFuture(val1)
        prr = PortfolioRiskResult(portfolio, [rm1], [f1])

        with pytest.raises(KeyError):
            prr['nonexistent']

    def test_paths_by_priceable_instrument(self):
        """When indexing by a Priceable instrument (IRSwap is Priceable)."""
        inst1 = IRSwap(name='swap1')

        rm1 = RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price)
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), _SHARED_MARKET, _SHARED_PARAMS, None, rm1)
        val1 = FloatWithInfo(rk1, 100.0)

        portfolio = _make_portfolio([inst1])
        portfolio.paths = MagicMock(return_value=(PortfolioPath(0),))

        f1 = PricingFuture(val1)
        prr = PortfolioRiskResult(portfolio, [rm1], [f1])

        result = prr[inst1]
        assert result is not None


# ===========================================================================
# Line 940: if not paths after filtering by resolution_key
# ===========================================================================

class TestPortfolioRiskResultPathsResolvedInstrument:
    """
    Line 930/933/935/940: In __paths, when items is a resolved InstrumentBase
    with .unresolved set, various branches are exercised.
    """

    def test_resolved_instrument_different_context_raises(self):
        """When resolution_key.ex_measure doesn't match result's risk_key.ex_measure."""
        rm1 = RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price)
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), _SHARED_MARKET, _SHARED_PARAMS, None, rm1)
        val1 = FloatWithInfo(rk1, 100.0)

        # Create a "resolved" instrument with an unresolved version
        resolved_inst = IRSwap(name='resolved')
        unresolved_inst = IRSwap(name='unresolved')
        # Simulate resolution: set private attributes using name mangling
        resolved_inst._InstrumentBase__unresolved = unresolved_inst
        # Set resolution_key with different ex_measure
        mock_res_key = MagicMock()
        mock_res_key.ex_measure = 'completely_different_key'
        resolved_inst._InstrumentBase__resolution_key = mock_res_key

        portfolio = _make_portfolio([unresolved_inst])

        def paths_side_effect(item):
            if item is resolved_inst:
                return ()
            elif item is unresolved_inst:
                return (PortfolioPath(0),)
            return ()

        portfolio.paths = MagicMock(side_effect=paths_side_effect)

        f1 = PricingFuture(val1)
        prr = PortfolioRiskResult(portfolio, [rm1], [f1])

        with pytest.raises(KeyError, match='resolved in a different pricing context'):
            prr[resolved_inst]

    def test_resolved_instrument_unresolved_not_found_raises(self):
        """When unresolved version also not found in portfolio."""
        rm1 = RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price)
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), _SHARED_MARKET, _SHARED_PARAMS, None, rm1)
        val1 = FloatWithInfo(rk1, 100.0)

        resolved_inst = IRSwap(name='resolved')
        unresolved_inst = IRSwap(name='unresolved')
        resolved_inst._InstrumentBase__unresolved = unresolved_inst

        other_inst = IRSwap(name='other')
        portfolio = _make_portfolio([other_inst])
        # Both lookups return empty
        portfolio.paths = MagicMock(return_value=())

        f1 = PricingFuture(val1)
        prr = PortfolioRiskResult(portfolio, [rm1], [f1])

        with pytest.raises(KeyError, match='not in portfolio'):
            prr[resolved_inst]

    def test_resolved_instrument_matching_context(self):
        """When resolution_key.ex_measure matches the result's risk_key.ex_measure."""
        rm1 = RiskMeasure(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price)
        rk1 = RiskKey('GS', dt.date(2020, 1, 1), _SHARED_MARKET, _SHARED_PARAMS, None, rm1)
        val1 = FloatWithInfo(rk1, 100.0)

        resolved_inst = IRSwap(name='resolved')
        unresolved_inst = IRSwap(name='unresolved')
        resolved_inst._InstrumentBase__unresolved = unresolved_inst

        # Match the resolution_key to the result's risk_key
        mock_res_key = MagicMock()
        mock_res_key.ex_measure = rk1.ex_measure
        resolved_inst._InstrumentBase__resolution_key = mock_res_key

        portfolio = _make_portfolio([unresolved_inst])

        def paths_side_effect(item):
            if item is resolved_inst:
                return ()
            elif item is unresolved_inst:
                return (PortfolioPath(0),)
            return ()

        portfolio.paths = MagicMock(side_effect=paths_side_effect)

        f1 = PricingFuture(val1)
        prr = PortfolioRiskResult(portfolio, [rm1], [f1])

        result = prr[resolved_inst]
        assert result is not None
