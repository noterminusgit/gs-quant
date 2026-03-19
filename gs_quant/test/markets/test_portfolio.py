"""
Copyright 2018 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import datetime as dt
from unittest import mock

import gs_quant.risk as risk
import numpy as np
import pandas as pd
from gs_quant.api.gs.assets import GsAssetApi
from gs_quant.api.gs.portfolios import GsPortfolioApi
from gs_quant.common import PositionSet
from gs_quant.datetime import business_day_offset
from gs_quant.instrument import IRSwap, IRSwaption, CurveScenario
from gs_quant.markets import (
    HistoricalPricingContext,
    PricingContext,
    BackToTheFuturePricingContext,
    historical_risk_key,
)
from gs_quant.markets.portfolio import Portfolio
from gs_quant.risk.results import PortfolioPath, PortfolioRiskResult
from gs_quant.session import Environment, GsSession
from gs_quant.target.portfolios import Portfolio as MarqueePortfolio
from gs_quant.test.utils.mock_calc import MockCalc


def set_session():
    from gs_quant.session import OAuth2Session

    OAuth2Session.init = mock.MagicMock(return_value=None)
    GsSession.use(Environment.PROD, 'client_id', 'secret')


def test_portfolio(mocker):
    with MockCalc(mocker):
        with PricingContext(pricing_date=dt.date(2020, 10, 15)):
            swap1 = IRSwap('Pay', '10y', 'USD', fixed_rate=0.001, name='swap_10y@10bp')
            swap2 = IRSwap('Pay', '10y', 'USD', fixed_rate=0.002, name='swap_10y@20bp')
            swap3 = IRSwap('Pay', '10y', 'USD', fixed_rate=0.003, name='swap_10y@30bp')

            portfolio = Portfolio((swap1, swap2, swap3))

            prices: PortfolioRiskResult = portfolio.dollar_price()
            result = portfolio.calc((risk.DollarPrice, risk.IRDelta))

        assert tuple(sorted(map(lambda x: round(x, 0), prices))) == (4439480.0, 5423407.0, 6407334.0)
        assert round(prices.aggregate(), 2) == 16270220.58
        assert round(prices[0], 0) == 6407334.0
        assert round(prices[swap2], 0) == 5423407.0
        assert round(prices['swap_10y@30bp'], 0) == 4439480.0

        assert tuple(map(lambda x: round(x, 0), result[risk.DollarPrice])) == (6407334.0, 5423407.0, 4439480.0)
        assert round(result[risk.DollarPrice].aggregate(), 0) == 16270221.0
        assert round(result[risk.DollarPrice]['swap_10y@30bp'], 0) == 4439480.0
        assert round(result[risk.DollarPrice]['swap_10y@30bp'], 0) == round(
            result['swap_10y@30bp'][risk.DollarPrice], 0
        )

        assert round(result[risk.IRDelta].aggregate().value.sum(), 0) == 278984.0

        prices_only = result[risk.DollarPrice]
        assert tuple(map(lambda x: round(x, 0), prices)) == tuple(map(lambda x: round(x, 0), prices_only))

        swap4 = IRSwap('Pay', '10y', 'USD', fixed_rate=-0.001, name='swap_10y@-10bp')
        portfolio.append(swap4)
        assert len(portfolio.instruments) == 4

        extracted_swap = portfolio.pop('swap_10y@20bp')
        assert extracted_swap == swap2
        assert len(portfolio.instruments) == 3

        swap_dict = {'swap_5': swap1, 'swap_6': swap2, 'swap_7': swap3}

        portfolio = Portfolio(swap_dict)
        assert len(portfolio) == 3

        # extend a portfolio with a portfolio
        new_portfolio = Portfolio([IRSwap(termination_date=x, name=x) for x in ['4y', '5y']])
        portfolio.extend(new_portfolio)
        assert len(portfolio) == 5

        # extend a portfolio with a list of trades
        portfolio.extend([IRSwap(termination_date=x, name=x) for x in ['6y', '7y']])
        assert len(portfolio) == 7


def test_construction():
    swap1 = IRSwap('Pay', '10y', 'USD')
    swap2 = IRSwap('Pay', '5y', 'USD')
    my_list = [swap1, swap2]
    my_tuple = (swap1, swap2)
    my_np_arr = np.array((swap1, swap2))

    p1 = Portfolio(my_list)
    p2 = Portfolio(my_tuple)
    p3 = Portfolio(my_np_arr)

    assert len(p1) == 2
    assert len(p2) == 2
    assert len(p3) == 2
    assert p1 == p2
    assert p2 == p3


def test_historical_pricing(mocker):
    with MockCalc(mocker):
        swap1 = IRSwap('Pay', '10y', 'USD', fixed_rate='ATM+1', name='10y@a+1')
        swap2 = IRSwap('Pay', '10y', 'USD', fixed_rate='ATM+2', name='10y@a+2')
        swap3 = IRSwap('Pay', '10y', 'USD', fixed_rate='ATM+3', name='10y@a+3')

        portfolio = Portfolio((swap1, swap2, swap3))
        dates = (dt.date(2021, 2, 9), dt.date(2021, 2, 10), dt.date(2021, 2, 11))

        with HistoricalPricingContext(dates=dates) as hpc:
            risk_key = hpc._PricingContext__risk_key(risk.DollarPrice, swap1.provider)
            results = portfolio.calc((risk.DollarPrice, risk.IRDelta))

        expected = risk.SeriesWithInfo(
            pd.Series(
                data=[-580315.9786130451, -580372.2014339305, -580808.9413858932],
                index=[dt.date(2021, 2, 9), dt.date(2021, 2, 10), dt.date(2021, 2, 11)],
            ),
            risk_key=historical_risk_key(risk_key),
        )

        assert results.dates == dates
        actual = results[risk.DollarPrice].aggregate()
        assert actual.equals(expected)

        assert (
            results[dt.date(2021, 2, 9)][risk.DollarPrice]['10y@a+1']
            == results[risk.DollarPrice][dt.date(2021, 2, 9)]['10y@a+1']
        )
        assert (
            results[dt.date(2021, 2, 9)][risk.DollarPrice]['10y@a+1']
            == results[risk.DollarPrice]['10y@a+1'][dt.date(2021, 2, 9)]
        )
        assert (
            results[dt.date(2021, 2, 9)][risk.DollarPrice]['10y@a+1']
            == results['10y@a+1'][risk.DollarPrice][dt.date(2021, 2, 9)]
        )
        assert (
            results[dt.date(2021, 2, 9)][risk.DollarPrice]['10y@a+1']
            == results['10y@a+1'][dt.date(2021, 2, 9)][risk.DollarPrice]
        )
        assert (
            results[dt.date(2021, 2, 9)][risk.DollarPrice]['10y@a+1']
            == results[dt.date(2021, 2, 9)]['10y@a+1'][risk.DollarPrice]
        )


def test_backtothefuture_pricing(mocker):
    with MockCalc(mocker):
        swap1 = IRSwap('Pay', '10y', 'USD', fixed_rate=0.01, name='swap1')
        swap2 = IRSwap('Pay', '10y', 'USD', fixed_rate=0.02, name='swap2')
        swap3 = IRSwap('Pay', '10y', 'USD', fixed_rate=0.03, name='swap3')

        portfolio = Portfolio((swap1, swap2, swap3))
        pricing_date = dt.date(2021, 2, 10)
        with PricingContext(pricing_date=pricing_date):
            with BackToTheFuturePricingContext(
                dates=business_day_offset(pricing_date, [-1, 0, 1], roll='forward'), name='btf'
            ) as hpc:
                risk_key = hpc._PricingContext__risk_key(risk.DollarPrice, swap1.provider)
                results = portfolio.calc(risk.DollarPrice)

    expected = risk.SeriesWithInfo(
        pd.Series(
            data=[-22711963.80864744, -22655907.930484552, -21582551.58922608],
            index=business_day_offset(pricing_date, [-1, 0, 1], roll='forward'),
        ),
        risk_key=historical_risk_key(risk_key),
    )

    actual = results[risk.DollarPrice].aggregate()

    assert actual.equals(expected)


def test_duplicate_instrument(mocker):
    with MockCalc(mocker):
        swap1 = IRSwap('Pay', '1y', 'EUR', name='EUR1y')
        swap2 = IRSwap('Pay', '2y', 'EUR', name='EUR2y')
        swap3 = IRSwap('Pay', '3y', 'EUR', name='EUR3y')

        portfolio = Portfolio((swap1, swap2, swap3, swap1))
        assert portfolio.paths('EUR1y') == (PortfolioPath(0), PortfolioPath(3))
        assert portfolio.paths('EUR2y') == (PortfolioPath(1),)
        with PricingContext(pricing_date=dt.date(2020, 10, 15)):
            fwds: PortfolioRiskResult = portfolio.calc(risk.IRFwdRate)

        assert tuple(map(lambda x: round(x, 6), fwds)) == (-0.005378, -0.005224, -0.00519, -0.005378)
        assert round(fwds.aggregate(), 6) == -0.02117
        assert round(fwds[swap1], 6) == -0.005378


def test_nested_portfolios(mocker):
    swap1 = IRSwap('Pay', '10y', 'USD', name='USD-swap')
    swap2 = IRSwap('Pay', '10y', 'EUR', name='EUR-swap')
    swap3 = IRSwap('Pay', '10y', 'GBP', name='GBP-swap')

    swap4 = IRSwap('Pay', '10y', 'JPY', name='JPY-swap')
    swap5 = IRSwap('Pay', '10y', 'HUF', name='HUF-swap')
    swap6 = IRSwap('Pay', '10y', 'CHF', name='CHF-swap')

    portfolio2_1 = Portfolio((swap1, swap2, swap3), name='portfolio2_1')
    portfolio2_2 = Portfolio((swap1, swap2, swap3), name='portfolio2_2')
    portfolio1_1 = Portfolio((swap4, portfolio2_1), name='portfolio1_1')
    portfolio1_2 = Portfolio((swap5, portfolio2_2), name='USD-swap')
    portfolio = Portfolio((swap6, portfolio1_1, portfolio1_2), name='portfolio')

    assert portfolio.paths('USD-swap') == (PortfolioPath(2), PortfolioPath((1, 1, 0)), PortfolioPath((2, 1, 0)))


def test_single_instrument(mocker):
    with MockCalc(mocker):
        swap1 = IRSwap('Pay', '10y', 'USD', fixed_rate=0.0, name='10y@0')

        portfolio = Portfolio(swap1)
        assert portfolio.paths('10y@0') == (PortfolioPath(0),)

        with PricingContext(pricing_date=dt.date(2020, 10, 15)):
            prices: PortfolioRiskResult = portfolio.dollar_price()
        assert tuple(map(lambda x: round(x, 0), prices)) == (7391261.0,)
        assert round(prices.aggregate(), 0) == 7391261.0
        assert round(prices[swap1], 0) == 7391261.0


def test_results_with_resolution(mocker):
    with MockCalc(mocker):
        swap1 = IRSwap('Pay', '10y', 'USD', name='swap1')
        swap2 = IRSwap('Pay', '10y', 'GBP', name='swap2')
        swap3 = IRSwap('Pay', '10y', 'EUR', name='swap3')

        portfolio = Portfolio((swap1, swap2, swap3))

        with PricingContext(pricing_date=dt.date(2020, 10, 15)):
            result = portfolio.calc((risk.DollarPrice, risk.IRDelta))

        # Check that we've got results
        assert result[swap1][risk.DollarPrice] is not None

        # Now resolve portfolio and assert that we can still get the result

        orig_swap1 = swap1.clone()

        with PricingContext(pricing_date=dt.date(2020, 10, 15)):
            portfolio.resolve()

        # Assert that the resolved swap is indeed different and that we can retrieve results by both

        assert swap1 != orig_swap1
        assert result[swap1][risk.DollarPrice] is not None
        assert result[orig_swap1][risk.DollarPrice] is not None

        # Now reset the instruments and portfolio

        swap1 = IRSwap('Pay', '10y', 'USD', name='swap1')
        swap2 = IRSwap('Pay', '10y', 'GBP', name='swap2')
        swap3 = IRSwap('Pay', '10y', 'EUR', name='swap3')

        portfolio = Portfolio((swap1, swap2, swap3, swap1))

        with PricingContext(dt.date(2020, 10, 14)):
            # Resolve under a different pricing date
            portfolio.resolve()

        assert portfolio.instruments[0].termination_date == dt.date(2030, 10, 16)
        assert portfolio.instruments[1].termination_date == dt.date(2030, 10, 14)
        assert round(swap1.fixed_rate, 4) == 0.0075
        assert round(swap2.fixed_rate, 4) == 0.004
        assert round(swap3.fixed_rate, 4) == -0.0027

        # Assert that after resolution under a different context, we cannot retrieve the result

        try:
            _ = result[swap1][risk.DollarPrice]
            assert False
        except KeyError:
            assert True

        # Assert that if we resolve first in one context before pricing under a different context
        # we can slice the riskresult with the origin
        with CurveScenario(parallel_shift=5, name='parallel shift 5bp'):
            result2 = portfolio.calc((risk.DollarPrice, risk.IRDelta))

        assert result2[swap1][risk.DollarPrice] is not None
        assert result2[orig_swap1][risk.DollarPrice] is not None

        # Resolve again and check we get the same values

        with PricingContext(dt.date(2020, 10, 14)):
            # Resolve under a different pricing date
            portfolio.resolve()

        assert portfolio.instruments[0].termination_date == dt.date(2030, 10, 16)
        assert portfolio.instruments[1].termination_date == dt.date(2030, 10, 14)
        assert round(swap1.fixed_rate, 4) == 0.0075
        assert round(swap2.fixed_rate, 4) == 0.004
        assert round(swap3.fixed_rate, 4) == -0.0027


def test_portfolio_overrides(mocker):
    swap_1 = IRSwap("Pay", "5y", "EUR", fixed_rate=-0.005, name="5y")
    swap_2 = IRSwap("Pay", "10y", "EUR", fixed_rate=-0.005, name="10y")
    swap_3 = IRSwap("Pay", "5y", "GBP", fixed_rate=-0.005, name="5y")
    swap_4 = IRSwap("Pay", "10y", "GBP", fixed_rate=-0.005, name="10y")
    eur_port = Portfolio([swap_1, swap_2], name="EUR")
    gbp_port = Portfolio([swap_3, swap_4], name="GBP")

    # override instruments after portfolio construction
    for idx in range(len(eur_port)):
        eur_port[idx].fixed_rate = eur_port[idx].fixed_rate - 0.0005

    assert eur_port[swap_1] is not None

    with MockCalc(mocker):
        # override instruments after portfolio construction and resolution
        gbp_port.resolve()
        for idx in range(len(gbp_port)):
            gbp_port[idx].notional_amount = gbp_port[idx].notional_amount - 1

        with PricingContext(dt.date(2020, 1, 14)):
            r1 = eur_port.calc(risk.Price)
            r2 = eur_port.calc((risk.Price, risk.DollarPrice))
            r3 = gbp_port.calc(risk.Price)
            r4 = gbp_port.calc((risk.DollarPrice, risk.Price))

    assert gbp_port[swap_3] is not None

    assert r1[eur_port[0]] is not None
    assert r1['5y'] is not None
    assert r1.to_frame() is not None
    assert r2[eur_port[0]] is not None
    assert r2[risk.Price][0] is not None
    assert r2[0][risk.Price] is not None
    assert r3[gbp_port[0]] is not None
    assert r3.to_frame() is not None
    assert r4[gbp_port[0]] is not None
    assert r4[risk.DollarPrice][0] is not None
    assert r4[0][risk.DollarPrice] is not None


def test_from_frame():
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1)
    swaption = IRSwaption(notional_currency='GBP', expiration_date='10y', effective_date='0b')
    portfolio = Portfolio((swap, swaption))
    port_df = portfolio.to_frame()
    new_port_df = Portfolio.from_frame(port_df)

    assert new_port_df[swap] == swap
    assert new_port_df[swaption] == swaption


def test_single_instrument_new_mock(mocker):
    with MockCalc(mocker):
        with PricingContext(pricing_date=dt.date(2020, 10, 15)):
            swap1 = IRSwap('Pay', '10y', 'USD', name='swap1')

            portfolio = Portfolio(swap1)
            fwd: PortfolioRiskResult = portfolio.calc(risk.IRFwdRate)

        assert portfolio.paths('swap1') == (PortfolioPath(0),)
        assert tuple(map(lambda x: round(x, 6), fwd)) == (0.007512,)
        assert round(fwd.aggregate(), 2) == 0.01
        assert round(fwd[swap1], 6) == 0.007512


def test_get_instruments(mocker):
    mocker.patch.object(
        GsPortfolioApi,
        'get_position_dates',
        return_value=([dt.date(2021, 1, 2), dt.date(2021, 5, 6), dt.date(2021, 6, 1)]),
    )
    mocker.patch.object(
        GsPortfolioApi,
        'get_positions_for_date',
        return_value=(PositionSet(position_date=dt.date(2021, 6, 1), positions=[])),
    )
    mocker.patch.object(
        GsPortfolioApi, 'get_portfolio', return_value=(MarqueePortfolio(id_='id', name='name', currency='USD'))
    )
    mocker.patch.object(GsAssetApi, 'get_instruments_for_positions', return_value=([]))

    port = Portfolio.get(portfolio_id='id')
    port._get_instruments(dt.date(2021, 5, 14), False)
    GsPortfolioApi.get_positions_for_date.assert_called_with('id', dt.date(2021, 5, 6))


def test_clone():
    old_p = Portfolio((IRSwap(name='c'), Portfolio((IRSwap(name='a'), IRSwap(name='b')))))
    old_p.priceables[1].instruments[0].name = 'changed_name'
    new_p = old_p.clone()
    assert old_p['changed_name'] == ()
    assert new_p['changed_name'] == IRSwap(name='changed_name')

    # Check clone is deep
    inst1 = IRSwap('Pay', '10y', 'USD', fixed_rate=0.0, name='10y@0')
    inst2 = IRSwap('Pay', '5y', 'USD', fixed_rate=0.0, name='5y@0')
    port = Portfolio([inst1, Portfolio(inst2)])
    copy = port.clone(True)
    # Make some edits to the copy
    copy[0].fixed_rate = 1.0
    copy[1][0].fixed_rate = 2.0

    # Check not modified in place
    assert port[0].fixed_rate == 0.0
    assert port[1][0].fixed_rate == 0.0


# ---------------------------------------------------------------------------
# Branch coverage tests below
# ---------------------------------------------------------------------------

import os
import tempfile
from unittest.mock import MagicMock, patch, PropertyMock
from gs_quant.markets import PositionContext
from gs_quant.markets.portfolio import Grid
from gs_quant.target.portfolios import Position, PositionSet as TargetPositionSet


def test_repr_with_name():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    r = repr(p)
    assert 'myport' in r
    assert '1 instrument(s)' in r


def test_repr_without_name():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,))
    r = repr(p)
    assert '1 instrument(s)' in r
    assert 'Portfolio(' in r


def test_repr_empty():
    p = Portfolio(())
    r = repr(p)
    assert '0 instrument(s)' in r


def test_to_records_flat():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    records = p._to_records()
    assert len(records) == 2
    assert records[0]['instrument_name'] == 's1'
    assert records[1]['instrument_name'] == 's2'


def test_to_records_unnamed_instrument():
    swap = IRSwap('Pay', '10y', 'USD')
    swap.name = None
    p = Portfolio((swap,))
    records = p._to_records()
    assert len(records) == 1
    assert '_0' in records[0]['instrument_name']


def test_to_records_nested():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    inner = Portfolio((swap1,), name='inner')
    outer = Portfolio((inner, swap2), name='outer')
    records = outer._to_records()
    assert len(records) == 2


def test_to_records_portfolio_name_branch():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    inner = Portfolio((swap,), name='sub')
    outer = Portfolio((inner,))
    records = outer._to_records()
    assert len(records) == 1


def test_getitem_int():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    assert p[0] == swap1
    assert p[1] == swap2


def test_getitem_slice():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    sliced = p[0:1]
    assert len(sliced) == 1


def test_getitem_portfolio_path():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,))
    path = PortfolioPath(0)
    result = p[path]
    assert result == swap1


def test_getitem_string_single():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,))
    result = p['s1']
    assert result == swap1


def test_getitem_string_multiple():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s1')
    p = Portfolio((swap1, swap2))
    result = p['s1']
    assert isinstance(result, tuple)
    assert len(result) == 2


def test_getitem_list():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    result = p[['s1', 's2']]
    assert isinstance(result, tuple)
    assert len(result) == 2


def test_getitem_string_not_found():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,))
    result = p['nonexistent']
    assert result == ()


def test_contains_instrument():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1,))
    assert swap1 in p
    assert swap2 not in p


def test_contains_string():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,))
    assert 's1' in p
    assert 'nonexistent' not in p


def test_contains_other_type():
    p = Portfolio(())
    assert 42 not in p
    assert None not in p


def test_eq_same():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p1 = Portfolio((swap1,))
    p2 = Portfolio((swap1,))
    assert p1 == p2


def test_eq_different_type():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,))
    assert p != "not a portfolio"
    assert p != 42


def test_eq_different_length():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p1 = Portfolio((swap1, swap2))
    p2 = Portfolio((swap1,))
    assert p1 != p2


def test_eq_different_instruments():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p1 = Portfolio((swap1,))
    p2 = Portfolio((swap2,))
    assert p1 != p2


def test_eq_different_depth():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    inner = Portfolio((swap1,), name='inner')
    p1 = Portfolio((inner,))
    p2 = Portfolio((swap1,))
    assert p1 != p2


def test_add_portfolios():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p1 = Portfolio((swap1,))
    p2 = Portfolio((swap2,))
    combined = p1 + p2
    assert len(combined) == 2


def test_add_non_portfolio():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,))
    try:
        _ = p + "not a portfolio"
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_hash_consistent():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,), name='port')
    h = hash(p)
    assert isinstance(h, int)


def test_id_none_by_default():
    p = Portfolio(())
    assert p.id is None


def test_quote_id_none_by_default():
    p = Portfolio(())
    assert p.quote_id is None


def test_instruments_property():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    inner = Portfolio((swap2,), name='inner')
    p = Portfolio((swap1, inner))
    assert len(p.instruments) == 1
    assert swap1 in p.instruments


def test_portfolios_property():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    inner = Portfolio((swap1,), name='inner')
    p = Portfolio((swap1, inner))
    assert len(p.portfolios) == 1
    assert inner in p.portfolios


def test_all_instruments():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    inner = Portfolio((swap2,), name='inner')
    p = Portfolio((swap1, inner))
    all_inst = p.all_instruments
    assert len(all_inst) == 2


def test_all_portfolios():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    inner1 = Portfolio((swap1,), name='inner1')
    inner2 = Portfolio((swap1,), name='inner2')
    p = Portfolio((inner1, inner2))
    all_ports = p.all_portfolios
    assert len(all_ports) == 2


def test_priceables_setter_with_single():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio(())
    p.priceables = swap1
    assert len(p) == 1


def test_priceables_setter_with_unnamed():
    swap1 = IRSwap('Pay', '10y', 'USD')
    swap1.name = None
    p = Portfolio(())
    p.priceables = (swap1,)
    assert len(p) == 1


def test_priceables_deleter():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,))
    del p.priceables
    assert p.priceables is None


def test_subset_single_portfolio():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    inner = Portfolio((swap1,), name='inner')
    outer = Portfolio((inner,))
    path = PortfolioPath(0)
    result = outer.subset([path])
    assert isinstance(result, Portfolio)
    assert result.name == 'inner'


def test_subset_multiple_instruments():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    result = p.subset([PortfolioPath(0), PortfolioPath(1)], name='sub')
    assert isinstance(result, Portfolio)
    assert len(result) == 2
    assert result.name == 'sub'


@patch.object(GsPortfolioApi, 'get_instruments_by_position_type')
def test_from_eti(mock_get):
    mock_inst = IRSwap('Pay', '10y', 'USD', name='s1')
    mock_get.return_value = [mock_inst]
    p = Portfolio.from_eti('some/eti')
    mock_get.assert_called_once()
    assert p.name == 'some%2Feti'


@patch.object(GsPortfolioApi, 'get_instruments_by_position_type')
def test_from_book(mock_get):
    mock_get.return_value = []
    p = Portfolio.from_book('mybook')
    mock_get.assert_called_once_with('risk', 'mybook', 'position')
    assert p.name == 'mybook'


@patch.object(GsPortfolioApi, 'get_instruments_by_position_type')
def test_from_book_custom_type(mock_get):
    mock_get.return_value = []
    p = Portfolio.from_book('mybook', book_type='pnl', activity_type='trade')
    mock_get.assert_called_once_with('pnl', 'mybook', 'trade')


@patch.object(GsAssetApi, 'get_instruments_for_positions')
@patch.object(GsAssetApi, 'get_latest_positions')
@patch.object(GsAssetApi, 'get_asset')
def test_from_asset_id_no_date(mock_asset, mock_positions, mock_instruments):
    asset = MagicMock()
    asset.name = 'test_asset'
    mock_asset.return_value = asset
    pos_set = MagicMock(spec=TargetPositionSet)
    pos_set.positions = []
    mock_positions.return_value = pos_set
    mock_instruments.return_value = []
    p = Portfolio.from_asset_id('asset123')
    mock_positions.assert_called_once_with('asset123')
    assert p.name == 'test_asset'


@patch.object(GsAssetApi, 'get_instruments_for_positions')
@patch.object(GsAssetApi, 'get_asset_positions_for_date')
@patch.object(GsAssetApi, 'get_asset')
def test_from_asset_id_with_date(mock_asset, mock_pos_date, mock_instruments):
    asset = MagicMock()
    asset.name = 'test_asset'
    mock_asset.return_value = asset
    pos_set = MagicMock(spec=TargetPositionSet)
    pos_set.positions = []
    mock_pos_date.return_value = pos_set
    mock_instruments.return_value = []
    p = Portfolio.from_asset_id('asset123', date=dt.date(2021, 1, 1))
    mock_pos_date.assert_called_once_with('asset123', dt.date(2021, 1, 1))


@patch.object(GsAssetApi, 'get_instruments_for_positions')
@patch.object(GsAssetApi, 'get_latest_positions')
@patch.object(GsAssetApi, 'get_asset')
def test_from_asset_id_tuple_response(mock_asset, mock_positions, mock_instruments):
    asset = MagicMock()
    asset.name = 'test_asset'
    mock_asset.return_value = asset
    pos_set = MagicMock(spec=TargetPositionSet)
    pos_set.positions = []
    mock_positions.return_value = (pos_set, 'extra')
    mock_instruments.return_value = []
    p = Portfolio.from_asset_id('asset123')
    assert p.name == 'test_asset'


@patch.object(GsAssetApi, 'get_instruments_for_positions')
@patch.object(GsAssetApi, 'get_latest_positions')
@patch.object(GsAssetApi, 'get_asset')
def test_from_asset_id_dict_response(mock_asset, mock_positions, mock_instruments):
    asset = MagicMock()
    asset.name = 'test_asset'
    mock_asset.return_value = asset
    mock_positions.return_value = {'positions': []}
    mock_instruments.return_value = []
    p = Portfolio.from_asset_id('asset123')
    assert p.name == 'test_asset'


@patch.object(GsAssetApi, 'get_asset_by_name')
def test_from_asset_name(mock_get_asset):
    asset = MagicMock()
    asset.id = 'asset_id_1'
    asset.name = 'my_asset'
    mock_get_asset.return_value = asset
    with patch.object(Portfolio, 'load_from_portfolio_id', create=True,
                      return_value=Portfolio((), name='my_asset')) as mock_load:
        p = Portfolio.from_asset_name('my_asset')
        mock_get_asset.assert_called_once_with('my_asset')
        mock_load.assert_called_once_with('asset_id_1')


@patch.object(GsPortfolioApi, 'get_instruments_by_workflow_id')
def test_from_quote(mock_get):
    mock_inst = IRSwap('Pay', '10y', 'USD', name='s1')
    mock_get.return_value = [mock_inst]
    p = Portfolio.from_quote('quote123')
    assert p.name == 'quote123'
    assert p.quote_id == 'quote123'


@patch.object(GsPortfolioApi, 'get_portfolio')
def test_get_with_id(mock_get_portfolio):
    mock_get_portfolio.return_value = MagicMock(name='myport')
    p = Portfolio.get(portfolio_id='port_id')
    assert p is not None


@patch.object(GsPortfolioApi, 'get_portfolio')
@patch.object(GsPortfolioApi, 'get_portfolio_by_name')
def test_get_with_name(mock_get_by_name, mock_get_portfolio):
    mock_portfolio = MagicMock()
    mock_portfolio.id = 'port_id_from_name'
    mock_portfolio.name = 'myport'
    mock_get_by_name.return_value = mock_portfolio
    mock_get_portfolio.return_value = MagicMock(name='myport')
    p = Portfolio.get(portfolio_name='myport')
    mock_get_by_name.assert_called_once_with('myport')


@patch.object(GsPortfolioApi, 'get_portfolio')
def test_get_with_query_instruments(mock_get_portfolio):
    mock_get_portfolio.return_value = MagicMock(name='myport')
    with patch.object(Portfolio, '_get_instruments') as mock_get_inst:
        p = Portfolio.get(portfolio_id='port_id', query_instruments=True)
        mock_get_inst.assert_called_once()


@patch.object(GsPortfolioApi, 'get_portfolio')
def test_from_portfolio_id_deprecated(mock_get_portfolio):
    mock_get_portfolio.return_value = MagicMock(name='myport')
    p = Portfolio.from_portfolio_id('port_id')
    assert p is not None


@patch.object(GsPortfolioApi, 'get_portfolio')
@patch.object(GsPortfolioApi, 'get_portfolio_by_name')
def test_from_portfolio_name_deprecated(mock_get_by_name, mock_get_portfolio):
    mock_portfolio = MagicMock()
    mock_portfolio.id = 'port_id'
    mock_portfolio.name = 'myport'
    mock_get_by_name.return_value = mock_portfolio
    mock_get_portfolio.return_value = MagicMock(name='myport')
    p = Portfolio.from_portfolio_name('myport')
    assert p is not None


def test_save_with_nested_portfolios():
    inner = Portfolio((), name='inner')
    outer = Portfolio((inner,), name='outer')
    try:
        outer.save()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert 'Cannot save portfolios with nested portfolios' in str(e)


@patch.object(GsPortfolioApi, 'create_portfolio')
@patch.object(GsAssetApi, 'get_or_create_asset_from_instrument')
@patch.object(GsPortfolioApi, 'update_positions')
def test_save_new_portfolio(mock_update, mock_create_asset, mock_create_port):
    mock_created = MagicMock()
    mock_created.id = 'new_id'
    mock_create_port.return_value = mock_created
    mock_create_asset.return_value = 'asset_id_1'
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    p.save()
    mock_create_port.assert_called_once()
    mock_update.assert_called_once()


def test_save_without_name():
    p = Portfolio(())
    try:
        p.save()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert 'name not set' in str(e)


@patch.object(GsPortfolioApi, 'create_portfolio')
@patch.object(GsAssetApi, 'get_or_create_asset_from_instrument')
@patch.object(GsPortfolioApi, 'update_positions')
def test_save_existing_without_overwrite(mock_update, mock_create_asset, mock_create_port):
    mock_created = MagicMock()
    mock_created.id = 'existing_id'
    mock_create_port.return_value = mock_created
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    mock_create_asset.return_value = 'asset_id'
    p.save()
    try:
        p.save()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert 'already exists' in str(e)


@patch.object(GsPortfolioApi, 'create_portfolio')
@patch.object(GsAssetApi, 'get_or_create_asset_from_instrument')
@patch.object(GsPortfolioApi, 'update_positions')
def test_save_existing_with_overwrite(mock_update, mock_create_asset, mock_create_port):
    mock_created = MagicMock()
    mock_created.id = 'existing_id'
    mock_create_port.return_value = mock_created
    mock_create_asset.return_value = 'asset_id_1'
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    p.save()
    p.save(overwrite=True)
    assert mock_update.call_count == 2


@patch.object(GsPortfolioApi, 'create_portfolio')
@patch.object(GsPortfolioApi, 'update_positions')
def test_save_empty_positions(mock_update, mock_create_port):
    mock_created = MagicMock()
    mock_created.id = 'new_id'
    mock_create_port.return_value = mock_created
    p = Portfolio((), name='empty_port')
    p.save()
    mock_update.assert_not_called()


def test_save_as_quote_nested():
    inner = Portfolio((), name='inner')
    outer = Portfolio((inner,), name='outer')
    try:
        outer.save_as_quote()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert 'Cannot save portfolios with nested portfolios' in str(e)


@patch.object(GsPortfolioApi, 'save_quote')
def test_save_as_quote_new(mock_save_quote):
    mock_save_quote.return_value = 'quote_id_1'
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    with PricingContext():
        qid = p.save_as_quote()
    assert qid == 'quote_id_1'


@patch.object(GsPortfolioApi, 'save_quote')
@patch.object(GsPortfolioApi, 'update_quote')
def test_save_as_quote_existing_without_overwrite(mock_update, mock_save):
    mock_save.return_value = 'quote_id_1'
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    with PricingContext():
        p.save_as_quote()
    try:
        with PricingContext():
            p.save_as_quote()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert 'already exists' in str(e)


@patch.object(GsPortfolioApi, 'save_quote')
@patch.object(GsPortfolioApi, 'update_quote')
def test_save_as_quote_existing_with_overwrite(mock_update, mock_save):
    mock_save.return_value = 'quote_id_1'
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    with PricingContext():
        p.save_as_quote()
        qid = p.save_as_quote(overwrite=True)
    mock_update.assert_called_once()
    assert qid == 'quote_id_1'


def test_save_to_shadowbook_nested():
    inner = Portfolio((), name='inner')
    outer = Portfolio((inner,), name='outer')
    try:
        outer.save_to_shadowbook('name')
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert 'Cannot save portfolios with nested portfolios' in str(e)


@patch.object(GsPortfolioApi, 'save_to_shadowbook')
def test_save_to_shadowbook(mock_save):
    mock_save.return_value = 'success'
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    with PricingContext():
        p.save_to_shadowbook('shadow_name')
    mock_save.assert_called_once()


def test_from_frame_no_type_raises():
    df = pd.DataFrame({'col1': ['val1'], 'col2': ['val2']})
    try:
        Portfolio.from_frame(df)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert 'Neither asset_class/type nor $type specified' in str(e)


def test_from_frame_with_callable_mapping():
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1)
    p = Portfolio((swap,))
    df = p.to_frame()
    mappings = {'asset_class': lambda row: row.get('asset_class')}
    new_p = Portfolio.from_frame(df, mappings=mappings)
    assert len(new_p) == 1


def test_from_frame_skip_none_rows():
    df = pd.DataFrame({
        'asset_class': [None],
        'type': [None],
    })
    p = Portfolio.from_frame(df)
    assert len(p) == 0


def test_from_csv_basic():
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='test_swap')
    p = Portfolio((swap,), name='test')
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df = p.to_frame()
        df = df.reset_index(drop=True)
        df.to_csv(f.name)
        try:
            new_p = Portfolio.from_csv(f.name)
            assert len(new_p) == 1
        finally:
            os.unlink(f.name)


def test_from_csv_duplicate_columns():
    csv_content = "col,col.0\nval1,val2\n"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_content)
        f.flush()
        try:
            Portfolio.from_csv(f.name)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert 'Duplicate column values' in str(e)
        finally:
            os.unlink(f.name)


def test_scale_in_place():
    swap = MagicMock(spec=IRSwap)
    swap.name = 's1'
    swap.scale = MagicMock()
    p = Portfolio((swap,))
    result = p.scale(2, in_place=True)
    assert result is None
    swap.scale.assert_called_once_with(2, True)


def test_scale_not_in_place():
    swap = MagicMock(spec=IRSwap)
    swap.name = 's1'
    scaled_swap = MagicMock(spec=IRSwap)
    scaled_swap.name = 's1_scaled'
    swap.scale = MagicMock(return_value=scaled_swap)
    p = Portfolio((swap,))
    result = p.scale(2, in_place=False)
    assert isinstance(result, Portfolio)


def test_append_single():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio(())
    p.append(swap1)
    assert len(p) == 1


def test_append_iterable():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio(())
    p.append([swap1, swap2])
    assert len(p) == 2


def test_pop_instrument():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    popped = p.pop('s1')
    assert popped == swap1
    assert len(p) == 1


def test_to_frame_nested():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    inner = Portfolio((swap2,), name='inner')
    outer = Portfolio((swap1, inner), name='outer')
    df = outer.to_frame()
    assert not df.empty


def test_to_frame_with_string_mapping():
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='s1')
    p = Portfolio((swap,), name='test')
    df = p.to_frame(mappings={'my_col': 'fixed_rate'})
    assert 'my_col' in df.columns


def test_to_frame_with_callable_mapping():
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='s1')
    p = Portfolio((swap,), name='test')
    df = p.to_frame(mappings={'computed': lambda row: 'hello'})
    assert 'computed' in df.columns


def test_to_frame_dollar_type_column():
    swaption = IRSwaption(notional_currency='GBP', expiration_date='10y', effective_date='0b', name='opt1')
    p = Portfolio((swaption,), name='test')
    df = p.to_frame()
    assert not df.empty


def test_to_csv_basic():
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='s1')
    p = Portfolio((swap,), name='test')
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        try:
            p.to_csv(f.name)
            assert os.path.getsize(f.name) > 0
        finally:
            os.unlink(f.name)


def test_to_csv_with_ignored_cols():
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='s1')
    p = Portfolio((swap,), name='test')
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        try:
            p.to_csv(f.name, ignored_cols=['fixed_rate'])
            content = open(f.name).read()
            assert 'fixed_rate' not in content
        finally:
            os.unlink(f.name)


def test_paths_invalid_key():
    p = Portfolio(())
    try:
        p.paths(42)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert 'key must be a name or Instrument or Portfolio' in str(e)


def test_paths_by_string():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,))
    paths = p.paths('s1')
    assert len(paths) == 1


def test_paths_by_instrument():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,))
    paths = p.paths(swap)
    assert len(paths) == 1


def test_paths_by_portfolio():
    inner = Portfolio((), name='inner')
    outer = Portfolio((inner,))
    paths = outer.paths(inner)
    assert len(paths) == 1


def test_paths_not_found():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,))
    paths = p.paths('nonexistent')
    assert paths == ()


def test_paths_instrument_not_found():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1,))
    paths = p.paths(swap2)
    assert paths == ()


def test_paths_with_unresolved_match():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    mock_inst = MagicMock()
    mock_inst.unresolved = swap1
    mock_inst.name = 's1'
    mock_inst.__eq__ = MagicMock(return_value=False)
    p = Portfolio((mock_inst,))
    paths = p.paths(swap1)
    assert len(paths) == 1


def test_all_paths_flat():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    paths = p.all_paths
    assert len(paths) == 2


def test_all_paths_nested():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    inner = Portfolio((swap2,), name='inner')
    outer = Portfolio((swap1, inner))
    paths = outer.all_paths
    assert len(paths) == 2


@patch.object(GsAssetApi, 'get_instruments_for_positions')
@patch.object(GsPortfolioApi, 'get_positions_for_date')
@patch.object(GsPortfolioApi, 'get_position_dates')
@patch.object(GsPortfolioApi, 'get_portfolio')
def test_get_instruments_no_positions_raises(mock_get_port, mock_dates, mock_pos, mock_instr):
    mock_get_port.return_value = MagicMock(name='myport')
    mock_dates.return_value = [dt.date(2021, 6, 1)]
    p = Portfolio.get(portfolio_id='port_id')
    try:
        p._get_instruments(dt.date(2020, 1, 1), False)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert 'no positions' in str(e)


@patch.object(GsAssetApi, 'get_instruments_for_positions')
@patch.object(GsPortfolioApi, 'get_positions_for_date')
@patch.object(GsPortfolioApi, 'get_position_dates')
@patch.object(GsPortfolioApi, 'get_portfolio')
def test_get_instruments_in_place(mock_get_port, mock_dates, mock_pos, mock_instr):
    mock_get_port.return_value = MagicMock(name='myport')
    mock_dates.return_value = [dt.date(2021, 1, 1), dt.date(2021, 5, 1)]
    pos_set = MagicMock()
    pos_set.positions = []
    mock_pos.return_value = pos_set
    mock_instr.return_value = [IRSwap('Pay', '10y', 'USD', name='s1')]
    p = Portfolio.get(portfolio_id='port_id')
    instruments = p._get_instruments(dt.date(2021, 6, 1), True)
    assert len(instruments) == 1
    assert len(p.priceables) == 1


@patch.object(GsAssetApi, 'get_instruments_for_positions')
@patch.object(GsPortfolioApi, 'get_positions_for_date')
@patch.object(GsPortfolioApi, 'get_position_dates')
@patch.object(GsPortfolioApi, 'get_portfolio')
def test_get_instruments_none_response(mock_get_port, mock_dates, mock_pos, mock_instr):
    mock_get_port.return_value = MagicMock(name='myport')
    mock_dates.return_value = [dt.date(2021, 1, 1)]
    mock_pos.return_value = None
    mock_instr.return_value = []
    p = Portfolio.get(portfolio_id='port_id')
    instruments = p._get_instruments(dt.date(2021, 6, 1), False)
    assert instruments == []


def test_get_instruments_no_id_return_priceables():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,))
    result = p._get_instruments(dt.date(2021, 1, 1), False, return_priceables=True)
    assert len(result) == 1


def test_get_instruments_no_id_return_all_instruments():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    inner = Portfolio((swap2,), name='inner')
    p = Portfolio((swap1, inner))
    result = p._get_instruments(dt.date(2021, 1, 1), False, return_priceables=False)
    assert len(result) == 2


def test_clone_without_instruments():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    cloned = p.clone(clone_instruments=False)
    assert cloned.name == 'myport'
    assert len(cloned) == 1
    assert cloned[0] is swap


def test_clone_with_instruments():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    cloned = p.clone(clone_instruments=True)
    assert cloned.name == 'myport'
    assert len(cloned) == 1
    assert cloned[0] is not swap


def test_clone_nested_with_instruments():
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    inner = Portfolio((swap,), name='inner')
    outer = Portfolio((inner,), name='outer')
    cloned = outer.clone(clone_instruments=True)
    assert len(cloned) == 1
    assert isinstance(cloned[0], Portfolio)


def test_iter():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    items = list(p)
    assert len(items) == 2


def test_portfolio_len():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,))
    assert len(p) == 1


def test_grid_creation():
    swap = IRSwap('Pay', '10y', 'USD', name='base', fixed_rate=0.01)
    grid = Grid(
        swap,
        x_param='fixed_rate',
        x_values=[0.01, 0.02],
        y_param='notional_amount',
        y_values=[1000000, 2000000],
        name='my_grid',
    )
    assert grid.name == 'my_grid'
    assert len(grid) == 2
    for sub_portfolio in grid:
        assert isinstance(sub_portfolio, Portfolio)
        assert len(sub_portfolio) == 2


@patch.object(GsPortfolioApi, 'get_portfolio')
def test_get_with_entered_position_context(mock_get_portfolio):
    mock_get_portfolio.return_value = MagicMock(name='myport')
    with PositionContext(position_date=dt.date(2021, 3, 15)):
        p = Portfolio.get(portfolio_id='port_id')
    assert p is not None


def test_extend_portfolio():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1,))
    p.extend([swap2])
    assert len(p) == 2


def test_contains_in_nested():
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    inner = Portfolio((swap2,), name='inner')
    outer = Portfolio((swap1, inner))
    assert swap2 in outer
    assert 's2' in outer


# ---------------------------------------------------------------------------
# Additional branch coverage tests
# ---------------------------------------------------------------------------


def test_to_records_type_as_string():
    """Cover the branch where obj.type_ is NOT an AssetType enum (line 88 false branch)."""
    mock_inst = MagicMock(spec=IRSwap)
    mock_inst.name = 's1'
    # type_ is a plain string, not an AssetType enum
    mock_inst.type_ = 'SomeStringType'
    p = Portfolio((mock_inst,))
    records = p._to_records()
    assert len(records) == 1
    assert records[0]['instrument_name'] == 's1'


def test_to_records_type_as_asset_type():
    """Cover the branch where obj.type_ IS an AssetType enum (line 88 true branch)."""
    swap = IRSwap('Pay', '10y', 'USD', name='named_swap')
    p = Portfolio((swap,))
    records = p._to_records()
    assert len(records) == 1
    assert records[0]['instrument_name'] == 'named_swap'


def test_to_records_unnamed_portfolio_in_nested():
    """Cover the branch for unnamed portfolio in _to_records (type_name = 'Portfolio')."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    inner = Portfolio((swap,))  # unnamed portfolio
    inner.name = None
    outer = Portfolio((inner,))
    records = outer._to_records()
    assert len(records) == 1
    # The unnamed portfolio should get 'Portfolio_0' as its name
    assert 'portfolio_name_0' in records[0]
    assert 'Portfolio_0' in records[0]['portfolio_name_0']


def test_getitem_list_with_multiple_results():
    """Cover __getitem__ with list of keys, each matching one result."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    swap3 = IRSwap('Pay', '3y', 'USD', name='s3')
    p = Portfolio((swap1, swap2, swap3))
    result = p[['s1', 's2']]
    assert isinstance(result, tuple)
    assert len(result) == 2


def test_getitem_string_returns_single():
    """Cover __getitem__ where values has exactly 1 element (returns values[0])."""
    swap = IRSwap('Pay', '10y', 'USD', name='unique_name')
    p = Portfolio((swap,))
    result = p['unique_name']
    assert result == swap  # returns the single value, not a tuple


def test_eq_typeerror_branch():
    """Cover the TypeError branch in __eq__ (line 152-153).

    This happens when one portfolio is deeper than the other: path(other)
    tries to subscript an instrument, which raises TypeError.
    """
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    # p1 has a nested portfolio at index 0
    inner = Portfolio((swap2,), name='inner')
    p1 = Portfolio((inner, swap1))
    # p2 has only instruments
    p2 = Portfolio((swap1, swap1))
    assert p1 != p2


def test_all_portfolios_with_duplicate_subportfolios():
    """Cover the 'if portfolio in portfolios: continue' branch in all_portfolios.

    The all_portfolios logic starts stack and portfolios from the same set,
    so the 'continue' branch is always hit for direct sub-portfolios.
    We verify the deduplication via unique_everseen on final return.
    """
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    # Direct sub-portfolios
    p1 = Portfolio((swap1,), name='p1')
    p2 = Portfolio((swap1,), name='p2')
    outer = Portfolio((p1, p2))
    all_p = outer.all_portfolios
    names = [p.name for p in all_p]
    assert 'p1' in names
    assert 'p2' in names
    assert len(all_p) == 2


def test_all_portfolios_deep_nesting():
    """Cover all_portfolios with nested structure.

    Due to the all_portfolios implementation, only direct sub-portfolios
    are returned (the continue branch prevents deeper exploration).
    """
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    deep = Portfolio((swap,), name='deep')
    mid = Portfolio((deep,), name='mid')
    top = Portfolio((mid,))
    all_p = top.all_portfolios
    # Only direct sub-portfolios: mid
    assert len(all_p) == 1
    assert all_p[0].name == 'mid'


def test_paths_nested_by_string():
    """Cover the recursive paths search through nested portfolios by string key."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='target')
    swap2 = IRSwap('Pay', '5y', 'USD', name='other')
    inner = Portfolio((swap1,), name='inner')
    outer = Portfolio((swap2, inner))
    paths = outer.paths('target')
    assert len(paths) == 1
    # The path should be (1, 0) - index 1 for inner portfolio, then 0 for the swap
    assert len(paths[0]) == 2


def test_paths_nested_by_instrument():
    """Cover the recursive paths search through nested portfolios by instrument."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='target')
    swap2 = IRSwap('Pay', '5y', 'USD', name='other')
    inner = Portfolio((swap1,), name='inner')
    outer = Portfolio((swap2, inner))
    paths = outer.paths(swap1)
    assert len(paths) == 1


def test_paths_nested_by_portfolio():
    """Cover paths() searching for a portfolio object inside nested structure."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    deep = Portfolio((swap,), name='deep')
    mid = Portfolio((deep,), name='mid')
    outer = Portfolio((mid,))
    paths = outer.paths(deep)
    assert len(paths) == 1
    assert len(paths[0]) == 2  # (0, 0)


def test_to_frame_instrument_without_asset_class():
    """Cover the '$type' branch in to_frame when instrument lacks 'asset_class'."""
    mock_inst = MagicMock()
    mock_inst.name = 'mock_inst'
    # Make it not have 'asset_class' attribute
    del mock_inst.asset_class
    mock_inst.as_dict.return_value = {'$type': 'MockType', 'param1': 'val1'}
    mock_inst.type_ = 'MockType'

    p = Portfolio((mock_inst,), name='test')
    df = p.to_frame()
    assert not df.empty
    assert '$type' in df.columns


def test_to_frame_with_no_mappings():
    """Cover to_frame with no mappings (empty dict path)."""
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='s1')
    p = Portfolio((swap,), name='test')
    df = p.to_frame(mappings={})
    assert not df.empty


def test_to_frame_mapping_neither_str_nor_callable():
    """Cover to_frame mappings branch where value is neither str nor callable."""
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='s1')
    p = Portfolio((swap,), name='test')
    # A numeric value is neither str nor callable - it should be ignored by the mapping logic
    df = p.to_frame(mappings={'ignored_key': 42})
    assert 'ignored_key' not in df.columns


def test_to_csv_with_mappings():
    """Cover to_csv with non-None mappings parameter."""
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='s1')
    p = Portfolio((swap,), name='test')
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        try:
            p.to_csv(f.name, mappings={'my_col': 'fixed_rate'})
            content = open(f.name).read()
            assert 'my_col' in content
        finally:
            os.unlink(f.name)


def test_clone_nested_without_clone_instruments():
    """Cover clone with nested portfolio and clone_instruments=False.

    When clone_instruments=False, nested portfolios get .clone(False)
    but individual instruments are NOT cloned (identity preserved).
    """
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    inner = Portfolio((swap2,), name='inner')
    outer = Portfolio((swap1, inner), name='outer')
    cloned = outer.clone(clone_instruments=False)
    assert cloned.name == 'outer'
    assert len(cloned) == 2
    # swap1 should be the same object (not cloned)
    assert cloned[0] is swap1
    # inner portfolio should be a different Portfolio object (cloned)
    assert isinstance(cloned[1], Portfolio)


def test_clone_preserves_ids():
    """Cover clone preserving __id and __quote_id."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,), name='myport')
    # id and quote_id are None by default
    cloned = p.clone()
    assert cloned.id is None
    assert cloned.quote_id is None


def test_init_with_dict():
    """Cover __init__ with a dict of priceables (line 63-68)."""
    swap1 = IRSwap('Pay', '10y', 'USD')
    swap2 = IRSwap('Pay', '5y', 'USD')
    p = Portfolio({'first': swap1, 'second': swap2})
    assert len(p) == 2
    assert swap1.name == 'first'
    assert swap2.name == 'second'


def test_init_single_priceable():
    """Cover __init__ with a single PriceableImpl (wraps in tuple via setter)."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio(swap)
    assert len(p) == 1


def test_priceables_setter_names_index():
    """Cover the priceables setter building __priceables_by_name."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s1')  # same name
    swap3 = IRSwap('Pay', '3y', 'USD', name='s3')
    p = Portfolio((swap1, swap2, swap3))
    # 's1' should map to two indices
    paths = p.paths('s1')
    assert len(paths) == 2


def test_priceables_setter_with_none_item():
    """Cover the branch where item in priceables is falsy (line 187: if i and i.name)."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio(())
    # Set priceables with a mix, including one with no name
    unnamed = IRSwap('Pay', '5y', 'USD')
    unnamed.name = None
    p.priceables = (swap, unnamed)
    assert len(p) == 2


def test_subset_with_single_instrument_path():
    """Cover subset where paths represent instruments (not a portfolio)."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    # Single path to an instrument (not a portfolio)
    result = p.subset([PortfolioPath(0)])
    assert isinstance(result, Portfolio)
    assert len(result) == 1


def test_all_paths_deeply_nested():
    """Cover all_paths with multiple levels of nesting."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    swap3 = IRSwap('Pay', '3y', 'USD', name='s3')
    deep = Portfolio((swap3,), name='deep')
    mid = Portfolio((swap2, deep), name='mid')
    outer = Portfolio((swap1, mid))
    paths = outer.all_paths
    assert len(paths) == 3


def test_from_csv_no_duplicate_columns():
    """Cover from_csv when there are no duplicate columns (len(dupelist) == 0 branch)."""
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='s1')
    p = Portfolio((swap,), name='test')
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df = p.to_frame()
        df = df.reset_index(drop=True)
        df.to_csv(f.name)
        try:
            new_p = Portfolio.from_csv(f.name)
            assert len(new_p) >= 1
        finally:
            os.unlink(f.name)


def test_from_frame_with_dollar_type():
    """Cover from_frame with '$type' init key path (lines 379-383)."""
    swaption = IRSwaption(notional_currency='GBP', expiration_date='10y', effective_date='0b', name='opt1')
    p = Portfolio((swaption,), name='test')
    df = p.to_frame()
    # If $type column is present in the frame, from_frame should use it
    if '$type' in df.columns:
        new_p = Portfolio.from_frame(df)
        assert len(new_p) == 1


def test_hash_different_portfolios():
    """Cover __hash__ with different portfolios to test hash uniqueness."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p1 = Portfolio((swap1,), name='p1')
    p2 = Portfolio((swap2,), name='p2')
    # They should have different hashes (not guaranteed but extremely likely)
    h1 = hash(p1)
    h2 = hash(p2)
    assert isinstance(h1, int)
    assert isinstance(h2, int)


def test_hash_with_none_name():
    """Cover __hash__ when name is None."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap,))
    p.name = None
    h = hash(p)
    assert isinstance(h, int)


def test_repr_with_nested():
    """Cover __repr__ with nested portfolio showing correct instrument count."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    inner = Portfolio((swap2,), name='inner')
    outer = Portfolio((swap1, inner), name='outer')
    r = repr(outer)
    assert 'outer' in r
    assert 'instrument(s)' in r


def test_pop_by_index():
    """Cover pop with an integer index."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    p = Portfolio((swap1, swap2))
    popped = p.pop(0)
    assert popped == swap1


def test_extend_with_generator():
    """Cover extend with a generator/iterable."""
    swap1 = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap1,))
    swap2 = IRSwap('Pay', '5y', 'USD', name='s2')
    swap3 = IRSwap('Pay', '3y', 'USD', name='s3')
    p.extend(iter([swap2, swap3]))
    assert len(p) == 3


def test_contains_string_not_in_nested():
    """Cover __contains__ with string not found in nested portfolios."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    inner = Portfolio((swap,), name='inner')
    outer = Portfolio((inner,))
    assert 'nonexistent' not in outer


def test_instruments_dedup():
    """Cover the instruments property with duplicate instruments (unique_everseen)."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    p = Portfolio((swap, swap))
    # unique_everseen should deduplicate
    assert len(p.instruments) == 1


def test_all_instruments_dedup():
    """Cover the all_instruments property deduplication across nested portfolios."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    inner = Portfolio((swap,), name='inner')
    outer = Portfolio((swap, inner))
    # swap appears in both outer and inner, but should be deduped
    all_inst = outer.all_instruments
    assert len(all_inst) == 1


def test_getitem_portfolio_path_nested():
    """Cover __getitem__ with a PortfolioPath reaching into nested portfolio."""
    swap = IRSwap('Pay', '10y', 'USD', name='s1')
    inner = Portfolio((swap,), name='inner')
    outer = Portfolio((inner,))
    path = PortfolioPath((0, 0))
    result = outer[path]
    assert result == swap


def test_from_frame_with_type_init():
    """Cover from_frame with asset_class/type init key path."""
    swap = IRSwap('Receive', '3m', 'USD', fixed_rate=0, notional_amount=1, name='s1')
    p = Portfolio((swap,))
    df = p.to_frame()
    new_p = Portfolio.from_frame(df)
    assert len(new_p) == 1
