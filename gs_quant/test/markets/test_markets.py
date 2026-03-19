"""
Copyright 2024 Goldman Sachs.
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
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from gs_quant.base import RiskKey
from gs_quant.common import PricingLocation


# ===========================================================================
# Tests for historical_risk_key
# ===========================================================================

class TestHistoricalRiskKey:
    def test_historical_risk_key(self):
        from gs_quant.markets.markets import historical_risk_key, LocationOnlyMarket

        market = MagicMock()
        market.location = PricingLocation.NYC
        rk = RiskKey(
            provider='prov',
            date=dt.date(2024, 1, 1),
            market=market,
            params='params',
            scenario='scenario',
            risk_measure='measure'
        )
        result = historical_risk_key(rk)
        assert result.date is None
        assert isinstance(result.market, LocationOnlyMarket)
        assert result.provider == 'prov'
        assert result.params == 'params'
        assert result.scenario == 'scenario'
        assert result.risk_measure == 'measure'


# ===========================================================================
# Tests for market_location
# ===========================================================================

class TestMarketLocation:
    def test_market_location_none_with_default(self):
        from gs_quant.markets.markets import market_location

        mock_ctx = MagicMock()
        mock_ctx.market_data_location = PricingLocation.NYC
        # PricingContext is imported locally from .core inside market_location
        with patch('gs_quant.markets.core.PricingContext') as mock_pc:
            mock_pc.current = mock_ctx
            result = market_location(None)
            assert result == PricingLocation.NYC

    def test_market_location_none_no_default(self):
        from gs_quant.markets.markets import market_location

        mock_ctx = MagicMock()
        mock_ctx.market_data_location = None
        with patch('gs_quant.markets.core.PricingContext') as mock_pc:
            mock_pc.current = mock_ctx
            result = market_location(None)
            assert result == PricingLocation.LDN

    def test_market_location_explicit(self):
        from gs_quant.markets.markets import market_location

        # When location is not None, it's returned directly without checking PricingContext
        result = market_location(PricingLocation.TKO)
        assert result == PricingLocation.TKO


# ===========================================================================
# Tests for close_market_date
# ===========================================================================

class TestCloseMarketDate:
    def test_close_market_date_before_roll(self):
        from gs_quant.markets.markets import close_market_date

        test_date = dt.date(2024, 3, 20)
        mock_ctx = MagicMock()
        mock_ctx.pricing_date = test_date

        # Mock datetime.now to return a time before the roll time
        early_time = dt.datetime(2024, 3, 20, 5, 0, 0)
        mock_now = MagicMock()
        mock_now.astimezone.return_value.replace.return_value = early_time

        with patch('gs_quant.markets.core.PricingContext') as mock_pc, \
             patch('gs_quant.markets.markets.dt') as mock_dt_module, \
             patch('gs_quant.markets.markets.prev_business_date', return_value=dt.date(2024, 3, 19)) as mock_pbd:
            mock_pc.current = mock_ctx
            mock_dt_module.datetime.now.return_value = mock_now
            # dt.datetime() constructor and dt.timedelta need to work normally
            mock_dt_module.datetime.side_effect = lambda *a, **kw: dt.datetime(*a, **kw)
            mock_dt_module.timedelta = dt.timedelta
            result = close_market_date(PricingLocation.LDN, test_date, (24, 0))
            mock_pbd.assert_called_once()
            assert result == dt.date(2024, 3, 19)

    def test_close_market_date_after_roll(self):
        from gs_quant.markets.markets import close_market_date

        test_date = dt.date(2024, 3, 20)
        mock_ctx = MagicMock()
        mock_ctx.pricing_date = test_date

        # Mock datetime.now to return a time after the roll time
        late_time = dt.datetime(2024, 3, 21, 5, 0, 0)
        mock_now = MagicMock()
        mock_now.astimezone.return_value.replace.return_value = late_time

        with patch('gs_quant.markets.core.PricingContext') as mock_pc, \
             patch('gs_quant.markets.markets.dt') as mock_dt_module:
            mock_pc.current = mock_ctx
            mock_dt_module.datetime.now.return_value = mock_now
            mock_dt_module.datetime.side_effect = lambda *a, **kw: dt.datetime(*a, **kw)
            mock_dt_module.timedelta = dt.timedelta
            result = close_market_date(PricingLocation.LDN, test_date, (24, 0))
            assert result == test_date

    def test_close_market_date_uses_pricing_context_date(self):
        from gs_quant.markets.markets import close_market_date

        test_date = dt.date(2024, 3, 20)
        mock_ctx = MagicMock()
        mock_ctx.pricing_date = test_date

        late_time = dt.datetime(2024, 3, 21, 5, 0, 0)
        mock_now = MagicMock()
        mock_now.astimezone.return_value.replace.return_value = late_time

        with patch('gs_quant.markets.core.PricingContext') as mock_pc, \
             patch('gs_quant.markets.markets.dt') as mock_dt_module:
            mock_pc.current = mock_ctx
            mock_dt_module.datetime.now.return_value = mock_now
            mock_dt_module.datetime.side_effect = lambda *a, **kw: dt.datetime(*a, **kw)
            mock_dt_module.timedelta = dt.timedelta
            # date=None should pick up PricingContext.current.pricing_date
            result = close_market_date(PricingLocation.LDN, None, (24, 0))
            assert result == test_date


# ===========================================================================
# Tests for MarketDataCoordinate
# ===========================================================================

class TestMarketDataCoordinate:
    def test_repr_basic(self):
        from gs_quant.markets.markets import MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD', mkt_class='Swap')
        result = repr(coord)
        assert result == 'IR_USD_Swap'

    def test_repr_with_none_fields(self):
        from gs_quant.markets.markets import MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset=None, mkt_class=None)
        result = repr(coord)
        assert result == 'IR__'

    def test_repr_with_mkt_point(self):
        from gs_quant.markets.markets import MarketDataCoordinate

        coord = MarketDataCoordinate(
            mkt_type='IR', mkt_asset='USD', mkt_class='Swap',
            mkt_point=('1Y', '2Y')
        )
        result = repr(coord)
        assert '1Y,2Y' in result

    def test_repr_with_mkt_quoting_style(self):
        from gs_quant.markets.markets import MarketDataCoordinate

        coord = MarketDataCoordinate(
            mkt_type='IR', mkt_asset='USD', mkt_class='Swap',
            mkt_quoting_style='ATMVol'
        )
        result = repr(coord)
        assert result.endswith('.ATMVol')

    def test_repr_with_point_and_quoting_style(self):
        from gs_quant.markets.markets import MarketDataCoordinate

        coord = MarketDataCoordinate(
            mkt_type='IR', mkt_asset='USD', mkt_class='Swap',
            mkt_point=('1Y',), mkt_quoting_style='Mid'
        )
        result = repr(coord)
        assert '1Y' in result
        assert result.endswith('.Mid')

    def test_from_string_single_point(self):
        from gs_quant.markets.markets import MarketDataCoordinate

        # GsDataApi is imported locally inside from_string
        with patch('gs_quant.api.gs.data.GsDataApi._coordinate_from_str') as mock_coord:
            ret_coord = MagicMock()
            ret_coord.mkt_point = ('1Y;2Y',)
            mock_coord.return_value = ret_coord
            result = MarketDataCoordinate.from_string('IR_USD_Swap_1Y;2Y')
            # Single point gets split by the regex
            assert ret_coord.mkt_point == ('1Y', '2Y')

    def test_from_string_multiple_points(self):
        from gs_quant.markets.markets import MarketDataCoordinate

        with patch('gs_quant.api.gs.data.GsDataApi._coordinate_from_str') as mock_coord:
            ret_coord = MagicMock()
            ret_coord.mkt_point = ('1Y', '2Y')  # already split, len > 1
            mock_coord.return_value = ret_coord
            result = MarketDataCoordinate.from_string('IR_USD_Swap_1Y_2Y')
            # Since len > 1, the split should not happen; mkt_point stays as-is
            assert ret_coord.mkt_point == ('1Y', '2Y')


# ===========================================================================
# Tests for MarketDataCoordinateValue
# ===========================================================================

class TestMarketDataCoordinateValue:
    def test_repr(self):
        from gs_quant.markets.markets import MarketDataCoordinateValue, MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD', mkt_class='Swap')
        cv = MarketDataCoordinateValue(coordinate=coord, value=1.5)
        result = repr(cv)
        assert '-->' in result
        assert '1.5' in result


# ===========================================================================
# Tests for LocationOnlyMarket
# ===========================================================================

class TestLocationOnlyMarket:
    def test_with_pricing_location(self):
        from gs_quant.markets.markets import LocationOnlyMarket

        m = LocationOnlyMarket(PricingLocation.NYC)
        assert m.location == PricingLocation.NYC
        assert m.market is None

    def test_with_string_location(self):
        from gs_quant.markets.markets import LocationOnlyMarket

        m = LocationOnlyMarket('NYC')
        assert m.location == PricingLocation.NYC

    def test_with_none_location(self):
        from gs_quant.markets.markets import LocationOnlyMarket

        m = LocationOnlyMarket(None)
        assert m.location is None


# ===========================================================================
# Tests for CloseMarket
# ===========================================================================

class TestCloseMarket:
    def test_close_market_repr(self):
        from gs_quant.markets.markets import CloseMarket

        m = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=False)
        result = repr(m)
        assert '2024-01-01' in result
        assert 'NYC' in result

    def test_close_market_market_property(self):
        from gs_quant.markets.markets import CloseMarket

        m = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=False)
        market = m.market
        assert market is not None

    def test_close_market_to_dict(self):
        from gs_quant.markets.markets import CloseMarket

        m = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=False)
        d = m.to_dict()
        assert d['marketType'] == 'CloseMarket'
        assert d['date'] == dt.date(2024, 1, 1)
        assert d['location'] == PricingLocation.NYC

    def test_close_market_hash_and_eq(self):
        from gs_quant.markets.markets import CloseMarket

        m1 = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=False)
        m2 = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=False)
        m3 = CloseMarket(date=dt.date(2024, 1, 2), location=PricingLocation.NYC, check=False)

        assert m1 == m2
        assert m1 != m3
        assert hash(m1) == hash(m2)

    def test_close_market_eq_different_type(self):
        from gs_quant.markets.markets import CloseMarket

        m = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=False)
        assert m != "not a market"

    def test_close_market_location_check_false(self):
        from gs_quant.markets.markets import CloseMarket

        m = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=False)
        assert m.location == PricingLocation.NYC

    def test_close_market_location_check_true_with_location(self):
        from gs_quant.markets.markets import CloseMarket

        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC) as mock_ml:
            m = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=True)
            loc = m.location
            mock_ml.assert_called_once_with(PricingLocation.NYC)
            assert loc == PricingLocation.NYC

    def test_close_market_location_none_check_true(self):
        from gs_quant.markets.markets import CloseMarket

        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.LDN) as mock_ml:
            m = CloseMarket(location=None, check=True)
            loc = m.location
            mock_ml.assert_called_once_with(None)
            assert loc == PricingLocation.LDN

    def test_close_market_location_none_check_false(self):
        """When location is None and check=False, still calls market_location."""
        from gs_quant.markets.markets import CloseMarket

        # location=None and check=False => the condition is:
        #   if self.__location is not None and not self.check -> False (location is None)
        #   else -> market_location(self.__location) is called
        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.LDN) as mock_ml:
            m = CloseMarket(location=None, check=False)
            loc = m.location
            mock_ml.assert_called_once_with(None)
            assert loc == PricingLocation.LDN

    def test_close_market_date_check_false(self):
        from gs_quant.markets.markets import CloseMarket

        m = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=False)
        assert m.date == dt.date(2024, 1, 1)

    def test_close_market_date_check_true(self):
        from gs_quant.markets.markets import CloseMarket

        with patch('gs_quant.markets.markets.close_market_date', return_value=dt.date(2024, 1, 2)):
            with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
                m = CloseMarket(date=dt.date(2024, 1, 1), location=PricingLocation.NYC, check=True)
                assert m.date == dt.date(2024, 1, 2)

    def test_close_market_date_none_check_true(self):
        from gs_quant.markets.markets import CloseMarket

        with patch('gs_quant.markets.markets.close_market_date', return_value=dt.date(2024, 1, 5)):
            with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
                m = CloseMarket(date=None, location=PricingLocation.NYC, check=True)
                assert m.date == dt.date(2024, 1, 5)

    def test_close_market_date_none_check_false(self):
        """When date is None and check=False, still calls close_market_date."""
        from gs_quant.markets.markets import CloseMarket

        with patch('gs_quant.markets.markets.close_market_date', return_value=dt.date(2024, 1, 10)):
            with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
                m = CloseMarket(date=None, location=PricingLocation.NYC, check=False)
                # date is None => condition "self.__date is not None and not self.check" is False
                assert m.date == dt.date(2024, 1, 10)

    def test_close_market_string_location(self):
        from gs_quant.markets.markets import CloseMarket

        m = CloseMarket(date=dt.date(2024, 1, 1), location='NYC', check=False)
        assert m.location == PricingLocation.NYC


# ===========================================================================
# Tests for TimestampedMarket
# ===========================================================================

class TestTimestampedMarket:
    def test_repr(self):
        from gs_quant.markets.markets import TimestampedMarket

        ts = dt.datetime(2024, 1, 1, 12, 0, 0)
        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
            m = TimestampedMarket(timestamp=ts, location=PricingLocation.NYC)
            result = repr(m)
            assert 'NYC' in result

    def test_market_property(self):
        from gs_quant.markets.markets import TimestampedMarket

        ts = dt.datetime(2024, 1, 1, 12, 0, 0)
        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
            m = TimestampedMarket(timestamp=ts, location=PricingLocation.NYC)
            market = m.market
            assert market is not None

    def test_location_with_none(self):
        from gs_quant.markets.markets import TimestampedMarket

        ts = dt.datetime(2024, 1, 1, 12, 0, 0)
        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.LDN):
            m = TimestampedMarket(timestamp=ts, location=None)
            assert m.location == PricingLocation.LDN

    def test_string_location(self):
        from gs_quant.markets.markets import TimestampedMarket

        ts = dt.datetime(2024, 1, 1, 12, 0, 0)
        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
            m = TimestampedMarket(timestamp=ts, location='NYC')
            assert m.location == PricingLocation.NYC


# ===========================================================================
# Tests for LiveMarket
# ===========================================================================

class TestLiveMarket:
    def test_repr(self):
        from gs_quant.markets.markets import LiveMarket

        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
            m = LiveMarket(location=PricingLocation.NYC)
            result = repr(m)
            assert 'Live' in result
            assert 'NYC' in result

    def test_market_property(self):
        from gs_quant.markets.markets import LiveMarket

        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
            m = LiveMarket(location=PricingLocation.NYC)
            market = m.market
            assert market is not None

    def test_location_none(self):
        from gs_quant.markets.markets import LiveMarket

        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.TKO):
            m = LiveMarket(location=None)
            assert m.location == PricingLocation.TKO

    def test_string_location(self):
        from gs_quant.markets.markets import LiveMarket

        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
            m = LiveMarket(location='NYC')
            assert m.location == PricingLocation.NYC

    def test_with_pricing_location(self):
        from gs_quant.markets.markets import LiveMarket

        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
            m = LiveMarket(location=PricingLocation.NYC)
            assert m.location == PricingLocation.NYC


# ===========================================================================
# Tests for OverlayMarket
# ===========================================================================

class TestOverlayMarket:
    def test_overlay_market_basic(self):
        from gs_quant.markets.markets import OverlayMarket, MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD', mkt_class='Swap')
        base = MagicMock()
        base.location = PricingLocation.NYC
        base.market = MagicMock()
        m = OverlayMarket(market_data={coord: 1.5}, base_market=base)
        assert m[coord] == 1.5
        assert len(m.coordinates) == 1

    def test_overlay_market_redacted(self):
        from gs_quant.markets.markets import OverlayMarket, MarketDataCoordinate

        coord1 = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD', mkt_class='Swap')
        coord2 = MarketDataCoordinate(mkt_type='FX', mkt_asset='EUR', mkt_class='Spot')
        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(market_data={coord1: 1.5, coord2: 'redacted'}, base_market=base)
        # Redacted coords filtered from market_data
        assert m[coord2] is None
        assert coord2 in m.redacted_coordinates
        assert m[coord1] == 1.5

    def test_overlay_market_setitem_direct(self):
        from gs_quant.markets.markets import OverlayMarket, MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(base_market=base)
        m[coord] = 2.0
        assert m[coord] == 2.0

    def test_overlay_market_setitem_redacted_raises(self):
        from gs_quant.markets.markets import OverlayMarket, MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD', mkt_class='Swap')
        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(market_data={coord: 'redacted'}, base_market=base)
        with pytest.raises(KeyError, match="cannot be overridden"):
            m[coord] = 3.0

    def test_overlay_market_getitem_string(self):
        from gs_quant.markets.markets import OverlayMarket, MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(market_data={coord: 5.0}, base_market=base)
        with patch.object(MarketDataCoordinate, 'from_string', return_value=coord):
            result = m['IR_USD']
            assert result == 5.0

    def test_overlay_market_setitem_string_key_converts(self):
        from gs_quant.markets.markets import OverlayMarket, MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(base_market=base)
        with patch.object(MarketDataCoordinate, 'from_string', return_value=coord):
            m['IR_USD'] = 7.0
            assert m[coord] == 7.0

    def test_overlay_market_repr(self):
        from gs_quant.markets.markets import OverlayMarket

        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(base_market=base)
        result = repr(m)
        assert 'Overlay' in result

    def test_overlay_market_no_market_data(self):
        from gs_quant.markets.markets import OverlayMarket

        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(market_data=None, base_market=base)
        assert len(m.coordinates) == 0

    def test_overlay_market_data_property(self):
        from gs_quant.markets.markets import OverlayMarket, MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(market_data={coord: 1.0}, base_market=base)
        md = m.market_data
        assert len(md) == 1
        assert md[0].value == 1.0

    def test_overlay_market_data_dict_property(self):
        from gs_quant.markets.markets import OverlayMarket, MarketDataCoordinate

        coord = MarketDataCoordinate(mkt_type='IR', mkt_asset='USD')
        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(market_data={coord: 1.0}, base_market=base)
        md = m.market_data_dict
        assert coord in md

    def test_overlay_market_model_data(self):
        from gs_quant.markets.markets import OverlayMarket

        base = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(base_market=base, binary_mkt_data='binary_data')
        assert m.market_model_data == 'binary_data'

    def test_overlay_market_location(self):
        from gs_quant.markets.markets import OverlayMarket

        base = MagicMock()
        base.location = PricingLocation.TKO
        m = OverlayMarket(base_market=base)
        assert m.location == PricingLocation.TKO

    def test_overlay_market_market_property(self):
        from gs_quant.markets.markets import OverlayMarket

        base = MagicMock()
        base.market = MagicMock()
        base.location = PricingLocation.NYC
        m = OverlayMarket(base_market=base)
        market = m.market
        assert market is not None

    def test_overlay_market_default_base(self):
        """When base_market is None, CloseMarket() is used as default."""
        from gs_quant.markets.markets import OverlayMarket, CloseMarket

        with patch('gs_quant.markets.markets.CloseMarket') as mock_cm:
            mock_inst = MagicMock()
            mock_inst.location = PricingLocation.LDN
            mock_cm.return_value = mock_inst
            m = OverlayMarket()
            assert m.location == PricingLocation.LDN


# ===========================================================================
# Tests for RefMarket
# ===========================================================================

class TestRefMarket:
    def test_repr(self):
        from gs_quant.markets.markets import RefMarket

        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.NYC):
            m = RefMarket('ref123')
            result = repr(m)
            assert 'ref123' in result
            assert 'Market Ref' in result

    def test_market_property(self):
        from gs_quant.markets.markets import RefMarket

        m = RefMarket('ref456')
        market = m.market
        assert market is not None

    def test_location(self):
        from gs_quant.markets.markets import RefMarket

        with patch('gs_quant.markets.markets.market_location', return_value=PricingLocation.HKG):
            m = RefMarket('ref789')
            assert m.location == PricingLocation.HKG


# ===========================================================================
# Tests for RelativeMarket
# ===========================================================================

class TestRelativeMarket:
    def test_repr(self):
        from gs_quant.markets.markets import RelativeMarket

        from_mkt = MagicMock()
        from_mkt.__repr__ = lambda self: 'FromMarket'
        to_mkt = MagicMock()
        to_mkt.__repr__ = lambda self: 'ToMarket'
        m = RelativeMarket(from_mkt, to_mkt)
        result = repr(m)
        assert 'FromMarket' in result
        assert 'ToMarket' in result
        assert '->' in result

    def test_market_property(self):
        from gs_quant.markets.markets import RelativeMarket

        from_mkt = MagicMock()
        to_mkt = MagicMock()
        m = RelativeMarket(from_mkt, to_mkt)
        market = m.market
        assert market is not None

    def test_location_same(self):
        from gs_quant.markets.markets import RelativeMarket

        from_mkt = MagicMock()
        from_mkt.location = PricingLocation.NYC
        to_mkt = MagicMock()
        to_mkt.location = PricingLocation.NYC
        m = RelativeMarket(from_mkt, to_mkt)
        assert m.location == PricingLocation.NYC

    def test_location_different(self):
        from gs_quant.markets.markets import RelativeMarket

        from_mkt = MagicMock()
        from_mkt.location = PricingLocation.NYC
        to_mkt = MagicMock()
        to_mkt.location = PricingLocation.LDN
        m = RelativeMarket(from_mkt, to_mkt)
        assert m.location is None
