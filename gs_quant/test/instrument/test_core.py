"""
Copyright 2019 Goldman Sachs.
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

import builtins
import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from gs_quant.base import RiskKey
from gs_quant.common import AssetClass, AssetType, RiskMeasure
from gs_quant.instrument.core import (
    DummyInstrument,
    Instrument,
    Security,
    encode_instrument,
    encode_instruments,
)
from gs_quant.risk import ResolvedInstrumentValues
from gs_quant.risk.results import ErrorValue, PricingFuture

builtins_isinstance = builtins.isinstance


# ---------------------------------------------------------------------------
# Instrument.__repr__
# ---------------------------------------------------------------------------

class TestInstrumentRepr:
    def test_repr_without_name(self):
        """A proper dataclass instrument with name=None shows class name only."""
        from gs_quant.target.instrument import IRSwap
        inst = IRSwap()
        assert inst.name is None
        result = repr(inst)
        assert result == 'IRSwap'

    def test_repr_with_name(self):
        """When name is set, repr includes it in parentheses."""
        from gs_quant.target.instrument import IRSwap
        inst = IRSwap()
        inst.name = 'my_swap'
        result = repr(inst)
        assert result == 'IRSwap(my_swap)'

    def test_repr_dummy_with_name(self):
        """DummyInstrument with name set shows it in parentheses."""
        inst = DummyInstrument()
        inst.name = 'test_dummy'
        result = repr(inst)
        assert result == 'DummyInstrument(test_dummy)'


# ---------------------------------------------------------------------------
# Instrument.from_dict
# ---------------------------------------------------------------------------

class TestInstrumentFromDict:
    def test_empty_dict_returns_none(self):
        assert Instrument.from_dict({}) is None

    def test_none_returns_none(self):
        assert Instrument.from_dict(None) is None

    def test_missing_asset_class_raises(self):
        with pytest.raises(ValueError, match='assetClass/asset_class not specified'):
            Instrument.from_dict({'type': 'Swap'})

    def test_missing_type_raises(self):
        with pytest.raises(ValueError, match='type not specified'):
            Instrument.from_dict({'assetClass': 'Rates'})

    def test_unable_to_build_instrument_raises(self):
        with pytest.raises(ValueError, match='unable to build instrument'):
            Instrument.from_dict({
                'assetClass': 'FakeClass',
                'type': 'FakeType',
            })

    def test_valid_asset_class_and_type(self):
        """from_dict with a known asset class/type resolves to the correct subclass."""
        from gs_quant.target.instrument import IRSwap
        result = Instrument.from_dict({
            'assetClass': 'Rates',
            'type': 'Swap',
        })
        assert isinstance(result, IRSwap)

    def test_asset_class_snake_case_key(self):
        """from_dict also accepts 'asset_class' instead of 'assetClass'."""
        from gs_quant.target.instrument import IRSwap
        result = Instrument.from_dict({
            'asset_class': 'Rates',
            'type': 'Swap',
        })
        assert isinstance(result, IRSwap)

    def test_security_default_type(self):
        """When asset_class and type are both Security-like, from_dict returns a Security."""
        result = Instrument.from_dict({
            'assetClass': 'Security',
            'type': 'Security',
            'bbid': 'AAPL UW',
        })
        assert isinstance(result, Security)

    def test_security_none_type(self):
        """Security is used when both assetClass and type are empty strings."""
        result = Instrument.from_dict({
            'assetClass': '',
            'type': '',
        })
        assert isinstance(result, Security)

    def test_builder_type_branch(self):
        """When $type is present, from_dict imports gs_quant_internal and calls decode_quill_value."""
        mock_decoder = MagicMock(return_value='decoded_value')
        values = {'$type': 'SomeBuilderType', 'field1': 'val1'}
        with patch.dict('sys.modules', {
            'gs_quant_internal': MagicMock(),
            'gs_quant_internal.base': MagicMock(decode_quill_value=mock_decoder),
        }):
            result = Instrument.from_dict(values)
            mock_decoder.assert_called_once_with(values)
            assert result == 'decoded_value'

    def test_builder_type_from_builder_key(self):
        """When 'builder' dict contains $type, the builder dict is passed to decode_quill_value."""
        mock_decoder = MagicMock(return_value='decoded_builder')
        builder_dict = {'$type': 'SomeType', 'param': 42}
        values = {'builder': builder_dict}
        with patch.dict('sys.modules', {
            'gs_quant_internal': MagicMock(),
            'gs_quant_internal.base': MagicMock(decode_quill_value=mock_decoder),
        }):
            result = Instrument.from_dict(values)
            mock_decoder.assert_called_once_with(builder_dict)
            assert result == 'decoded_builder'

    def test_builder_type_from_defn_key(self):
        """When 'defn' dict contains $type, the defn dict is passed to decode_quill_value."""
        mock_decoder = MagicMock(return_value='decoded_defn')
        defn_dict = {'$type': 'SomeType', 'param': 99}
        values = {'defn': defn_dict}
        with patch.dict('sys.modules', {
            'gs_quant_internal': MagicMock(),
            'gs_quant_internal.base': MagicMock(decode_quill_value=mock_decoder),
        }):
            result = Instrument.from_dict(values)
            mock_decoder.assert_called_once_with(defn_dict)
            assert result == 'decoded_defn'

    def test_from_dict_on_subclass_with_asset_class(self):
        """Calling from_dict on a concrete subclass that has asset_class skips the mapping lookup."""
        from gs_quant.target.instrument import IRSwap
        result = IRSwap.from_dict({'notional_currency': 'USD'})
        assert isinstance(result, IRSwap)


# ---------------------------------------------------------------------------
# Instrument.compose
# ---------------------------------------------------------------------------

class TestInstrumentCompose:
    def test_compose_with_error_values(self):
        rk1 = RiskKey('provider', dt.date(2021, 1, 1), None, None, None, None)
        rk2 = RiskKey('provider', dt.date(2021, 1, 2), None, None, None, None)

        ev1 = ErrorValue(rk1, 'error1')
        ev2 = ErrorValue(rk2, 'error2')

        result = Instrument.compose([ev1, ev2])
        assert result[dt.date(2021, 1, 1)] is ev1
        assert result[dt.date(2021, 1, 2)] is ev2

    def test_compose_with_resolved_instruments(self):
        """compose uses resolution_key.date for non-ErrorValue items."""
        mock_inst1 = MagicMock()
        mock_inst1.resolution_key.date = dt.date(2021, 3, 1)
        mock_inst2 = MagicMock()
        mock_inst2.resolution_key.date = dt.date(2021, 3, 2)

        result = Instrument.compose([mock_inst1, mock_inst2])
        assert result[dt.date(2021, 3, 1)] is mock_inst1
        assert result[dt.date(2021, 3, 2)] is mock_inst2

    def test_compose_empty(self):
        result = Instrument.compose([])
        assert result == {}


# ---------------------------------------------------------------------------
# Instrument.flip and Instrument.scale
# ---------------------------------------------------------------------------

class TestInstrumentFlipAndScale:
    def test_scale_with_none_returns_self(self):
        inst = DummyInstrument(42.0)
        result = inst.scale(None)
        assert result is inst

    def test_scale_without_scale_in_place_raises(self):
        inst = DummyInstrument(42.0)
        assert not hasattr(inst, 'scale_in_place')
        with pytest.raises(NotImplementedError, match='scale_in_place not implemented'):
            inst.scale(2.0)

    def test_scale_in_place_true(self):
        inst = DummyInstrument(42.0)
        mock_scale = MagicMock()
        inst.scale_in_place = mock_scale

        result = inst.scale(2.0, in_place=True)
        mock_scale.assert_called_once_with(2.0, check_resolved=True)
        assert result is None

    def test_scale_in_place_false(self):
        inst = DummyInstrument(42.0)
        mock_scale = MagicMock()
        inst.scale_in_place = mock_scale

        result = inst.scale(3.0, in_place=False)
        # The original should NOT have been scaled
        mock_scale.assert_not_called()
        # Result should be a different object (deepcopy)
        assert result is not inst
        assert isinstance(result, DummyInstrument)

    def test_scale_check_resolved_false(self):
        inst = DummyInstrument(42.0)
        mock_scale = MagicMock()
        inst.scale_in_place = mock_scale

        inst.scale(5.0, in_place=True, check_resolved=False)
        mock_scale.assert_called_once_with(5.0, check_resolved=False)

    def test_flip_delegates_to_scale(self):
        inst = DummyInstrument(42.0)
        mock_scale = MagicMock()
        inst.scale_in_place = mock_scale

        result = inst.flip(in_place=True)
        mock_scale.assert_called_once_with(-1, check_resolved=True)
        assert result is None

    def test_flip_not_in_place(self):
        inst = DummyInstrument(42.0)
        mock_scale = MagicMock()
        inst.scale_in_place = mock_scale

        result = inst.flip(in_place=False)
        assert result is not inst
        assert isinstance(result, DummyInstrument)


# ---------------------------------------------------------------------------
# Instrument.provider
# ---------------------------------------------------------------------------

class TestInstrumentProvider:
    def test_provider_returns_gs_risk_api(self):
        from gs_quant.api.gs.risk import GsRiskApi
        inst = DummyInstrument()
        assert inst.provider is GsRiskApi


# ---------------------------------------------------------------------------
# DummyInstrument
# ---------------------------------------------------------------------------

class TestDummyInstrument:
    def test_create_default(self):
        inst = DummyInstrument()
        assert inst.dummy_result is None

    def test_create_with_float(self):
        inst = DummyInstrument(42.0)
        assert inst.dummy_result == 42.0

    def test_create_with_string(self):
        inst = DummyInstrument('result_str')
        assert inst.dummy_result == 'result_str'

    def test_set_dummy_result(self):
        inst = DummyInstrument()
        inst.dummy_result = 99.9
        assert inst.dummy_result == 99.9

    def test_type_property(self):
        inst = DummyInstrument()
        assert inst.type == AssetType.Any


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------

class TestSecurity:
    def test_create_with_ticker(self):
        sec = Security(ticker='AAPL')
        assert sec.ticker == 'AAPL'
        assert sec.quantity_ == 1

    def test_create_with_bbid(self):
        sec = Security(bbid='AAPL UW')
        assert sec.bbid == 'AAPL UW'

    def test_create_with_ric(self):
        sec = Security(ric='AAPL.O')
        assert sec.ric == 'AAPL.O'

    def test_create_with_isin(self):
        sec = Security(isin='US0378331005')
        assert sec.isin == 'US0378331005'

    def test_create_with_cusip(self):
        sec = Security(cusip='037833100')
        assert sec.cusip == '037833100'

    def test_create_with_prime_id(self):
        sec = Security(prime_id='12345')
        assert sec.prime_id == '12345'

    def test_create_with_quantity(self):
        sec = Security(ticker='AAPL', quantity=100)
        assert sec.quantity_ == 100

    def test_multiple_identifiers_raises(self):
        with pytest.raises(ValueError, match='Only specify one identifier'):
            Security(ticker='AAPL', bbid='AAPL UW')

    def test_from_dict(self):
        sec = Security.from_dict({'ticker': 'AAPL', 'quantity': 50})
        assert isinstance(sec, Security)
        assert sec.ticker == 'AAPL'
        assert sec.quantity_ == 50

    def test_from_dict_ignores_unknown_keys(self):
        sec = Security.from_dict({'ticker': 'AAPL', 'unknown_key': 'value'})
        assert isinstance(sec, Security)
        assert sec.ticker == 'AAPL'


# ---------------------------------------------------------------------------
# encode_instrument / encode_instruments
# ---------------------------------------------------------------------------

class TestEncodeInstrument:
    def test_encode_none_returns_none(self):
        assert encode_instrument(None) is None

    def test_encode_instrument_returns_dict(self):
        inst = DummyInstrument(42.0)
        result = encode_instrument(inst)
        assert isinstance(result, dict)

    def test_encode_instruments_none_returns_none(self):
        assert encode_instruments(None) is None

    def test_encode_instruments_returns_list(self):
        inst1 = DummyInstrument(1.0)
        inst2 = DummyInstrument(2.0)
        result = encode_instruments([inst1, inst2])
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(r, dict) for r in result)

    def test_encode_instruments_empty_list(self):
        result = encode_instruments([])
        assert result == []


# ---------------------------------------------------------------------------
# Instrument.resolve (mocked)
# ---------------------------------------------------------------------------

class TestInstrumentResolve:
    def test_resolve_calls_calc(self):
        """resolve() delegates to calc(ResolvedInstrumentValues, fn=...)."""
        inst = DummyInstrument(42.0)
        with patch.object(Instrument, 'calc') as mock_calc:
            mock_calc.return_value = None
            inst.resolve(in_place=True)
            mock_calc.assert_called_once()
            args, kwargs = mock_calc.call_args
            assert args[0] is ResolvedInstrumentValues
            assert 'fn' in kwargs
            assert callable(kwargs['fn'])

    def test_resolve_not_in_place(self):
        """resolve(in_place=False) delegates to calc and returns result."""
        inst = DummyInstrument(42.0)
        with patch.object(Instrument, 'calc') as mock_calc:
            mock_calc.return_value = 'resolved_result'
            result = inst.resolve(in_place=False)
            mock_calc.assert_called_once()
            assert result == 'resolved_result'

    def test_resolve_in_place_historical_raises(self):
        """resolve(in_place=True) under HistoricalPricingContext raises RuntimeError."""
        from gs_quant.markets import HistoricalPricingContext

        inst = DummyInstrument(42.0)
        mock_ctx = MagicMock(spec=HistoricalPricingContext)

        with patch('gs_quant.instrument.core.PricingContext') as mock_pc:
            mock_pc.current = mock_ctx
            with patch('gs_quant.instrument.core.isinstance') as mock_isinst:
                def side_effect(obj, cls):
                    if cls is HistoricalPricingContext:
                        return True
                    return builtins_isinstance(obj, cls)
                mock_isinst.side_effect = side_effect
                with pytest.raises(RuntimeError, match='Cannot resolve in place under a HistoricalPricingContext'):
                    inst.resolve(in_place=True)


# ---------------------------------------------------------------------------
# Instrument.calc (mocked)
# ---------------------------------------------------------------------------

class TestInstrumentCalc:
    def _make_pricing_future(self, result_value):
        """Create a PricingFuture that's already resolved with a value."""
        f = PricingFuture()
        f.set_result(result_value)
        return f

    def _make_mock_ctx(self, future_or_side_effect):
        """Create a mock context that acts as both context manager and PricingContext."""
        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_ctx)
        mock_ctx.__exit__ = MagicMock(return_value=False)
        if isinstance(future_or_side_effect, list):
            mock_ctx.calc.side_effect = future_or_side_effect
        else:
            mock_ctx.calc.return_value = future_or_side_effect
        return mock_ctx

    def test_calc_single_measure(self):
        """calc with a single RiskMeasure returns the result."""
        inst = DummyInstrument(42.0)
        measure = RiskMeasure(name='TestMeasure')
        future = self._make_pricing_future(100.0)
        mock_ctx = self._make_mock_ctx(future)

        with patch.object(type(inst), '_pricing_context', new_callable=PropertyMock, return_value=mock_ctx), \
             patch.object(type(inst), '_return_future', new_callable=PropertyMock, return_value=False), \
             patch('gs_quant.instrument.core.Scenario') as mock_scenario, \
             patch.object(RiskMeasure, 'pricing_context', new_callable=PropertyMock, return_value=mock_ctx):
            mock_scenario.path = []
            result = inst.calc(measure)
            assert result == 100.0

    def test_calc_with_fn_callback(self):
        """calc with fn= wraps the result through the callback."""
        inst = DummyInstrument(42.0)
        measure = RiskMeasure(name='TestMeasure')
        future = self._make_pricing_future(100.0)
        mock_ctx = self._make_mock_ctx(future)

        with patch.object(type(inst), '_pricing_context', new_callable=PropertyMock, return_value=mock_ctx), \
             patch.object(type(inst), '_return_future', new_callable=PropertyMock, return_value=False), \
             patch('gs_quant.instrument.core.Scenario') as mock_scenario, \
             patch.object(RiskMeasure, 'pricing_context', new_callable=PropertyMock, return_value=mock_ctx):
            mock_scenario.path = []
            result = inst.calc(measure, fn=lambda x: x * 2)
            assert result == 200.0

    def test_calc_multiple_measures(self):
        """calc with multiple RiskMeasures returns a MultipleRiskMeasureFuture result."""
        inst = DummyInstrument(42.0)
        measure1 = RiskMeasure(name='Measure1')
        measure2 = RiskMeasure(name='Measure2')

        future1 = self._make_pricing_future(10.0)
        future2 = self._make_pricing_future(20.0)
        mock_ctx = self._make_mock_ctx([future1, future2])

        with patch.object(type(inst), '_pricing_context', new_callable=PropertyMock, return_value=mock_ctx), \
             patch.object(type(inst), '_return_future', new_callable=PropertyMock, return_value=True), \
             patch('gs_quant.instrument.core.Scenario') as mock_scenario, \
             patch.object(RiskMeasure, 'pricing_context', new_callable=PropertyMock, return_value=mock_ctx):
            mock_scenario.path = []
            result = inst.calc([measure1, measure2])
            # Returns a future since _return_future is True
            assert result is not None

    def test_calc_returns_future_when_async(self):
        """calc returns a PricingFuture when _return_future is True."""
        inst = DummyInstrument(42.0)
        measure = RiskMeasure(name='TestMeasure')
        future = self._make_pricing_future(50.0)
        mock_ctx = self._make_mock_ctx(future)

        with patch.object(type(inst), '_pricing_context', new_callable=PropertyMock, return_value=mock_ctx), \
             patch.object(type(inst), '_return_future', new_callable=PropertyMock, return_value=True), \
             patch('gs_quant.instrument.core.Scenario') as mock_scenario, \
             patch.object(RiskMeasure, 'pricing_context', new_callable=PropertyMock, return_value=mock_ctx):
            mock_scenario.path = []
            result = inst.calc(measure)
            # Should return the future itself, not the result
            assert isinstance(result, PricingFuture)
            assert result.result() == 50.0
