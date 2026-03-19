"""
Branch coverage tests for gs_quant/timeseries/measure_registry.py
"""
from unittest.mock import MagicMock

import pytest

from gs_quant.errors import MqError
from gs_quant.timeseries.measure_registry import MultiMeasure, register_measure, registry


class TestMultiMeasure:
    def test_init(self):
        mm = MultiMeasure('test_measure')
        assert mm.display_name == 'test_measure'
        assert mm.measure_map == {}

    def test_register(self):
        mm = MultiMeasure('test')
        fn = MagicMock()
        fn.asset_class = ['Equity', 'FX']
        mm.register(fn)
        assert 'Equity' in mm.measure_map
        assert 'FX' in mm.measure_map
        assert fn in mm.measure_map['Equity']
        assert fn in mm.measure_map['FX']

    def test_register_multiple_functions_same_class(self):
        mm = MultiMeasure('test')
        fn1 = MagicMock()
        fn1.asset_class = ['Equity']
        fn2 = MagicMock()
        fn2.asset_class = ['Equity']
        mm.register(fn1)
        mm.register(fn2)
        assert len(mm.measure_map['Equity']) == 2

    def test_get_fn_matching_asset_type(self):
        mm = MultiMeasure('test')

        fn = MagicMock()
        fn.asset_class = ['Equity']
        mock_asset_type = MagicMock()
        mock_asset_type.value = 'Stock'
        fn.asset_type = [mock_asset_type]
        fn.asset_type_excluded = None
        mm.register(fn)

        asset = MagicMock()
        asset.asset_class = 'Equity'
        asset_type = MagicMock()
        asset_type.value = 'Stock'
        asset.get_type.return_value = asset_type

        result = mm.get_fn(asset)
        assert result == fn

    def test_get_fn_asset_type_none(self):
        """fn.asset_type is None => matches any asset type"""
        mm = MultiMeasure('test')

        fn = MagicMock()
        fn.asset_class = ['Equity']
        fn.asset_type = None
        fn.asset_type_excluded = None
        mm.register(fn)

        asset = MagicMock()
        asset.asset_class = 'Equity'
        asset_type = MagicMock()
        asset_type.value = 'Anything'
        asset.get_type.return_value = asset_type

        result = mm.get_fn(asset)
        assert result == fn

    def test_get_fn_asset_type_excluded(self):
        """fn.asset_type is None but asset_type_excluded excludes the type"""
        mm = MultiMeasure('test')

        fn = MagicMock()
        fn.asset_class = ['Equity']
        fn.asset_type = None
        excluded_type = MagicMock()
        excluded_type.value = 'Stock'
        fn.asset_type_excluded = [excluded_type]
        mm.register(fn)

        asset = MagicMock()
        asset.asset_class = 'Equity'
        asset_type = MagicMock()
        asset_type.value = 'Stock'
        asset.get_type.return_value = asset_type

        with pytest.raises(MqError, match='No measure'):
            mm.get_fn(asset)

    def test_get_fn_no_matching_class(self):
        """No functions registered for the asset class"""
        mm = MultiMeasure('test')

        asset = MagicMock()
        asset.asset_class = 'Equity'
        asset_type = MagicMock()
        asset_type.value = 'Stock'
        asset.get_type.return_value = asset_type

        with pytest.raises(MqError, match='No measure'):
            mm.get_fn(asset)

    def test_get_fn_no_matching_type(self):
        """Functions registered but none match the asset type"""
        mm = MultiMeasure('test')

        fn = MagicMock()
        fn.asset_class = ['Equity']
        mock_asset_type = MagicMock()
        mock_asset_type.value = 'Bond'
        fn.asset_type = [mock_asset_type]
        fn.asset_type_excluded = None
        mm.register(fn)

        asset = MagicMock()
        asset.asset_class = 'Equity'
        asset_type = MagicMock()
        asset_type.value = 'Stock'
        asset.get_type.return_value = asset_type

        with pytest.raises(MqError, match='No measure'):
            mm.get_fn(asset)

    def test_get_fn_canonicalize_special_chars(self):
        """Test canonicalize strips non-word characters and is case-insensitive"""
        mm = MultiMeasure('test')

        fn = MagicMock()
        fn.asset_class = ['Equity']
        mock_asset_type = MagicMock()
        mock_asset_type.value = 'Stock-Option'
        fn.asset_type = [mock_asset_type]
        fn.asset_type_excluded = None
        mm.register(fn)

        asset = MagicMock()
        asset.asset_class = 'Equity'
        asset_type = MagicMock()
        asset_type.value = 'StockOption'
        asset.get_type.return_value = asset_type

        result = mm.get_fn(asset)
        assert result == fn

    def test_call(self):
        """Test __call__ delegates to get_fn and calls the function"""
        mm = MultiMeasure('test')

        fn = MagicMock()
        fn.asset_class = ['Equity']
        fn.asset_type = None
        fn.asset_type_excluded = None
        fn.return_value = 42
        mm.register(fn)

        asset = MagicMock()
        asset.asset_class = 'Equity'
        asset_type = MagicMock()
        asset_type.value = 'Stock'
        asset.get_type.return_value = asset_type

        result = mm(asset, 'arg1', key='val')
        fn.assert_called_once_with(asset, 'arg1', key='val')
        assert result == 42

    def test_get_fn_asset_type_excluded_none_and_type_matches(self):
        """fn.asset_type matches, fn.asset_type_excluded is None"""
        mm = MultiMeasure('test')

        fn = MagicMock()
        fn.asset_class = ['Equity']
        mock_type = MagicMock()
        mock_type.value = 'ETF'
        fn.asset_type = [mock_type]
        fn.asset_type_excluded = None
        mm.register(fn)

        asset = MagicMock()
        asset.asset_class = 'Equity'
        asset_type = MagicMock()
        asset_type.value = 'ETF'
        asset.get_type.return_value = asset_type

        result = mm.get_fn(asset)
        assert result == fn

    def test_get_fn_asset_type_not_excluded(self):
        """fn.asset_type is None, asset_type_excluded does not exclude the type"""
        mm = MultiMeasure('test')

        fn = MagicMock()
        fn.asset_class = ['Equity']
        fn.asset_type = None
        excluded_type = MagicMock()
        excluded_type.value = 'Bond'
        fn.asset_type_excluded = [excluded_type]
        mm.register(fn)

        asset = MagicMock()
        asset.asset_class = 'Equity'
        asset_type = MagicMock()
        asset_type.value = 'Stock'
        asset.get_type.return_value = asset_type

        result = mm.get_fn(asset)
        assert result == fn


class TestRegisterMeasure:
    def test_register_new_measure(self):
        """Test registering a function creates a new MultiMeasure"""
        fn = MagicMock()
        fn.display_name = 'new_test_measure_abc123'
        fn.__name__ = 'test_fn'
        fn.asset_class = ['Equity']

        # Remove from registry if exists
        registry.pop('new_test_measure_abc123', None)

        result = register_measure(fn)
        assert isinstance(result, MultiMeasure)
        assert result.display_name == 'new_test_measure_abc123'
        assert 'new_test_measure_abc123' in registry

        # Cleanup
        registry.pop('new_test_measure_abc123', None)

    def test_register_existing_measure(self):
        """Test registering a function to an existing MultiMeasure"""
        fn1 = MagicMock()
        fn1.display_name = 'existing_test_measure_xyz789'
        fn1.__name__ = 'fn1'
        fn1.asset_class = ['Equity']

        fn2 = MagicMock()
        fn2.display_name = 'existing_test_measure_xyz789'
        fn2.__name__ = 'fn2'
        fn2.asset_class = ['FX']

        registry.pop('existing_test_measure_xyz789', None)

        result1 = register_measure(fn1)
        result2 = register_measure(fn2)
        assert result1 is result2
        assert 'FX' in result2.measure_map

        # Cleanup
        registry.pop('existing_test_measure_xyz789', None)

    def test_register_no_display_name(self):
        """Test when display_name is falsy, __name__ is used"""
        fn = MagicMock()
        fn.display_name = None
        fn.__name__ = 'fallback_name_test_777'
        fn.asset_class = ['Equity']

        registry.pop('fallback_name_test_777', None)

        result = register_measure(fn)
        assert result.display_name == 'fallback_name_test_777'
        assert 'fallback_name_test_777' in registry

        # Cleanup
        registry.pop('fallback_name_test_777', None)

    def test_register_display_name_empty_string(self):
        """Test when display_name is empty string, __name__ is used"""
        fn = MagicMock()
        fn.display_name = ''
        fn.__name__ = 'fallback_empty_test_888'
        fn.asset_class = ['Equity']

        registry.pop('fallback_empty_test_888', None)

        result = register_measure(fn)
        assert result.display_name == 'fallback_empty_test_888'

        # Cleanup
        registry.pop('fallback_empty_test_888', None)
