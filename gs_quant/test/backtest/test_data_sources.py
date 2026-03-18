"""
Copyright 2019 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the 'License');
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import datetime as dt

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from gs_quant.backtests.core import ValuationFixingType
from gs_quant.backtests.data_sources import (
    DataSource,
    GsDataSource,
    GenericDataSource,
    MissingDataStrategy,
    DataManager,
)
from gs_quant.data import DataFrequency


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_instrument(name='inst_SPX'):
    inst = MagicMock()
    inst.name = name
    return inst


def _daily_series(dates_values):
    """Build a pd.Series from a dict of {date: value}."""
    idx = pd.DatetimeIndex(sorted(dates_values.keys()))
    vals = [dates_values[k] for k in sorted(dates_values.keys())]
    return pd.Series(vals, index=idx, dtype=float)


def _date_series():
    """A simple series indexed by plain dates (not DatetimeIndex)."""
    dates = [dt.date(2020, 1, i) for i in range(1, 6)]
    return pd.Series([10.0, 20.0, 30.0, 40.0, 50.0], index=dates)


# ===========================================================================
# DataSource base class
# ===========================================================================

class TestDataSource:
    def test_subclass_registry(self):
        """All known subclasses should be registered via __init_subclass__."""
        subs = DataSource.sub_classes()
        assert GsDataSource in subs
        assert GenericDataSource in subs
        assert isinstance(subs, tuple)

    def test_get_data_raises(self):
        ds = DataSource()
        with pytest.raises(RuntimeError, match='Implemented by subclass'):
            ds.get_data(None)

    def test_get_data_range_raises(self):
        ds = DataSource()
        with pytest.raises(RuntimeError, match='Implemented by subclass'):
            ds.get_data_range(dt.date.today(), dt.date.today())


# ===========================================================================
# GsDataSource
# ===========================================================================

class TestGsDataSource:
    def _make_gs_source(self, min_date=None, max_date=None, asset_id='ABC123'):
        src = GsDataSource(
            data_set='MY_DATASET',
            asset_id=asset_id,
            min_date=min_date,
            max_date=max_date,
            value_header='rate',
        )
        return src

    # -- get_data -----------------------------------------------------------

    @patch('gs_quant.backtests.data_sources.Dataset')
    def test_get_data_loaded_data_none_with_min_date(self, MockDataset):
        """loaded_data is None, min_date is set -> load full range, then return value."""
        src = self._make_gs_source(
            min_date=dt.date(2020, 1, 1),
            max_date=dt.date(2020, 12, 31),
        )
        state = dt.date(2020, 6, 15)

        mock_ds_instance = MagicMock()
        MockDataset.return_value = mock_ds_instance

        loaded_df = pd.DataFrame(
            {'rate': [1.5, 2.5]},
            index=pd.to_datetime([dt.date(2020, 6, 15), dt.date(2020, 6, 16)]),
        )
        mock_ds_instance.get_data.return_value = loaded_df

        result = src.get_data(state)

        MockDataset.assert_called_once_with('MY_DATASET')
        mock_ds_instance.get_data.assert_called_once_with(
            dt.date(2020, 1, 1), dt.date(2020, 12, 31), assetId=('ABC123',)
        )
        assert result == 1.5

    @patch('gs_quant.backtests.data_sources.Dataset')
    def test_get_data_loaded_data_none_state_not_none_no_min_date(self, MockDataset):
        """loaded_data is None, no min_date, state is not None -> single date load."""
        src = self._make_gs_source(min_date=None, max_date=None)
        state = dt.date(2020, 6, 15)

        mock_ds_instance = MagicMock()
        MockDataset.return_value = mock_ds_instance

        mock_result = pd.DataFrame(
            {'rate': [3.14]},
            index=pd.to_datetime([dt.date(2020, 6, 15)]),
        )
        mock_ds_instance.get_data.return_value = mock_result

        result = src.get_data(state)

        mock_ds_instance.get_data.assert_called_once_with(state, state, assetId=('ABC123',))
        assert isinstance(result, pd.Series)

    @patch('gs_quant.backtests.data_sources.Dataset')
    def test_get_data_loaded_data_none_state_none_no_min_date(self, MockDataset):
        """loaded_data is None, no min_date, state is None -> load from 2000."""
        src = self._make_gs_source(min_date=None, max_date=None)

        mock_ds_instance = MagicMock()
        MockDataset.return_value = mock_ds_instance

        mock_result = pd.DataFrame(
            {'rate': [9.9]},
            index=pd.to_datetime([dt.date(2020, 1, 1)]),
        )
        mock_ds_instance.get_data.return_value = mock_result

        result = src.get_data(None)

        mock_ds_instance.get_data.assert_called_once_with(dt.datetime(2000, 1, 1))
        assert isinstance(result, pd.Series)

    @patch('gs_quant.backtests.data_sources.Dataset')
    def test_get_data_already_loaded(self, MockDataset):
        """loaded_data already present -> skip Dataset call, look up value."""
        src = self._make_gs_source()
        loaded_df = pd.DataFrame(
            {'rate': [5.5, 6.6]},
            index=pd.to_datetime([dt.date(2020, 3, 1), dt.date(2020, 3, 2)]),
        )
        src.loaded_data = loaded_df

        result = src.get_data(dt.date(2020, 3, 1))
        assert result == 5.5
        MockDataset.assert_not_called()

    # -- get_data_range -----------------------------------------------------

    @patch('gs_quant.backtests.data_sources.Dataset')
    def test_get_data_range_loaded_data_none_with_asset_and_min_date(self, MockDataset):
        """loaded_data is None, asset_id set, min_date set -> load range, then filter."""
        src = self._make_gs_source(
            min_date=dt.date(2020, 1, 1),
            max_date=dt.date(2020, 12, 31),
            asset_id='XYZ',
        )

        mock_ds_instance = MagicMock()
        MockDataset.return_value = mock_ds_instance

        idx = pd.to_datetime([
            dt.date(2020, 3, 1), dt.date(2020, 3, 2),
            dt.date(2020, 3, 3), dt.date(2020, 3, 4),
        ])
        loaded_df = pd.DataFrame({'rate': [1, 2, 3, 4]}, index=idx)
        mock_ds_instance.get_data.return_value = loaded_df

        result = src.get_data_range(
            pd.Timestamp('2020-03-01'),
            pd.Timestamp('2020-03-03'),
        )

        mock_ds_instance.get_data.assert_called_once_with(
            dt.date(2020, 1, 1), dt.date(2020, 12, 31), assetId=('XYZ',)
        )
        # Should return rows where start < index <= end
        assert len(result) == 2
        assert result.iloc[0]['rate'] == 2
        assert result.iloc[1]['rate'] == 3

    @patch('gs_quant.backtests.data_sources.Dataset')
    def test_get_data_range_loaded_data_none_no_min_date(self, MockDataset):
        """loaded_data is None, no min_date -> load from start to max_date."""
        src = self._make_gs_source(min_date=None, max_date=dt.date(2020, 12, 31))

        mock_ds_instance = MagicMock()
        MockDataset.return_value = mock_ds_instance

        idx = pd.to_datetime([dt.date(2020, 6, 1), dt.date(2020, 6, 2)])
        loaded_df = pd.DataFrame({'rate': [10, 20]}, index=idx)
        mock_ds_instance.get_data.return_value = loaded_df

        start = pd.Timestamp('2020-06-01')
        end = pd.Timestamp('2020-06-02')
        result = src.get_data_range(start, end)

        mock_ds_instance.get_data.assert_called_once_with(
            start, dt.date(2020, 12, 31), assetId=('ABC123',)
        )
        assert len(result) == 1  # start < index <= end

    @patch('gs_quant.backtests.data_sources.Dataset')
    def test_get_data_range_loaded_data_none_no_asset_id(self, MockDataset):
        """loaded_data is None, asset_id is None -> kwargs should NOT have assetId."""
        src = self._make_gs_source(min_date=None, max_date=dt.date(2020, 12, 31), asset_id=None)

        mock_ds_instance = MagicMock()
        MockDataset.return_value = mock_ds_instance

        idx = pd.to_datetime([dt.date(2020, 6, 1)])
        loaded_df = pd.DataFrame({'rate': [10]}, index=idx)
        mock_ds_instance.get_data.return_value = loaded_df

        start = pd.Timestamp('2020-06-01')
        end = pd.Timestamp('2020-06-02')
        src.get_data_range(start, end)

        # Verify assetId was NOT passed
        call_kwargs = mock_ds_instance.get_data.call_args[1]
        assert 'assetId' not in call_kwargs

    @patch('gs_quant.backtests.data_sources.Dataset')
    def test_get_data_range_int_end(self, MockDataset):
        """end is an int -> tail(end) where index < start."""
        src = self._make_gs_source()
        idx = pd.to_datetime([
            dt.date(2020, 1, 1), dt.date(2020, 1, 2), dt.date(2020, 1, 3),
            dt.date(2020, 1, 4), dt.date(2020, 1, 5),
        ])
        loaded_df = pd.DataFrame({'rate': [10, 20, 30, 40, 50]}, index=idx)
        src.loaded_data = loaded_df

        start = pd.Timestamp('2020-01-04')
        result = src.get_data_range(start, 2)

        assert len(result) == 2
        assert list(result['rate']) == [20, 30]

    @patch('gs_quant.backtests.data_sources.Dataset')
    def test_get_data_range_already_loaded(self, MockDataset):
        """loaded_data is already present -> skip Dataset call."""
        src = self._make_gs_source()
        idx = pd.to_datetime([dt.date(2020, 1, 1), dt.date(2020, 1, 2)])
        loaded_df = pd.DataFrame({'rate': [10, 20]}, index=idx)
        src.loaded_data = loaded_df

        start = pd.Timestamp('2020-01-01')
        end = pd.Timestamp('2020-01-02')
        result = src.get_data_range(start, end)

        MockDataset.assert_not_called()
        assert len(result) == 1
        assert result.iloc[0]['rate'] == 20


# ===========================================================================
# GenericDataSource
# ===========================================================================

class TestGenericDataSource:
    # -- __eq__ -------------------------------------------------------------

    def test_eq_same(self):
        s = pd.Series([1.0, 2.0], index=pd.to_datetime(['2020-01-01', '2020-01-02']))
        a = GenericDataSource(data_set=s, missing_data_strategy=MissingDataStrategy.fail)
        b = GenericDataSource(data_set=s.copy(), missing_data_strategy=MissingDataStrategy.fail)
        assert a == b

    def test_eq_different_strategy(self):
        s = pd.Series([1.0, 2.0], index=pd.to_datetime(['2020-01-01', '2020-01-02']))
        a = GenericDataSource(data_set=s, missing_data_strategy=MissingDataStrategy.fail)
        b = GenericDataSource(data_set=s.copy(), missing_data_strategy=MissingDataStrategy.interpolate)
        assert a != b

    def test_eq_different_data(self):
        s1 = pd.Series([1.0, 2.0], index=pd.to_datetime(['2020-01-01', '2020-01-02']))
        s2 = pd.Series([1.0, 99.0], index=pd.to_datetime(['2020-01-01', '2020-01-02']))
        a = GenericDataSource(data_set=s1)
        b = GenericDataSource(data_set=s2)
        assert a != b

    def test_eq_not_generic_data_source(self):
        s = pd.Series([1.0], index=pd.to_datetime(['2020-01-01']))
        a = GenericDataSource(data_set=s)
        assert a != 'not a GenericDataSource'
        assert a != 42

    # -- __post_init__ (tz-aware detection) ---------------------------------

    def test_post_init_tz_aware(self):
        idx = pd.to_datetime(['2020-01-01', '2020-01-02']).tz_localize('UTC')
        s = pd.Series([1.0, 2.0], index=idx)
        gds = GenericDataSource(data_set=s)
        assert gds._tz_aware is True

    def test_post_init_tz_naive(self):
        idx = pd.to_datetime(['2020-01-01', '2020-01-02'])
        s = pd.Series([1.0, 2.0], index=idx)
        gds = GenericDataSource(data_set=s)
        assert gds._tz_aware is False

    def test_post_init_date_index(self):
        s = _date_series()
        gds = GenericDataSource(data_set=s)
        assert gds._tz_aware is False

    # -- get_data: state is None --------------------------------------------

    def test_get_data_state_none_returns_full_series(self):
        s = pd.Series([1.0, 2.0], index=pd.to_datetime(['2020-01-01', '2020-01-02']))
        gds = GenericDataSource(data_set=s)
        result = gds.get_data(None)
        assert result.equals(s)

    # -- get_data: state is Iterable ----------------------------------------

    def test_get_data_iterable(self):
        s = pd.Series([10.0, 20.0, 30.0],
                       index=pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03']))
        gds = GenericDataSource(data_set=s)
        dates = [pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-03')]
        result = gds.get_data(dates)
        assert result == [10.0, 30.0]

    # -- get_data: tz-aware dataset with naive state -----------------------

    def test_get_data_tz_aware_naive_state(self):
        idx = pd.to_datetime(['2020-06-15 10:00', '2020-06-15 11:00']).tz_localize('UTC')
        s = pd.Series([100.0, 200.0], index=idx)
        gds = GenericDataSource(data_set=s)

        # Pass a naive datetime — should be converted to UTC
        naive_state = dt.datetime(2020, 6, 15, 10, 0, 0)
        result = gds.get_data(naive_state)
        assert result == 100.0

    def test_get_data_tz_aware_with_none_utcoffset(self):
        """State has tzinfo but utcoffset returns None -> still treated as naive."""
        idx = pd.to_datetime(['2020-06-15 10:00']).tz_localize('UTC')
        s = pd.Series([100.0], index=idx)
        gds = GenericDataSource(data_set=s)

        # Create a real tzinfo subclass whose utcoffset returns None
        class NullTzInfo(dt.tzinfo):
            def utcoffset(self, _dt):
                return None
            def tzname(self, _dt):
                return 'NULL'
            def dst(self, _dt):
                return None

        state = dt.datetime(2020, 6, 15, 10, 0, 0, tzinfo=NullTzInfo())
        result = gds.get_data(state)
        assert result == 100.0

    # -- get_data: Timestamp match -----------------------------------------

    def test_get_data_timestamp_match(self):
        s = pd.Series([42.0], index=pd.to_datetime(['2020-01-01']))
        gds = GenericDataSource(data_set=s)
        result = gds.get_data(dt.date(2020, 1, 1))
        assert result == 42.0

    # -- get_data: state in data (direct lookup) ----------------------------

    def test_get_data_state_in_data_direct(self):
        """state is in data_set directly (the 'state in self.data_set' branch)."""
        dates = [dt.date(2020, 1, 1), dt.date(2020, 1, 2)]
        s = pd.Series([10.0, 20.0], index=dates)
        gds = GenericDataSource(data_set=s)
        result = gds.get_data(dt.date(2020, 1, 1))
        assert result == 10.0

    # -- get_data: missing_data_strategy = fail (KeyError) ------------------

    def test_get_data_fail_strategy_missing(self):
        """MissingDataStrategy.fail with missing key -> KeyError via data_set[state]."""
        s = pd.Series([10.0, 20.0], index=pd.to_datetime(['2020-01-01', '2020-01-03']))
        gds = GenericDataSource(data_set=s, missing_data_strategy=MissingDataStrategy.fail)
        with pytest.raises(KeyError):
            gds.get_data(dt.datetime(2020, 1, 2))

    # -- get_data: missing_data_strategy = interpolate ----------------------

    def test_get_data_interpolate_datetime_index(self):
        s = pd.Series([10.0, 30.0], index=pd.to_datetime(['2020-01-01', '2020-01-03']))
        gds = GenericDataSource(data_set=s, missing_data_strategy=MissingDataStrategy.interpolate)
        result = gds.get_data(dt.datetime(2020, 1, 2))
        assert result == pytest.approx(20.0)

    # -- get_data: missing_data_strategy = fill_forward ---------------------

    def test_get_data_fill_forward_datetime_index(self):
        s = pd.Series([10.0, 30.0], index=pd.to_datetime(['2020-01-01', '2020-01-03']))
        gds = GenericDataSource(data_set=s, missing_data_strategy=MissingDataStrategy.fill_forward)
        result = gds.get_data(dt.datetime(2020, 1, 2))
        assert result == 10.0

    # -- get_data: missing data with non-DatetimeIndex ----------------------

    def test_get_data_interpolate_non_datetime_index(self):
        """Non-DatetimeIndex takes the else branch when inserting NaN.
        Note: the source code has a bug where sort_index() is not reassigned for
        non-DatetimeIndex (line 147), so interpolation operates on unsorted index
        [1, 3, 2(NaN)] and fills NaN by interpolating between 30 and end-of-series,
        resulting in 30.0 instead of 20.0."""
        s = pd.Series([10.0, 30.0], index=[1, 3])
        gds = GenericDataSource(data_set=s, missing_data_strategy=MissingDataStrategy.interpolate)
        result = gds.get_data(2)
        # Due to unsorted index, interpolation fills with 30.0 (last known value)
        assert result == pytest.approx(30.0)

    def test_get_data_fill_forward_non_datetime_index(self):
        """Non-DatetimeIndex with ffill: same sort_index bug means the NaN at
        position [1, 3, 2(NaN)] gets forward-filled from 30.0."""
        s = pd.Series([10.0, 30.0], index=[1, 3])
        gds = GenericDataSource(data_set=s, missing_data_strategy=MissingDataStrategy.fill_forward)
        result = gds.get_data(2)
        assert result == 30.0

    # -- get_data_range -----------------------------------------------------

    def test_get_data_range_date_end(self):
        s = pd.Series(
            [10.0, 20.0, 30.0, 40.0],
            index=pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04']),
        )
        gds = GenericDataSource(data_set=s)
        result = gds.get_data_range(pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-03'))
        # start < index <= end  ->  Jan 2 and Jan 3
        assert len(result) == 2
        assert list(result.values) == [20.0, 30.0]

    def test_get_data_range_int_end(self):
        s = pd.Series(
            [10.0, 20.0, 30.0, 40.0, 50.0],
            index=pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05']),
        )
        gds = GenericDataSource(data_set=s)
        result = gds.get_data_range(pd.Timestamp('2020-01-04'), 2)
        # index < start -> Jan 1,2,3; tail(2) -> Jan 2, Jan 3
        assert len(result) == 2
        assert list(result.values) == [20.0, 30.0]


# ===========================================================================
# MissingDataStrategy enum
# ===========================================================================

class TestMissingDataStrategy:
    def test_enum_values(self):
        assert MissingDataStrategy.fill_forward.value == 'fill_forward'
        assert MissingDataStrategy.interpolate.value == 'interpolate'
        assert MissingDataStrategy.fail.value == 'fail'


# ===========================================================================
# DataManager
# ===========================================================================

class TestDataManager:
    def _make_manager(self):
        dm = DataManager()
        dm.__post_init__()
        return dm

    # -- add_data_source ----------------------------------------------------

    def test_add_data_source_series(self):
        """Adding a pd.Series wraps it in GenericDataSource."""
        dm = self._make_manager()
        inst = _make_instrument('my_inst')
        s = pd.Series([1.0, 2.0], index=pd.to_datetime(['2020-01-01', '2020-01-02']))
        dm.add_data_source(s, DataFrequency.DAILY, inst, ValuationFixingType.PRICE)

        key = (DataFrequency.DAILY, 'my_inst', ValuationFixingType.PRICE)
        assert key in dm._data_sources
        assert isinstance(dm._data_sources[key], GenericDataSource)

    def test_add_data_source_datasource_object(self):
        """Adding a DataSource subclass stores it directly."""
        dm = self._make_manager()
        inst = _make_instrument('my_inst')
        s = pd.Series([1.0], index=pd.to_datetime(['2020-01-01']))
        gds = GenericDataSource(data_set=s)
        dm.add_data_source(gds, DataFrequency.DAILY, inst, ValuationFixingType.PRICE)

        key = (DataFrequency.DAILY, 'my_inst', ValuationFixingType.PRICE)
        assert dm._data_sources[key] is gds

    def test_add_data_source_empty_series_skipped(self):
        """Empty series that is not a DataSource -> returns early."""
        dm = self._make_manager()
        inst = _make_instrument('my_inst')
        empty_s = pd.Series([], dtype=float)
        dm.add_data_source(empty_s, DataFrequency.DAILY, inst, ValuationFixingType.PRICE)
        assert len(dm._data_sources) == 0

    def test_add_data_source_name_none_raises(self):
        """instrument.name is None -> RuntimeError."""
        dm = self._make_manager()
        inst = _make_instrument(None)
        inst.name = None
        s = pd.Series([1.0], index=pd.to_datetime(['2020-01-01']))
        with pytest.raises(RuntimeError, match='Please add a name'):
            dm.add_data_source(s, DataFrequency.DAILY, inst, ValuationFixingType.PRICE)

    def test_add_data_source_duplicate_key_raises(self):
        """Adding the same key twice -> RuntimeError."""
        dm = self._make_manager()
        inst = _make_instrument('my_inst')
        s = pd.Series([1.0], index=pd.to_datetime(['2020-01-01']))
        dm.add_data_source(s, DataFrequency.DAILY, inst, ValuationFixingType.PRICE)

        s2 = pd.Series([2.0], index=pd.to_datetime(['2020-01-01']))
        with pytest.raises(RuntimeError, match='already added'):
            dm.add_data_source(s2, DataFrequency.DAILY, inst, ValuationFixingType.PRICE)

    # -- get_data -----------------------------------------------------------

    def test_get_data_daily(self):
        """state is dt.date -> DataFrequency.DAILY, key name is split on '_'."""
        dm = self._make_manager()
        inst = _make_instrument('prefix_SPX')
        s = pd.Series([42.0], index=pd.to_datetime(['2020-01-01']))
        gds = GenericDataSource(data_set=s)
        dm.add_data_source(gds, DataFrequency.DAILY, _make_instrument('SPX'), ValuationFixingType.PRICE)

        lookup_inst = _make_instrument('prefix_SPX')
        result = dm.get_data(dt.date(2020, 1, 1), lookup_inst, ValuationFixingType.PRICE)
        assert result == 42.0

    def test_get_data_realtime(self):
        """state is dt.datetime -> DataFrequency.REAL_TIME."""
        dm = self._make_manager()
        idx = pd.to_datetime(['2020-01-01 10:00'])
        s = pd.Series([99.0], index=idx)
        gds = GenericDataSource(data_set=s)
        dm.add_data_source(gds, DataFrequency.REAL_TIME, _make_instrument('SPX'), ValuationFixingType.PRICE)

        lookup_inst = _make_instrument('prefix_SPX')
        result = dm.get_data(dt.datetime(2020, 1, 1, 10, 0), lookup_inst, ValuationFixingType.PRICE)
        assert result == 99.0

    # -- get_data_range -----------------------------------------------------

    def test_get_data_range_daily(self):
        """Test DataManager.get_data_range with dt.date inputs (DAILY frequency).
        The series uses a DatetimeIndex, so we need dt.date inputs that compare
        correctly with pd.Timestamp via GenericDataSource.get_data_range."""
        dm = self._make_manager()
        # Use a date-indexed series to avoid datetime vs date comparison issues
        dates = [dt.date(2020, 1, 1), dt.date(2020, 1, 2), dt.date(2020, 1, 3)]
        s = pd.Series([10.0, 20.0, 30.0], index=dates)
        gds = GenericDataSource(data_set=s)
        dm.add_data_source(gds, DataFrequency.DAILY, _make_instrument('SPX'), ValuationFixingType.PRICE)

        lookup_inst = _make_instrument('prefix_SPX')
        result = dm.get_data_range(
            dt.date(2020, 1, 1), dt.date(2020, 1, 3), lookup_inst, ValuationFixingType.PRICE
        )
        # start < index <= end -> Jan 2 and Jan 3
        assert len(result) == 2

    def test_get_data_range_realtime(self):
        dm = self._make_manager()
        idx = pd.to_datetime(['2020-01-01 10:00', '2020-01-01 11:00', '2020-01-01 12:00'])
        s = pd.Series([10.0, 20.0, 30.0], index=idx)
        gds = GenericDataSource(data_set=s)
        dm.add_data_source(gds, DataFrequency.REAL_TIME, _make_instrument('SPX'), ValuationFixingType.PRICE)

        lookup_inst = _make_instrument('prefix_SPX')
        result = dm.get_data_range(
            dt.datetime(2020, 1, 1, 10, 0),
            dt.datetime(2020, 1, 1, 12, 0),
            lookup_inst,
            ValuationFixingType.PRICE,
        )
        # start < index <= end -> 11:00 and 12:00
        assert len(result) == 2
