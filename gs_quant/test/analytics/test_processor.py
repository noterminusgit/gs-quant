"""
Tests for gs_quant/analytics/core/processor.py
Covers BaseProcessor, DataQueryInfo, MeasureQueryInfo, and all branch paths.
"""

import asyncio
import datetime as dt
import functools
from collections import defaultdict
from typing import Optional, List, Union
from unittest.mock import MagicMock, patch, AsyncMock

import numpy as np
import pandas as pd
import pytest

from gs_quant.analytics.common import (
    TYPE, PROCESSOR, PARAMETERS, DATA_COORDINATE, ENTITY, VALUE, DATE, DATETIME,
    PROCESSOR_NAME, ENTITY_ID, ENTITY_TYPE, PARAMETER, REFERENCE, RELATIVE_DATE, LIST,
)
from gs_quant.analytics.common.enumerators import ScaleShape
from gs_quant.analytics.core.processor import (
    BaseProcessor,
    DataCoordinateOrProcessor,
    DataQueryInfo,
    MeasureQueryInfo,
    DateOrDatetimeOrRDate,
    PARSABLE_OBJECT_MAP,
)
from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.data import DataCoordinate, DataFrequency
from gs_quant.data.query import DataQuery, DataQueryType
from gs_quant.datetime.relative_date import RelativeDate
from gs_quant.entities.entity import Entity


# ---------------------------------------------------------------------------
# Concrete subclass for testing the abstract BaseProcessor
# ---------------------------------------------------------------------------
class ConcreteProcessor(BaseProcessor):
    """Minimal concrete subclass for testing BaseProcessor."""

    def __init__(
        self,
        a: DataCoordinateOrProcessor = None,
        b: DataCoordinateOrProcessor = None,
        *,
        start: Optional[DateOrDatetimeOrRDate] = None,
        end: Optional[DateOrDatetimeOrRDate] = None,
        entity_param: Optional[Entity] = None,
        enum_param: Optional[ScaleShape] = None,
        date_param: Optional[dt.date] = None,
        datetime_param: Optional[dt.datetime] = None,
        rdate_param: Optional[RelativeDate] = None,
        list_param: Optional[List] = None,
        float_param: Optional[float] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.children['a'] = a
        self.children['b'] = b
        self.start = start
        self.end = end
        self.entity_param = entity_param
        self.enum_param = enum_param
        self.date_param = date_param
        self.datetime_param = datetime_param
        self.rdate_param = rdate_param
        self.list_param = list_param
        self.float_param = float_param

    def process(self, *args):
        a_data = self.children_data.get('a')
        if isinstance(a_data, ProcessorResult) and a_data.success:
            self.value = ProcessorResult(True, a_data.data)
        return self.value

    def get_plot_expression(self):
        return None


class FailingProcessor(BaseProcessor):
    """Processor that raises during process()."""

    def __init__(self, a: DataCoordinateOrProcessor = None, **kwargs):
        super().__init__(**kwargs)
        if a is not None:
            self.children['a'] = a

    def process(self, *args):
        raise ValueError('intentional failure')

    def get_plot_expression(self):
        return None


class MeasureConcreteProcessor(BaseProcessor):
    """Processor with measure_processor=True for testing."""

    def __init__(self, a: DataCoordinateOrProcessor = None, **kwargs):
        kwargs['measure_processor'] = True
        super().__init__(**kwargs)
        if a is not None:
            self.children['a'] = a

    def process(self, entity=None):
        a_data = self.children_data.get('a')
        if isinstance(a_data, ProcessorResult) and a_data.success:
            self.value = ProcessorResult(True, a_data.data)
        return self.value

    def get_plot_expression(self):
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_series(dates=None, values=None):
    """Create a pd.Series with datetime index."""
    if dates is None:
        dates = pd.date_range('2021-01-01', periods=5)
    if values is None:
        values = range(len(dates))
    return pd.Series(values, index=dates)


def _make_entity(entity_id='test_id', entity_type_value='Asset'):
    entity = MagicMock(spec=Entity)
    entity.get_marquee_id.return_value = entity_id
    entity.entity_type.return_value = MagicMock(value=entity_type_value)
    return entity


def _make_rdate_entity_map():
    return {}


# ---------------------------------------------------------------------------
# DataQueryInfo / MeasureQueryInfo dataclass tests
# ---------------------------------------------------------------------------
class TestDataQueryInfo:
    def test_creation(self):
        proc = ConcreteProcessor()
        entity = _make_entity()
        query = MagicMock(spec=DataQuery)
        info = DataQueryInfo(attr='a', processor=proc, query=query, entity=entity)
        assert info.attr == 'a'
        assert info.processor is proc
        assert info.entity is entity
        assert info.data is None

    def test_data_default_none(self):
        info = DataQueryInfo(attr='x', processor=MagicMock(), query=MagicMock(), entity=MagicMock())
        assert info.data is None

    def test_data_can_be_set(self):
        s = _make_series()
        info = DataQueryInfo(attr='x', processor=MagicMock(), query=MagicMock(), entity=MagicMock(), data=s)
        pd.testing.assert_series_equal(info.data, s)


class TestMeasureQueryInfo:
    def test_creation(self):
        proc = ConcreteProcessor()
        entity = _make_entity()
        info = MeasureQueryInfo(attr='a', processor=proc, entity=entity)
        assert info.attr == 'a'
        assert info.processor is proc
        assert info.entity is entity


# ---------------------------------------------------------------------------
# BaseProcessor.__init__
# ---------------------------------------------------------------------------
class TestBaseProcessorInit:
    def test_default_values(self):
        proc = ConcreteProcessor()
        assert proc.value == ProcessorResult(False, 'Value not set')
        assert proc.parent is None
        assert proc.parent_attr is None
        assert proc.data_cell is None
        assert proc.last_value is False
        assert proc.measure_processor is False

    def test_last_value_kwarg(self):
        proc = ConcreteProcessor(last_value=True)
        assert proc.last_value is True

    def test_measure_processor_kwarg(self):
        proc = MeasureConcreteProcessor()
        assert proc.measure_processor is True

    def test_id_format(self):
        proc = ConcreteProcessor()
        assert proc.id.startswith('ConcreteProcessor-')


# ---------------------------------------------------------------------------
# post_process
# ---------------------------------------------------------------------------
class TestPostProcess:
    def test_last_value_false_no_change(self):
        proc = ConcreteProcessor()
        series = _make_series()
        proc.value = ProcessorResult(True, series)
        proc.post_process()
        assert len(proc.value.data) == 5  # unchanged

    def test_last_value_true_trims_to_last(self):
        proc = ConcreteProcessor(last_value=True)
        series = _make_series(values=[10, 20, 30, 40, 50])
        proc.value = ProcessorResult(True, series)
        proc.post_process()
        assert len(proc.value.data) == 1
        assert proc.value.data.iloc[0] == 50

    def test_last_value_true_but_not_success(self):
        proc = ConcreteProcessor(last_value=True)
        proc.value = ProcessorResult(False, 'error')
        proc.post_process()
        assert proc.value.data == 'error'

    def test_last_value_true_but_not_series(self):
        proc = ConcreteProcessor(last_value=True)
        proc.value = ProcessorResult(True, 'not a series')
        proc.post_process()
        assert proc.value.data == 'not a series'

    def test_last_value_true_empty_series(self):
        proc = ConcreteProcessor(last_value=True)
        proc.value = ProcessorResult(True, pd.Series(dtype=float))
        proc.post_process()
        assert proc.value.data.empty

    def test_last_value_true_but_not_processor_result(self):
        proc = ConcreteProcessor(last_value=True)
        proc.value = 'not a ProcessorResult'
        proc.post_process()
        assert proc.value == 'not a ProcessorResult'


# ---------------------------------------------------------------------------
# __handle_date_range
# ---------------------------------------------------------------------------
class TestHandleDateRange:
    def _setup_proc_with_dates(self, start=None, end=None):
        proc = ConcreteProcessor(start=start, end=end)
        proc.data_cell = MagicMock()
        proc.data_cell.entity = _make_entity('eid')
        return proc

    def test_not_processor_result(self):
        proc = self._setup_proc_with_dates(start=dt.date(2021, 1, 2))
        # Should not raise, just return
        proc._BaseProcessor__handle_date_range('not a result', {})

    def test_failed_result(self):
        proc = self._setup_proc_with_dates(start=dt.date(2021, 1, 2))
        result = ProcessorResult(False, 'error')
        proc._BaseProcessor__handle_date_range(result, {})
        assert result.data == 'error'

    def test_no_start_no_end(self):
        proc = self._setup_proc_with_dates()
        series = _make_series()
        result = ProcessorResult(True, series)
        proc._BaseProcessor__handle_date_range(result, {})
        assert len(result.data) == 5  # unchanged

    def test_start_and_end_dates(self):
        proc = self._setup_proc_with_dates(
            start=dt.date(2021, 1, 2),
            end=dt.date(2021, 1, 4),
        )
        series = _make_series()
        result = ProcessorResult(True, series)
        proc._BaseProcessor__handle_date_range(result, {})
        assert all(result.data.index >= np.datetime64(dt.date(2021, 1, 2)))
        assert all(result.data.index <= np.datetime64(dt.date(2021, 1, 4)))

    def test_start_only(self):
        proc = self._setup_proc_with_dates(start=dt.date(2021, 1, 3))
        series = _make_series()
        result = ProcessorResult(True, series)
        proc._BaseProcessor__handle_date_range(result, {})
        assert all(result.data.index >= np.datetime64(dt.date(2021, 1, 3)))

    def test_end_only(self):
        """Bug 1 fix: end-only uses <= not >=."""
        proc = self._setup_proc_with_dates(end=dt.date(2021, 1, 3))
        series = _make_series()
        result = ProcessorResult(True, series)
        proc._BaseProcessor__handle_date_range(result, {})
        assert all(result.data.index <= np.datetime64(dt.date(2021, 1, 3)))

    def test_start_and_end_with_relative_dates(self):
        start_rdate = MagicMock(spec=RelativeDate)
        start_rdate.rule = '-5d'
        start_rdate.base_date_passed_in = False
        end_rdate = MagicMock(spec=RelativeDate)
        end_rdate.rule = '-1d'
        end_rdate.base_date_passed_in = False

        proc = self._setup_proc_with_dates(start=start_rdate, end=end_rdate)

        rdate_map = {
            'eid::-5d::None': dt.date(2021, 1, 2),
            'eid::-1d::None': dt.date(2021, 1, 4),
        }
        series = _make_series()
        result = ProcessorResult(True, series)
        proc._BaseProcessor__handle_date_range(result, rdate_map)
        assert all(result.data.index >= np.datetime64(dt.date(2021, 1, 2)))
        assert all(result.data.index <= np.datetime64(dt.date(2021, 1, 4)))

    def test_start_only_relative_date(self):
        start_rdate = MagicMock(spec=RelativeDate)
        start_rdate.rule = '-3d'
        start_rdate.base_date_passed_in = False

        proc = self._setup_proc_with_dates(start=start_rdate)
        rdate_map = {'eid::-3d::None': dt.date(2021, 1, 3)}
        series = _make_series()
        result = ProcessorResult(True, series)
        proc._BaseProcessor__handle_date_range(result, rdate_map)
        assert all(result.data.index >= np.datetime64(dt.date(2021, 1, 3)))

    def test_end_only_relative_date(self):
        end_rdate = MagicMock(spec=RelativeDate)
        end_rdate.rule = '-2d'
        end_rdate.base_date_passed_in = False

        proc = self._setup_proc_with_dates(end=end_rdate)
        rdate_map = {'eid::-2d::None': dt.date(2021, 1, 3)}
        series = _make_series()
        result = ProcessorResult(True, series)
        proc._BaseProcessor__handle_date_range(result, rdate_map)
        assert all(result.data.index <= np.datetime64(dt.date(2021, 1, 3)))

    def test_entity_is_string(self):
        """When entity is a string, entity_id defaults to ''."""
        proc = ConcreteProcessor(start=dt.date(2021, 1, 2), end=dt.date(2021, 1, 4))
        proc.data_cell = MagicMock()
        proc.data_cell.entity = 'string_entity'  # not a mock with get_marquee_id

        series = _make_series()
        result = ProcessorResult(True, series)
        proc._BaseProcessor__handle_date_range(result, {})
        # Should use entity_id='' and still filter
        assert all(result.data.index >= np.datetime64(dt.date(2021, 1, 2)))


# ---------------------------------------------------------------------------
# update (async)
# ---------------------------------------------------------------------------
class TestUpdate:
    @pytest.mark.asyncio
    async def test_success_no_pool(self):
        proc = ConcreteProcessor()
        series = _make_series()
        result = ProcessorResult(True, series)
        await proc.update('a', result, {})
        assert proc.children_data['a'] is result
        assert proc.value.success is True

    @pytest.mark.asyncio
    async def test_failure_result(self):
        proc = ConcreteProcessor()
        result = ProcessorResult(False, 'error')
        await proc.update('a', result, {})
        assert proc.value is result

    @pytest.mark.asyncio
    async def test_not_processor_result(self):
        proc = ConcreteProcessor()
        await proc.update('a', 'not a result', {})
        assert proc.children_data['a'] == 'not a result'
        # value should still be the default since we skip processing
        assert proc.value == ProcessorResult(False, 'Value not set')

    @pytest.mark.asyncio
    async def test_exception_in_process(self):
        proc = FailingProcessor()
        result = ProcessorResult(True, _make_series())
        await proc.update('a', result, {})
        assert proc.value.success is False
        assert 'intentional failure' in proc.value.data

    @pytest.mark.asyncio
    async def test_with_pool_no_measure(self):
        proc = ConcreteProcessor()
        series = _make_series()
        result = ProcessorResult(True, series)

        mock_pool = MagicMock()
        loop = asyncio.get_running_loop()

        # Mock run_in_executor to call process directly
        async def mock_run_in_executor(pool, fn):
            return fn()

        with patch.object(loop, 'run_in_executor', side_effect=mock_run_in_executor):
            await proc.update('a', result, {}, pool=mock_pool)

        assert proc.value.success is True

    @pytest.mark.asyncio
    async def test_with_pool_measure_processor(self):
        proc = MeasureConcreteProcessor()
        entity = _make_entity()
        series = _make_series()
        result = ProcessorResult(True, series)
        query_info = MeasureQueryInfo(attr='a', processor=proc, entity=entity)

        mock_pool = MagicMock()
        loop = asyncio.get_running_loop()

        async def mock_run_in_executor(pool, fn):
            return fn()

        with patch.object(loop, 'run_in_executor', side_effect=mock_run_in_executor):
            await proc.update('a', result, {}, pool=mock_pool, query_info=query_info)

        assert proc.value.success is True

    @pytest.mark.asyncio
    async def test_no_pool_measure_processor(self):
        proc = MeasureConcreteProcessor()
        entity = _make_entity()
        series = _make_series()
        result = ProcessorResult(True, series)
        query_info = MeasureQueryInfo(attr='a', processor=proc, entity=entity)

        await proc.update('a', result, {}, query_info=query_info)
        assert proc.value.success is True

    @pytest.mark.asyncio
    async def test_measure_processor_skips_date_range(self):
        """When measure_processor=True, __handle_date_range is not called."""
        proc = MeasureConcreteProcessor()
        proc.start = dt.date(2021, 1, 3)  # Would filter if handle_date_range ran
        series = _make_series()
        result = ProcessorResult(True, series)
        entity = _make_entity()
        query_info = MeasureQueryInfo(attr='a', processor=proc, entity=entity)

        await proc.update('a', result, {}, query_info=query_info)
        # data_cell not set, so handle_date_range would fail if called
        assert proc.children_data['a'] is result

    @pytest.mark.asyncio
    async def test_post_process_called_on_success(self):
        proc = ConcreteProcessor(last_value=True)
        series = _make_series(values=[10, 20, 30, 40, 50])
        result = ProcessorResult(True, series)
        await proc.update('a', result, {})
        # post_process should have trimmed to last value
        assert len(proc.value.data) == 1


# ---------------------------------------------------------------------------
# build_graph
# ---------------------------------------------------------------------------
class TestBuildGraph:
    def test_data_coordinate_daily(self):
        coord = DataCoordinate(measure='price', dataset_id='DS', frequency=DataFrequency.DAILY)
        proc = ConcreteProcessor(a=coord)
        entity = _make_entity()
        cell = MagicMock()
        queries = []
        rdate_map = defaultdict(set)

        proc.build_graph(entity, cell, queries, rdate_map, overrides=None)

        assert proc.data_cell is cell
        assert len(queries) == 1
        assert isinstance(queries[0], DataQueryInfo)
        assert queries[0].attr == 'a'
        assert queries[0].query.query_type == DataQueryType.RANGE

    def test_data_coordinate_real_time(self):
        coord = DataCoordinate(measure='price', dataset_id='DS', frequency=DataFrequency.REAL_TIME)
        proc = ConcreteProcessor(a=coord)
        entity = _make_entity()
        queries = []
        rdate_map = defaultdict(set)

        proc.build_graph(entity, MagicMock(), queries, rdate_map, overrides=None)

        assert len(queries) == 1
        assert queries[0].query.query_type == DataQueryType.LAST

    def test_child_is_processor(self):
        inner = ConcreteProcessor()
        outer = ConcreteProcessor(a=inner)
        entity = _make_entity()
        queries = []
        rdate_map = defaultdict(set)

        outer.build_graph(entity, MagicMock(), queries, rdate_map, overrides=None)

        assert inner.parent is outer
        assert inner.parent_attr == 'a'

    def test_child_is_data_query_info(self):
        query = MagicMock(spec=DataQuery)
        dqi = DataQueryInfo(attr='x', processor=None, query=query, entity=MagicMock())
        proc = ConcreteProcessor()
        proc.children['a'] = dqi

        entity = _make_entity()
        queries = []
        rdate_map = defaultdict(set)

        proc.build_graph(entity, MagicMock(), queries, rdate_map, overrides=None)

        assert len(queries) == 1
        assert queries[0] is dqi
        assert dqi.processor is proc

    def test_measure_processor_appends_measure_query(self):
        proc = MeasureConcreteProcessor()
        entity = _make_entity()
        queries = []
        rdate_map = defaultdict(set)

        proc.build_graph(entity, MagicMock(), queries, rdate_map, overrides=None)

        assert len(queries) == 1
        assert isinstance(queries[0], MeasureQueryInfo)

    def test_overrides_with_matching_coordinate_id(self):
        coord = DataCoordinate(measure='price', dataset_id='DS', frequency=DataFrequency.DAILY)
        proc = ConcreteProcessor(a=coord)

        override = MagicMock()
        override.coordinate = coord
        override.coordinate_id = coord.id
        override.dimensions = {'asset_id': 'MA123'}

        entity = _make_entity()
        queries = []
        rdate_map = defaultdict(set)

        proc.build_graph(entity, MagicMock(), queries, rdate_map, overrides=[override])
        # set_dimensions should have been called with the override
        assert len(queries) == 1

    def test_overrides_with_none_coordinate_id_uses_default(self):
        coord = DataCoordinate(measure='price', dataset_id='DS', frequency=DataFrequency.DAILY)
        proc = ConcreteProcessor(a=coord)

        override = MagicMock()
        override.coordinate = coord
        override.coordinate_id = None
        override.dimensions = {'asset_id': 'DEFAULT'}

        entity = _make_entity()
        queries = []
        rdate_map = defaultdict(set)

        proc.build_graph(entity, MagicMock(), queries, rdate_map, overrides=[override])
        assert len(queries) == 1

    def test_overrides_no_matching_coord(self):
        coord = DataCoordinate(measure='price', dataset_id='DS', frequency=DataFrequency.DAILY)
        proc = ConcreteProcessor(a=coord)

        override = MagicMock()
        override.coordinate = MagicMock()  # different coordinate
        override.coordinate_id = 'other_id'
        override.dimensions = {'asset_id': 'OTHER'}

        entity = _make_entity()
        queries = []
        rdate_map = defaultdict(set)

        proc.build_graph(entity, MagicMock(), queries, rdate_map, overrides=[override])
        assert len(queries) == 1

    def test_add_required_rdates(self):
        start_rdate = MagicMock(spec=RelativeDate)
        start_rdate.rule = '-5d'
        start_rdate.base_date_passed_in = True
        start_rdate.base_date = dt.date(2021, 6, 1)

        end_rdate = MagicMock(spec=RelativeDate)
        end_rdate.rule = '-1d'
        end_rdate.base_date_passed_in = False

        proc = ConcreteProcessor(start=start_rdate, end=end_rdate)
        entity = _make_entity('eid')
        queries = []
        rdate_map = defaultdict(set)

        proc.build_graph(entity, MagicMock(), queries, rdate_map, overrides=None)

        assert 'eid' in rdate_map
        tuples = rdate_map['eid']
        assert ('-5d', '2021-06-01') in tuples
        assert ('-1d', None) in tuples

    def test_add_required_rdates_entity_is_not_entity_type(self):
        """When entity is not Entity instance, entity_id defaults to ''."""
        start_rdate = MagicMock(spec=RelativeDate)
        start_rdate.rule = '-1d'
        start_rdate.base_date_passed_in = False

        proc = ConcreteProcessor(start=start_rdate)
        queries = []
        rdate_map = defaultdict(set)

        # Pass a string instead of Entity
        proc.build_graph('string_entity', MagicMock(), queries, rdate_map, overrides=None)
        assert '' in rdate_map

    def test_nested_build_graph(self):
        """Nested processors: outer -> inner with DataCoordinate."""
        coord = DataCoordinate(measure='price', dataset_id='DS', frequency=DataFrequency.DAILY)
        inner = ConcreteProcessor(a=coord)
        outer = ConcreteProcessor(a=inner)

        entity = _make_entity()
        queries = []
        rdate_map = defaultdict(set)

        outer.build_graph(entity, MagicMock(), queries, rdate_map, overrides=None)

        assert len(queries) == 1
        assert isinstance(queries[0], DataQueryInfo)
        assert inner.parent is outer

    def test_build_graph_with_start_end_on_daily_coord(self):
        """Start/end from processor attrs are passed to DataQuery."""
        coord = DataCoordinate(measure='price', dataset_id='DS', frequency=DataFrequency.DAILY)
        start = dt.date(2021, 1, 1)
        end = dt.date(2021, 6, 1)
        proc = ConcreteProcessor(a=coord, start=start, end=end)

        entity = _make_entity()
        queries = []
        rdate_map = defaultdict(set)

        proc.build_graph(entity, MagicMock(), queries, rdate_map, overrides=None)

        assert queries[0].query.start == start
        assert queries[0].query.end == end


# ---------------------------------------------------------------------------
# calculate (async)
# ---------------------------------------------------------------------------
class TestCalculate:
    @pytest.mark.asyncio
    async def test_no_parent(self):
        proc = ConcreteProcessor()
        series = _make_series()
        result = ProcessorResult(True, series)
        await proc.calculate('a', result, {})
        assert proc.value.success is True

    @pytest.mark.asyncio
    async def test_parent_is_base_processor(self):
        child = ConcreteProcessor()
        parent = ConcreteProcessor()
        child.parent = parent
        child.parent_attr = 'a'
        child.data_cell = MagicMock()

        series = _make_series()
        result = ProcessorResult(True, series)

        await child.calculate('a', result, {})
        # Parent should have been called with child's value
        assert parent.children_data.get('a') is not None

    @pytest.mark.asyncio
    async def test_parent_is_data_cell(self):
        child = ConcreteProcessor()
        data_cell = MagicMock()
        child.parent = data_cell  # Not a BaseProcessor
        child.parent_attr = 'a'
        child.data_cell = data_cell

        series = _make_series()
        result = ProcessorResult(True, series)

        await child.calculate('a', result, {})
        data_cell.update.assert_called_once()

    @pytest.mark.asyncio
    async def test_value_not_successful_sets_data_cell_error(self):
        child = ConcreteProcessor()
        data_cell = MagicMock()
        parent_proc = MagicMock()
        child.parent = parent_proc
        child.parent_attr = 'a'
        child.data_cell = data_cell

        result = ProcessorResult(False, 'error msg')

        await child.calculate('a', result, {})
        # data_cell.value should be set to the error
        assert data_cell.value.success is False
        assert data_cell.updated_time is not None

    @pytest.mark.asyncio
    async def test_with_pool(self):
        proc = ConcreteProcessor()
        series = _make_series()
        result = ProcessorResult(True, series)

        mock_pool = MagicMock()
        loop = asyncio.get_running_loop()

        async def mock_run_in_executor(pool, fn):
            return fn()

        with patch.object(loop, 'run_in_executor', side_effect=mock_run_in_executor):
            await proc.calculate('a', result, {}, pool=mock_pool)

        assert proc.value.success is True

    @pytest.mark.asyncio
    async def test_value_not_processor_result_does_not_traverse(self):
        """If value is not ProcessorResult after update, don't traverse parent."""
        child = ConcreteProcessor()
        parent = MagicMock()
        child.parent = parent
        child.parent_attr = 'a'
        child.data_cell = MagicMock()

        # Make update set value to something that's not a ProcessorResult
        result = ProcessorResult(True, _make_series())

        # Override process to set a non-ProcessorResult value
        original_process = child.process
        def bad_process(*args):
            child.value = 'not a ProcessorResult'
            return child.value
        child.process = bad_process

        await child.calculate('a', result, {})
        # parent.calculate should NOT have been called
        parent.calculate.assert_not_called()


# ---------------------------------------------------------------------------
# as_dict
# ---------------------------------------------------------------------------
class TestAsDict:
    def test_basic_processor(self):
        coord = DataCoordinate(measure='price', dataset_id='DS', frequency=DataFrequency.DAILY)
        proc = ConcreteProcessor(a=coord)
        result = proc.as_dict()

        assert result[TYPE] == PROCESSOR
        assert result[PROCESSOR_NAME] == 'ConcreteProcessor'
        assert PARAMETERS in result

    def test_child_is_processor(self):
        inner = ConcreteProcessor()
        outer = ConcreteProcessor(a=inner)
        result = outer.as_dict()

        params = result[PARAMETERS]
        assert 'a' in params
        assert params['a'][TYPE] == PROCESSOR

    def test_child_is_data_coordinate(self):
        coord = DataCoordinate(measure='price', dataset_id='DS', frequency=DataFrequency.DAILY)
        proc = ConcreteProcessor(a=coord)
        result = proc.as_dict()

        params = result[PARAMETERS]
        assert 'a' in params
        assert params['a'][TYPE] == DATA_COORDINATE

    def test_entity_param(self):
        entity = MagicMock(spec=Entity)
        entity.get_marquee_id.return_value = 'eid'
        entity.entity_type.return_value = MagicMock(value='Asset')
        proc = ConcreteProcessor(entity_param=entity)
        result = proc.as_dict()

        params = result[PARAMETERS]
        assert 'entity_param' in params
        assert params['entity_param'][TYPE] == ENTITY
        assert params['entity_param'][ENTITY_ID] == 'eid'

    def test_enum_param(self):
        proc = ConcreteProcessor(enum_param=ScaleShape.DIAMOND)
        result = proc.as_dict()

        params = result[PARAMETERS]
        assert 'enum_param' in params
        assert params['enum_param'][VALUE] == 'diamond'

    def test_date_param(self):
        proc = ConcreteProcessor(date_param=dt.date(2021, 6, 15))
        result = proc.as_dict()

        params = result[PARAMETERS]
        assert 'date_param' in params
        assert params['date_param'][VALUE] == '2021-06-15'

    def test_datetime_param(self):
        # Note: dt.datetime is a subclass of dt.date, so the code's isinstance(attribute, dt.date)
        # check at line 345 catches both. The datetime is formatted via str().
        proc = ConcreteProcessor(datetime_param=dt.datetime(2021, 6, 15, 10, 30, 0, 123456))
        result = proc.as_dict()

        params = result[PARAMETERS]
        assert 'datetime_param' in params
        # str(dt.datetime(...)) produces '2021-06-15 10:30:00.123456'
        assert '2021-06-15' in params['datetime_param'][VALUE]

    def test_builtin_param(self):
        proc = ConcreteProcessor(float_param=3.14)
        result = proc.as_dict()

        params = result[PARAMETERS]
        assert 'float_param' in params
        assert params['float_param'][VALUE] == 3.14

    def test_none_attribute_skipped_for_simple_types(self):
        proc = ConcreteProcessor()
        result = proc.as_dict()
        params = result[PARAMETERS]
        # None attributes should not appear
        assert 'float_param' not in params
        assert 'enum_param' not in params

    def test_none_child_skipped(self):
        proc = ConcreteProcessor()
        proc.children['a'] = None  # explicitly set to None
        result = proc.as_dict()
        params = result[PARAMETERS]
        assert 'a' not in params

    def test_last_value_in_default_params(self):
        proc = ConcreteProcessor(last_value=True)
        result = proc.as_dict()
        params = result[PARAMETERS]
        assert 'last_value' in params
        assert params['last_value']['value'] is True

    def test_last_value_false_not_in_default_params(self):
        proc = ConcreteProcessor()
        result = proc.as_dict()
        params = result[PARAMETERS]
        assert 'last_value' not in params

    def test_list_param(self):
        inner1 = ConcreteProcessor()
        inner2 = ConcreteProcessor()
        proc = ConcreteProcessor(list_param=[inner1, inner2])
        result = proc.as_dict()

        params = result[PARAMETERS]
        assert 'list_param' in params
        assert params['list_param'][VALUE] is not None
        assert len(params['list_param'][VALUE]) == 2

    def test_rdate_param_as_dict(self):
        """RelativeDate has as_dict, goes to else branch."""
        rdate = MagicMock()
        rdate.as_dict.return_value = {'rule': '-1d', 'baseDate': None}
        proc = ConcreteProcessor(rdate_param=rdate)
        type(rdate).__name__ = 'RelativeDate'
        result = proc.as_dict()

        params = result[PARAMETERS]
        assert 'rdate_param' in params

    def test_child_unknown_type_skipped(self):
        """When child is not BaseProcessor or DataCoordinate, continue."""
        proc = ConcreteProcessor()
        proc.children['a'] = 12345  # not a recognized type
        result = proc.as_dict()
        params = result[PARAMETERS]
        # 'a' should exist (dict created) but no TYPE set, just empty after continue
        assert 'a' in params


# ---------------------------------------------------------------------------
# from_dict
# ---------------------------------------------------------------------------
class TestFromDict:
    def test_data_coordinate_parameter(self):
        """Parameter type dataCoordinate should create DataCoordinate."""
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_processor_parameter(self):
        """Nested processor parameter type."""
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: PROCESSOR,
                    PROCESSOR_NAME: 'LastProcessor',
                    PARAMETERS: {
                        'a': {
                            TYPE: DATA_COORDINATE,
                            'measure': 'price',
                            'frequency': 'daily',
                            'dataSetId': 'DS1',
                            'dimensions': {},
                        },
                    },
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_entity_parameter(self):
        """Entity parameters add to reference list."""
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
                'my_entity': {
                    TYPE: ENTITY,
                    ENTITY_ID: 'eid123',
                    ENTITY_TYPE: 'Asset',
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert len(ref_list) == 1
        assert ref_list[0][ENTITY_ID] == 'eid123'
        assert ref_list[0][REFERENCE] is result

    def test_date_parameter(self):
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
                'start': {
                    TYPE: DATE,
                    VALUE: '2021-06-15',
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_datetime_parameter(self):
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
                'start': {
                    TYPE: DATETIME,
                    VALUE: '2021-06-15T10:30:00.000Z',
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_relative_date_parameter_with_base_date(self):
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
                'start': {
                    TYPE: RELATIVE_DATE,
                    VALUE: {'rule': '-1d', 'baseDate': '2021-06-15'},
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_relative_date_parameter_without_base_date(self):
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
                'start': {
                    TYPE: RELATIVE_DATE,
                    VALUE: {'rule': '-1d', 'baseDate': None},
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_parsable_enum_parameter(self):
        """ScaleShape is in PARSABLE_OBJECT_MAP and is an Enum."""
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
                'scaleShape': {
                    TYPE: 'scaleShape',
                    VALUE: 'diamond',
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_parsable_object_from_dict(self):
        """Window is in PARSABLE_OBJECT_MAP and uses from_dict."""
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
                'window': {
                    TYPE: 'window',
                    VALUE: {'w': 5, 'r': 0},
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_list_parameter(self):
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
                'items': {
                    TYPE: LIST,
                    'value': [
                        {
                            TYPE: PROCESSOR,
                            PROCESSOR_NAME: 'LastProcessor',
                            PARAMETERS: {
                                'a': {
                                    TYPE: DATA_COORDINATE,
                                    'measure': 'price',
                                    'frequency': 'daily',
                                    'dataSetId': 'DS1',
                                    'dimensions': {},
                                },
                            },
                        },
                        'plain_string',
                    ],
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_builtin_type_parameter(self):
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {
                'a': {
                    TYPE: DATA_COORDINATE,
                    'measure': 'price',
                    'frequency': 'daily',
                    'dataSetId': 'DS1',
                    'dimensions': {},
                },
                'addend': {
                    TYPE: 'float',
                    VALUE: 3.14,
                },
            },
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is not None

    def test_unknown_processor_returns_none(self):
        """Non-existent processor name should return None."""
        obj = {
            PROCESSOR_NAME: 'NonExistentProcessor',
            PARAMETERS: {},
        }
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is None

    def test_empty_parameters(self):
        obj = {
            PROCESSOR_NAME: 'LastProcessor',
            PARAMETERS: {},
        }
        ref_list = []
        # LastProcessor requires 'a', so this will raise
        with pytest.raises(TypeError):
            BaseProcessor.from_dict(obj, ref_list)

    def test_no_processor_name(self):
        """Missing processorName defaults to 'None' string."""
        obj = {PARAMETERS: {}}
        ref_list = []
        result = BaseProcessor.from_dict(obj, ref_list)
        assert result is None


# ---------------------------------------------------------------------------
# get_default_params
# ---------------------------------------------------------------------------
class TestGetDefaultParams:
    def test_last_value_true(self):
        proc = ConcreteProcessor(last_value=True)
        params = proc.get_default_params()
        assert 'last_value' in params
        assert params['last_value'] == dict(type='bool', value=True)

    def test_last_value_false(self):
        proc = ConcreteProcessor()
        params = proc.get_default_params()
        assert params == {}


# ---------------------------------------------------------------------------
# PARSABLE_OBJECT_MAP
# ---------------------------------------------------------------------------
class TestParsableObjectMap:
    def test_contains_expected_keys(self):
        assert 'window' in PARSABLE_OBJECT_MAP
        assert 'returns' in PARSABLE_OBJECT_MAP
        assert 'currency' in PARSABLE_OBJECT_MAP
        assert 'scaleShape' in PARSABLE_OBJECT_MAP
