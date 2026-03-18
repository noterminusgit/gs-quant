"""
Tests for gs_quant.analytics.datagrid.data_cell
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gs_quant.analytics.common import DATA_CELL_NOT_CALCULATED
from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.analytics.datagrid.data_cell import DataCell


def _make_processor():
    proc = MagicMock()
    proc.build_graph = MagicMock()
    return proc


def _make_entity():
    entity = MagicMock()
    entity.get_marquee_id.return_value = 'test_id'
    entity.entity_type.return_value = MagicMock(value='Asset')
    return entity


class TestDataCellConstruction:
    def test_defaults(self):
        proc = _make_processor()
        entity = _make_entity()
        cell = DataCell('col1', proc, entity, [], column_index=0, row_index=0)

        assert cell.name == 'col1'
        assert cell.entity is entity
        assert cell.column_index == 0
        assert cell.row_index == 0
        assert cell.row_group is None
        assert cell.updated_time is None
        assert cell.value.success is False
        assert cell.value.data == DATA_CELL_NOT_CALCULATED
        assert cell.data_queries == []

    def test_deep_copies_processor(self):
        proc = _make_processor()
        entity = _make_entity()
        cell = DataCell('col1', proc, entity, [], column_index=0, row_index=0)
        # processor should be deep-copied, so not the same object
        assert cell.processor is not proc

    def test_row_group_set(self):
        proc = _make_processor()
        entity = _make_entity()
        cell = DataCell('col1', proc, entity, [], column_index=0, row_index=0, row_group='group_a')
        assert cell.row_group == 'group_a'


class TestBuildCellGraph:
    def test_processor_exists(self):
        proc = _make_processor()
        entity = _make_entity()
        cell = DataCell('col1', proc, entity, [], column_index=0, row_index=0)
        all_queries = []
        rdate_map = {}
        cell.build_cell_graph(all_queries, rdate_map)
        # build_graph should have been called on the deep-copied processor
        cell.processor.build_graph.assert_called_once()
        assert cell.processor.parent is cell

    def test_processor_is_none(self):
        entity = _make_entity()
        cell = DataCell('col1', MagicMock(), entity, [], column_index=0, row_index=0)
        cell.processor = None
        all_queries = []
        rdate_map = {}
        # Should not raise
        cell.build_cell_graph(all_queries, rdate_map)
        assert cell.data_queries == []


class TestUpdate:
    def test_series_non_empty(self):
        proc = _make_processor()
        entity = _make_entity()
        cell = DataCell('col1', proc, entity, [], column_index=0, row_index=0)
        series = pd.Series([10, 20, 30], index=pd.date_range('2021-01-01', periods=3))
        result = ProcessorResult(True, series)

        with patch('gs_quant.analytics.datagrid.data_cell.get_utc_now', return_value='2021-01-01T00:00:00.000Z'):
            cell.update(result)

        assert cell.value.success is True
        assert cell.value.data == 30
        assert cell.updated_time == '2021-01-01T00:00:00.000Z'

    def test_series_empty(self):
        proc = _make_processor()
        entity = _make_entity()
        cell = DataCell('col1', proc, entity, [], column_index=0, row_index=0)
        result = ProcessorResult(True, pd.Series(dtype=float))

        with patch('gs_quant.analytics.datagrid.data_cell.get_utc_now', return_value='2021-01-01T00:00:00.000Z'):
            cell.update(result)

        assert cell.value.success is False
        assert 'Empty series' in cell.value.data

    def test_non_series_data(self):
        proc = _make_processor()
        entity = _make_entity()
        cell = DataCell('col1', proc, entity, [], column_index=0, row_index=0)
        result = ProcessorResult(True, 42.5)

        with patch('gs_quant.analytics.datagrid.data_cell.get_utc_now', return_value='2021-01-01T00:00:00.000Z'):
            cell.update(result)

        assert cell.value.success is True
        assert cell.value.data == 42.5
