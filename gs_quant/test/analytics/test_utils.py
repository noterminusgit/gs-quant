"""
Tests for gs_quant.analytics.datagrid.utils
"""

import re

import pytest

from gs_quant.analytics.datagrid.utils import (
    DataGridFilter,
    DataGridSort,
    FilterCondition,
    FilterOperation,
    SortOrder,
    SortType,
    get_utc_now,
)


# ---------------------------------------------------------------------------
# get_utc_now
# ---------------------------------------------------------------------------

class TestGetUtcNow:
    def test_returns_string_matching_pattern(self):
        result = get_utc_now()
        # Expected format: 2021-01-01T00:00:00.000Z
        assert re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$', result)


# ---------------------------------------------------------------------------
# DataGridSort
# ---------------------------------------------------------------------------

class TestDataGridSort:
    def test_construction(self):
        s = DataGridSort(columnName='price', sortType=SortType.VALUE, order=SortOrder.DESCENDING)
        assert s.columnName == 'price'
        assert s.sortType == SortType.VALUE
        assert s.order == SortOrder.DESCENDING

    def test_post_init_coercion_from_string(self):
        s = DataGridSort(columnName='price', sortType='absoluteValue', order='descending')
        assert s.sortType == SortType.ABSOLUTE_VALUE
        assert s.order == SortOrder.DESCENDING

    def test_from_dict_ignores_extra_keys(self):
        d = {'columnName': 'volume', 'sortType': 'value', 'order': 'ascending', 'extra': True}
        s = DataGridSort.from_dict(d)
        assert s.columnName == 'volume'
        assert s.sortType == SortType.VALUE
        assert not hasattr(s, 'extra')


# ---------------------------------------------------------------------------
# DataGridFilter
# ---------------------------------------------------------------------------

class TestDataGridFilter:
    def test_construction(self):
        f = DataGridFilter(columnName='price', operation=FilterOperation.TOP, value=10)
        assert f.columnName == 'price'
        assert f.operation == FilterOperation.TOP
        assert f.value == 10
        assert f.condition == FilterCondition.AND

    def test_post_init_coercion(self):
        f = DataGridFilter(columnName='price', operation='bottom', value=5, condition='or')
        assert f.operation == FilterOperation.BOTTOM
        assert f.condition == FilterCondition.OR

    def test_from_dict(self):
        d = {'columnName': 'v', 'operation': 'equals', 'value': [1.0, 2.0],
             'condition': 'and', 'extra': 'ignored'}
        f = DataGridFilter.from_dict(d)
        assert f.operation == FilterOperation.EQUALS
        assert f.value == [1.0, 2.0]
        assert f.condition == FilterCondition.AND
