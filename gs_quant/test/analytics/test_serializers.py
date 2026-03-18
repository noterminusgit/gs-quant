"""
Tests for gs_quant.analytics.datagrid.serializers
"""

from unittest.mock import MagicMock

import pytest

from gs_quant.analytics.datagrid.data_row import ROW_SEPARATOR
from gs_quant.analytics.datagrid.serializers import row_from_dict


class TestRowFromDict:
    def test_row_separator_type(self):
        row_dict = {'type': ROW_SEPARATOR, 'name': 'Section A'}
        result = row_from_dict(row_dict, reference_list=[])
        from gs_quant.analytics.datagrid.data_row import RowSeparator
        assert isinstance(result, RowSeparator)
        assert result.name == 'Section A'

    def test_non_separator_type(self):
        row_dict = {
            'type': 'dataRow',
            'entityId': 'eid1',
            'entityType': 'Asset',
        }
        ref_list = []
        result = row_from_dict(row_dict, ref_list)
        from gs_quant.analytics.datagrid.data_row import DataRow
        assert isinstance(result, DataRow)
        assert len(ref_list) == 1

    def test_missing_type_defaults_to_data_row(self):
        row_dict = {'entityId': 'eid1', 'entityType': 'Asset'}
        ref_list = []
        result = row_from_dict(row_dict, ref_list)
        from gs_quant.analytics.datagrid.data_row import DataRow
        assert isinstance(result, DataRow)
