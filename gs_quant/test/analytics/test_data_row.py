"""
Tests for gs_quant.analytics.datagrid.data_row
"""

from unittest.mock import MagicMock, patch

import pytest

from gs_quant.analytics.datagrid.data_row import (
    DATA_ROW,
    DIMENSIONS_OVERRIDE,
    PROCESSOR_OVERRIDE,
    ROW_SEPARATOR,
    VALUE_OVERRIDE,
    DataRow,
    DimensionsOverride,
    Override,
    ProcessorOverride,
    RowSeparator,
    ValueOverride,
)
from gs_quant.data import DataCoordinate, DataFrequency
from gs_quant.data.fields import DataDimension


# ---------------------------------------------------------------------------
# Override base
# ---------------------------------------------------------------------------

class TestOverride:
    def test_as_dict(self):
        # Override is ABC but as_dict is concrete
        ov = ValueOverride(column_names=['col1'], value=0)
        base = Override.as_dict(ov)
        assert base == {'columnNames': ['col1']}


# ---------------------------------------------------------------------------
# ValueOverride
# ---------------------------------------------------------------------------

class TestValueOverride:
    def test_as_dict(self):
        vo = ValueOverride(['col1', 'col2'], 42)
        d = vo.as_dict()
        assert d['type'] == VALUE_OVERRIDE
        assert d['value'] == 42
        assert d['columnNames'] == ['col1', 'col2']

    def test_from_dict(self):
        obj = {'columnNames': ['a'], 'value': 'hello'}
        vo = ValueOverride.from_dict(obj, ref=None)
        assert vo.value == 'hello'
        assert vo.column_names == ['a']


# ---------------------------------------------------------------------------
# DimensionsOverride
# ---------------------------------------------------------------------------

class TestDimensionsOverride:
    def _make_coordinate(self):
        return DataCoordinate(measure='price', dataset_id='DS1',
                              dimensions={'assetId': 'abc'}, frequency=DataFrequency.DAILY)

    def test_as_dict_without_coordinate_id(self):
        coord = self._make_coordinate()
        do = DimensionsOverride(['col1'], {'strike': 100}, coord)
        d = do.as_dict()
        assert d['type'] == DIMENSIONS_OVERRIDE
        assert d['dimensions'] == {'strike': 100}
        assert 'coordinateId' not in d

    def test_as_dict_with_coordinate_id(self):
        coord = self._make_coordinate()
        do = DimensionsOverride(['col1'], {'strike': 100}, coord, coordinate_id='cid1')
        d = do.as_dict()
        assert d['coordinateId'] == 'cid1'

    def test_enum_keys_converted(self):
        coord = self._make_coordinate()
        do = DimensionsOverride(['col1'], {DataDimension.ASSET_ID: 'xyz'}, coord)
        assert 'assetId' in do.dimensions

    def test_from_dict_with_data_dimension_keys(self):
        coord_dict = {'measure': 'price', 'frequency': 'daily',
                      'datasetId': 'DS1', 'dimensions': {'assetId': 'abc'}}
        obj = {
            'columnNames': ['col1'],
            'dimensions': {'assetId': 'xyz', 'customDim': 'val'},
            'coordinate': coord_dict,
            'coordinateId': 'cid1',
        }
        do = DimensionsOverride.from_dict(obj, reference_list=[])
        # __init__ converts DataDimension enum keys back to string values
        assert 'assetId' in do.dimensions
        assert do.dimensions['assetId'] == 'xyz'
        assert 'customDim' in do.dimensions
        assert do.coordinate_id == 'cid1'


# ---------------------------------------------------------------------------
# ProcessorOverride
# ---------------------------------------------------------------------------

class TestProcessorOverride:
    def test_as_dict_with_processor(self):
        proc = MagicMock()
        proc.__class__.__name__ = 'TestProc'
        proc.as_dict.return_value = {'type': 'processor', 'parameters': {}}
        po = ProcessorOverride(['col1'], proc)
        d = po.as_dict()
        assert d['type'] == PROCESSOR_OVERRIDE
        assert d['processor']['processorName'] == 'TestProc'

    def test_as_dict_with_none_processor(self):
        po = ProcessorOverride(['col1'], None)
        d = po.as_dict()
        assert d['type'] == PROCESSOR_OVERRIDE
        assert d['processor'] is None

    def test_from_dict(self):
        obj = {'columnNames': ['col1'], 'processor': {}}
        with patch(
            'gs_quant.analytics.datagrid.data_row.BaseProcessor.from_dict',
            return_value=None,
        ):
            po = ProcessorOverride.from_dict(obj, reference_list=[])
        assert po.column_names == ['col1']
        assert po.processor is None


# ---------------------------------------------------------------------------
# RowSeparator
# ---------------------------------------------------------------------------

class TestRowSeparator:
    def test_as_dict(self):
        rs = RowSeparator('section1')
        d = rs.as_dict()
        assert d == {'type': ROW_SEPARATOR, 'name': 'section1'}

    def test_from_dict(self):
        rs = RowSeparator.from_dict({'name': 'section2'})
        assert rs.name == 'section2'


# ---------------------------------------------------------------------------
# DataRow
# ---------------------------------------------------------------------------

class TestDataRow:
    def test_as_dict_with_entity(self):
        entity = MagicMock()
        entity.get_marquee_id.return_value = 'eid1'
        entity.entity_type.return_value = MagicMock(value='Asset')
        # Make isinstance(entity, Entity) return True
        from gs_quant.entities.entity import Entity
        entity.__class__ = Entity

        row = DataRow(entity)
        d = row.as_dict()
        assert d['type'] == DATA_ROW
        assert d['entityId'] == 'eid1'
        assert d['entityType'] == 'Asset'
        assert 'overrides' not in d

    def test_as_dict_with_string_entity(self):
        row = DataRow('some_id')
        d = row.as_dict()
        assert d['entityId'] == 'some_id'
        assert d['entityType'] == ''

    def test_as_dict_with_overrides(self):
        entity = MagicMock()
        entity.get_marquee_id.return_value = 'eid1'
        entity.entity_type.return_value = MagicMock(value='Asset')
        from gs_quant.entities.entity import Entity
        entity.__class__ = Entity

        vo = ValueOverride(['col1'], 99)
        row = DataRow(entity, overrides=[vo])
        d = row.as_dict()
        assert len(d['overrides']) == 1
        assert d['overrides'][0]['type'] == VALUE_OVERRIDE

    def test_from_dict_with_all_override_types(self):
        obj = {
            'entityId': 'eid1',
            'entityType': 'Asset',
            'overrides': [
                {'type': PROCESSOR_OVERRIDE, 'columnNames': ['c1'], 'processor': {}},
                {'type': DIMENSIONS_OVERRIDE, 'columnNames': ['c2'],
                 'dimensions': {'assetId': 'abc'},
                 'coordinate': {'measure': 'price', 'frequency': 'daily',
                                'datasetId': 'DS1', 'dimensions': {}}},
                {'type': VALUE_OVERRIDE, 'columnNames': ['c3'], 'value': 10},
            ],
        }
        ref_list = []
        with patch(
            'gs_quant.analytics.datagrid.data_row.BaseProcessor.from_dict',
            return_value=None,
        ):
            row = DataRow.from_dict(obj, ref_list)

        assert row.entity is None  # resolved later
        assert len(row.overrides) == 3
        assert isinstance(row.overrides[0], ProcessorOverride)
        assert isinstance(row.overrides[1], DimensionsOverride)
        assert isinstance(row.overrides[2], ValueOverride)
        # Reference list should have one entry for the data row
        assert len(ref_list) == 1
        assert ref_list[0]['entityId'] == 'eid1'
