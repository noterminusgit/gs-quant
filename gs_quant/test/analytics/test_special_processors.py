"""
Tests for gs_quant.analytics.processors.special_processors
"""
import pytest
from enum import Enum
from unittest.mock import MagicMock, PropertyMock

from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.analytics.processors.special_processors import (
    EntityProcessor,
    CoordinateProcessor,
    MeasureProcessor,
)
from gs_quant.data import DataDimension


# ---------------------------------------------------------------------------
# EntityProcessor
# ---------------------------------------------------------------------------
class TestEntityProcessor:
    def test_entity_is_string(self):
        proc = EntityProcessor(field='name')
        result = proc.process('SOME_ENTITY_ID')
        assert result.success is False
        assert 'Unable to resolve Entity' in result.data

    def test_field_found_on_entity(self):
        entity = MagicMock()
        entity.get_entity.return_value = {'name': 'TestAsset', 'identifiers': []}
        proc = EntityProcessor(field='name')
        result = proc.process(entity)
        assert result.success is True
        assert result.data == 'TestAsset'

    def test_field_found_in_identifiers(self):
        entity = MagicMock()
        entity.get_entity.return_value = {
            'identifiers': [{'type': 'bbid', 'value': 'AAPL UW'}]
        }
        proc = EntityProcessor(field='bbid')
        result = proc.process(entity)
        assert result.success is True
        assert result.data == 'AAPL UW'

    def test_field_not_found(self):
        entity = MagicMock()
        entity.get_entity.return_value = {
            'name': 'TestAsset',
            'identifiers': [{'type': 'bbid', 'value': 'AAPL'}]
        }
        entity.get_marquee_id.return_value = 'MQID123'
        proc = EntityProcessor(field='nonexistent')
        result = proc.process(entity)
        assert result.success is False
        assert 'Unable to find' in result.data
        assert 'nonexistent' in result.data

    def test_value_error(self):
        entity = MagicMock()
        entity.get_entity.side_effect = ValueError('bad entity')
        proc = EntityProcessor(field='name')
        result = proc.process(entity)
        assert result.success is False
        assert 'Could not get field' in result.data

    def test_nested_field(self):
        entity = MagicMock()
        entity.get_entity.return_value = {
            'xref': {'bbid': 'SPX'},
            'identifiers': []
        }
        proc = EntityProcessor(field='xref.bbid')
        result = proc.process(entity)
        assert result.success is True
        assert result.data == 'SPX'

    def test_update_is_noop(self):
        proc = EntityProcessor(field='name')
        # Should not raise
        proc.update('a', ProcessorResult(True, 'data'))

    def test_get_plot_expression(self):
        proc = EntityProcessor(field='name')
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# CoordinateProcessor
# ---------------------------------------------------------------------------
class TestCoordinateProcessor:
    def test_dimension_as_enum(self):
        coord = MagicMock()
        coord.dimensions = {'strikeReference': 'delta'}

        class FakeDim(Enum):
            STRIKE_REF = 'strikeReference'

        proc = CoordinateProcessor(a=coord, dimension=FakeDim.STRIKE_REF)
        result = proc.process()
        assert result.success is True
        assert result.data == 'delta'

    def test_dimension_as_str(self):
        coord = MagicMock()
        coord.dimensions = {'tenor': '3m'}
        proc = CoordinateProcessor(a=coord, dimension='tenor')
        result = proc.process()
        assert result.success is True
        assert result.data == '3m'

    def test_dimension_not_found(self):
        coord = MagicMock()
        coord.dimensions = {'tenor': '3m'}
        proc = CoordinateProcessor(a=coord, dimension='strike')
        result = proc.process()
        assert result.success is False
        assert 'strike' in result.data

    def test_coordinate_is_none(self):
        proc = CoordinateProcessor(a=MagicMock(), dimension='tenor')
        proc.children['a'] = None
        result = proc.process()
        assert result.success is False

    def test_update_is_noop(self):
        coord = MagicMock()
        coord.dimensions = {}
        proc = CoordinateProcessor(a=coord, dimension='tenor')
        proc.update('a', ProcessorResult(True, 'data'))

    def test_get_plot_expression(self):
        coord = MagicMock()
        coord.dimensions = {}
        proc = CoordinateProcessor(a=coord, dimension='tenor')
        assert proc.get_plot_expression() is None


# ---------------------------------------------------------------------------
# MeasureProcessor
# ---------------------------------------------------------------------------
class TestMeasureProcessor:
    def test_construction(self):
        proc = MeasureProcessor()
        assert proc.measure_processor is True

    def test_process_returns_none(self):
        proc = MeasureProcessor()
        result = proc.process()
        assert result is None

    def test_process_with_args(self):
        proc = MeasureProcessor()
        result = proc.process('some_entity')
        assert result is None

    def test_get_plot_expression(self):
        proc = MeasureProcessor()
        assert proc.get_plot_expression() is None
