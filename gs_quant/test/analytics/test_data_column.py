"""
Tests for gs_quant.analytics.datagrid.data_column
"""

from unittest.mock import MagicMock, patch

import pytest

from gs_quant.analytics.datagrid.data_column import (
    ColumnFormat,
    DataColumn,
    DEFAULT_WIDTH,
    HeatMapColorRange,
    MultiColumnGroup,
    RenderType,
)


# ---------------------------------------------------------------------------
# RenderType
# ---------------------------------------------------------------------------

class TestRenderType:
    def test_constants(self):
        assert RenderType.DEFAULT == 'default'
        assert RenderType.HEATMAP == 'heatmap'
        assert RenderType.BOXPLOT == 'boxplot'
        assert RenderType.SCALE == 'scale'
        assert RenderType.DATE_MMM_YY == 'dateMmmYy'
        assert RenderType.TIME_HH_MM == 'timeHhMm'


# ---------------------------------------------------------------------------
# HeatMapColorRange
# ---------------------------------------------------------------------------

class TestHeatMapColorRange:
    def test_from_dict(self):
        d = {'low': '#ff0000', 'mid': '#00ff00', 'high': '#0000ff', 'extra': 'ignored'}
        hm = HeatMapColorRange.from_dict(d)
        assert hm.low == '#ff0000'
        assert hm.mid == '#00ff00'
        assert hm.high == '#0000ff'


# ---------------------------------------------------------------------------
# MultiColumnGroup
# ---------------------------------------------------------------------------

class TestMultiColumnGroup:
    def test_asdict_minimal(self):
        g = MultiColumnGroup(id=1, columnIndices=[0, 1])
        d = g.asdict()
        assert d == {'id': 1, 'columnIndices': [0, 1]}
        assert 'groupAll' not in d
        assert 'heatMapColorRange' not in d

    def test_asdict_with_group_all_and_heatmap(self):
        hm = HeatMapColorRange('#ff0000', '#00ff00', '#0000ff')
        g = MultiColumnGroup(id=2, columnIndices=[0], groupAll=True, heatMapColorRange=hm)
        d = g.asdict()
        assert d['groupAll'] is True
        assert d['heatMapColorRange'] == {'low': '#ff0000', 'mid': '#00ff00', 'high': '#0000ff'}

    def test_from_dict(self):
        d = {'id': 3, 'columnIndices': [1, 2], 'groupAll': True,
             'heatMapColorRange': {'low': '#a', 'mid': '#b', 'high': '#c'}}
        g = MultiColumnGroup.from_dict(d)
        assert g.id == 3
        assert g.groupAll is True
        assert g.heatMapColorRange.low == '#a'

    def test_from_dict_no_heatmap(self):
        d = {'id': 0, 'columnIndices': [0]}
        g = MultiColumnGroup.from_dict(d)
        assert g.heatMapColorRange is None


# ---------------------------------------------------------------------------
# ColumnFormat
# ---------------------------------------------------------------------------

class TestColumnFormat:
    def test_as_dict_default(self):
        cf = ColumnFormat()
        d = cf.as_dict()
        assert d == {'renderType': 'default', 'precision': 2, 'humanReadable': True}
        assert 'tooltip' not in d
        assert 'displayValues' not in d  # only added for non-DEFAULT renderType

    def test_as_dict_with_tooltip(self):
        cf = ColumnFormat(tooltip='help text')
        d = cf.as_dict()
        assert d['tooltip'] == 'help text'

    def test_as_dict_non_default_render_type(self):
        cf = ColumnFormat(renderType=RenderType.HEATMAP, displayValues=False)
        d = cf.as_dict()
        assert d['displayValues'] is False

    def test_from_dict(self):
        d = {'renderType': 'heatmap', 'precision': 4, 'humanReadable': False,
             'tooltip': 'tip', 'displayValues': True}
        cf = ColumnFormat.from_dict(d)
        assert cf.renderType == 'heatmap'
        assert cf.precision == 4
        assert cf.humanReadable is False
        assert cf.tooltip == 'tip'
        assert cf.displayValues is True


# ---------------------------------------------------------------------------
# DataColumn
# ---------------------------------------------------------------------------

class TestDataColumn:
    def test_as_dict_without_processor(self):
        col = DataColumn('Price')
        d = col.as_dict()
        assert d['name'] == 'Price'
        assert d['width'] == DEFAULT_WIDTH
        assert 'format' in d
        assert 'processorName' not in d

    def test_as_dict_with_processor(self):
        proc = MagicMock()
        proc.__class__.__name__ = 'LastProcessor'
        proc.as_dict.return_value = {'parameters': {}}
        col = DataColumn('Price', processor=proc)
        d = col.as_dict()
        assert d['processorName'] == 'LastProcessor'

    def test_from_dict(self):
        obj = {'name': 'Volume', 'format': {'renderType': 'default', 'precision': 2, 'humanReadable': True},
               'width': 150}
        with patch.object(
            __import__('gs_quant.analytics.core.processor', fromlist=['BaseProcessor']).BaseProcessor,
            'from_dict',
            return_value=None,
        ):
            col = DataColumn.from_dict(obj, reference_list=[])
        assert col.name == 'Volume'
        assert col.width == 150
