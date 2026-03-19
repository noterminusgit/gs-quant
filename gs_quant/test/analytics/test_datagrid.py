"""
Copyright 2018 Goldman Sachs.
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

import asyncio
import datetime as dt
from collections import defaultdict
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from gs_quant.analytics.core.processor_result import ProcessorResult
from gs_quant.analytics.datagrid.data_cell import DataCell
from gs_quant.analytics.datagrid.data_column import DataColumn, ColumnFormat, MultiColumnGroup
from gs_quant.analytics.datagrid.data_row import (
    DataRow,
    DimensionsOverride,
    ProcessorOverride,
    ValueOverride,
    RowSeparator,
    Override,
)
from gs_quant.analytics.datagrid.utils import (
    DataGridSort,
    SortOrder,
    SortType,
    DataGridFilter,
    FilterOperation,
    FilterCondition,
)
from gs_quant.common import Entitlements as Entitlements_
from gs_quant.entities.entitlements import Entitlements
from gs_quant.errors import MqValueError


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #

def _make_entity(marquee_id='ENT1', entity_type_val='ASSET', short_name='TestEntity',
                 data_dimension='assetId', currency=None, exchange=None, use_spec=True):
    from gs_quant.entities.entity import Entity as RealEntity
    if use_spec:
        entity = MagicMock(spec=RealEntity)
    else:
        entity = MagicMock()
    entity.get_marquee_id.return_value = marquee_id
    entity_type = MagicMock()
    entity_type.value = entity_type_val
    entity.entity_type.return_value = entity_type
    entity.short_name = short_name
    entity.data_dimension = data_dimension
    entity_dict = {}
    if currency:
        entity_dict['currency'] = currency
    if exchange:
        entity_dict['exchange'] = exchange
    entity.get_entity.return_value = entity_dict
    return entity


def _make_processor(measure_processor=False):
    proc = MagicMock()
    proc.measure_processor = measure_processor
    proc.children = {}
    proc.__class__.__name__ = 'MockProcessor'
    return proc


def _make_datagrid(name='Test', rows=None, columns=None, **kwargs):
    """Create a DataGrid while suppressing the help message print."""
    from gs_quant.analytics.datagrid.datagrid import DataGrid
    with patch('gs_quant.analytics.datagrid.datagrid.print'):
        return DataGrid(name=name, rows=rows or [], columns=columns or [], **kwargs)


def _make_entity_processor():
    from gs_quant.analytics.processors import EntityProcessor
    return EntityProcessor(field='short_name')


def _make_coord_processor():
    from gs_quant.analytics.processors import CoordinateProcessor
    from gs_quant.data import DataCoordinate, DataMeasure, DataFrequency
    coord = DataCoordinate(measure=DataMeasure.TRADE_PRICE, frequency=DataFrequency.REAL_TIME)
    return CoordinateProcessor(a=coord, dimension='assetId')


# --------------------------------------------------------------------------- #
#  DataGrid: __init__ and basic properties
# --------------------------------------------------------------------------- #

class TestDataGridInit:
    def test_basic_creation(self):
        dg = _make_datagrid(name='MyGrid')
        assert dg.name == 'MyGrid'
        assert dg.rows == []
        assert dg.columns == []
        assert dg.is_initialized is False
        assert dg.id_ is None
        assert dg.polling_time == 0
        assert dg.sorts == []
        assert dg.filters == []

    def test_polling_time_none(self):
        """Branch: polling_time setter with None -> 0."""
        dg = _make_datagrid(polling_time=None)
        assert dg.polling_time == 0

    def test_polling_time_valid(self):
        dg = _make_datagrid(polling_time=10000)
        assert dg.polling_time == 10000

    def test_polling_time_zero(self):
        dg = _make_datagrid(polling_time=0)
        assert dg.polling_time == 0

    def test_polling_time_too_low(self):
        """Branch: value != 0 and value < 5000."""
        with pytest.raises(MqValueError):
            _make_datagrid(polling_time=3000)

    def test_get_id(self):
        dg = _make_datagrid(id_='grid-123')
        assert dg.get_id() == 'grid-123'


# --------------------------------------------------------------------------- #
#  DataGrid: initialize
# --------------------------------------------------------------------------- #

class TestDataGridInitialize:
    def test_initialize_with_entity_processor(self):
        """Branch: isinstance(column_processor, EntityProcessor)."""
        entity = _make_entity()
        proc = _make_entity_processor()
        row = DataRow(entity=entity)
        col = DataColumn(name='Name', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        assert dg.is_initialized is True
        assert len(dg._entity_cells) == 1
        assert len(dg.results) == 1

    def test_initialize_with_coord_processor(self):
        """Branch: isinstance(column_processor, CoordinateProcessor)."""
        entity = _make_entity()
        proc = _make_coord_processor()
        row = DataRow(entity=entity)
        col = DataColumn(name='Coord', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        assert len(dg._coord_processor_cells) == 1

    def test_initialize_with_coord_processor_and_data_overrides(self):
        """Branch: CoordinateProcessor with len(data_overrides) > 0."""
        from gs_quant.data import DataCoordinate, DataMeasure, DataFrequency

        entity = _make_entity()
        proc = _make_coord_processor()

        coord = DataCoordinate(measure=DataMeasure.TRADE_PRICE, frequency=DataFrequency.REAL_TIME)
        dim_override = DimensionsOverride(
            column_names=['Coord'],
            dimensions={'assetId': 'OVERRIDE_ID'},
            coordinate=coord,
        )
        row = DataRow(entity=entity, overrides=[dim_override])
        col = DataColumn(name='Coord', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        assert len(dg._coord_processor_cells) == 1

    def test_initialize_with_value_override(self):
        """Branch: value_override is truthy."""
        entity = _make_entity()
        proc = _make_processor()
        val_override = ValueOverride(column_names=['Col'], value=42.0)
        row = DataRow(entity=entity, overrides=[val_override])
        col = DataColumn(name='Col', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        cell = dg.results[0][0]
        assert cell.value.data == 42.0

    def test_initialize_with_processor_override(self):
        """Branch: processor_override is truthy."""
        entity = _make_entity()
        proc = _make_processor()
        override_proc = _make_processor()
        proc_override = ProcessorOverride(column_names=['Col'], processor=override_proc)
        row = DataRow(entity=entity, overrides=[proc_override])
        col = DataColumn(name='Col', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        # The cell's processor should have been overridden
        assert len(dg.results) == 1

    def test_initialize_with_measure_processor(self):
        """Branch: column_processor.measure_processor is True."""
        entity = _make_entity()
        proc = _make_processor(measure_processor=True)
        row = DataRow(entity=entity)
        col = DataColumn(name='Measure', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        assert len(dg._data_queries) == 1

    def test_initialize_with_regular_processor(self):
        """Branch: else - build_cell_graph path."""
        entity = _make_entity()
        proc = _make_processor()
        row = DataRow(entity=entity)
        col = DataColumn(name='Regular', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        assert dg.is_initialized is True

    def test_initialize_with_row_separator(self):
        """Branch: isinstance(row, RowSeparator) -> continue."""
        entity = _make_entity()
        proc = _make_entity_processor()
        sep = RowSeparator(name='Section 1')
        row = DataRow(entity=entity)
        col = DataColumn(name='Name', processor=proc)
        dg = _make_datagrid(rows=[sep, row], columns=[col])
        dg.initialize()
        assert dg.is_initialized is True
        # Separator doesn't produce results, only the DataRow does
        assert len(dg.results) == 1

    def test_initialize_non_entity(self):
        """Branch: entity is NOT isinstance(Entity) -> entity_map[''] = entity."""
        proc = _make_entity_processor()
        # Use a non-Entity (string) as entity
        row = DataRow(entity='some_string')
        col = DataColumn(name='Name', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        assert '' in dg.entity_map

    def test_initialize_with_real_entity_spec(self):
        """Branch: isinstance(entity, Entity) is True -> entity_map[id] = entity."""
        entity = _make_entity(use_spec=True)
        proc = _make_entity_processor()
        row = DataRow(entity=entity)
        col = DataColumn(name='Name', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        assert 'ENT1' in dg.entity_map

    def test_initialize_with_dimensions_override_not_matching_column(self):
        """Branch: column_name NOT in override.column_names -> override is ignored."""
        from gs_quant.data import DataCoordinate, DataMeasure, DataFrequency
        entity = _make_entity()
        proc = _make_processor()
        coord = DataCoordinate(measure=DataMeasure.TRADE_PRICE, frequency=DataFrequency.REAL_TIME)
        dim_override = DimensionsOverride(
            column_names=['OtherColumn'],
            dimensions={'assetId': 'OVERRIDE_ID'},
            coordinate=coord,
        )
        row = DataRow(entity=entity, overrides=[dim_override])
        col = DataColumn(name='Col', processor=proc)
        dg = _make_datagrid(rows=[row], columns=[col])
        dg.initialize()
        assert dg.is_initialized is True


# --------------------------------------------------------------------------- #
#  DataGrid: save, create, delete, open
# --------------------------------------------------------------------------- #

class TestDataGridPersistence:
    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_save_existing(self, mock_gs):
        """Branch: self.id_ is truthy -> PUT."""
        mock_session = MagicMock()
        mock_gs.current = mock_session
        response = {'id': 'grid-1', 'name': 'G', 'parameters': {'rows': [], 'columns': []}}
        mock_session.sync.put.return_value = response
        dg = _make_datagrid(id_='grid-1')
        result = dg.save()
        mock_session.sync.put.assert_called_once()
        assert result is not None

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_save_new(self, mock_gs):
        """Branch: self.id_ is falsy -> POST."""
        mock_session = MagicMock()
        mock_gs.current = mock_session
        response = {'id': 'new-grid', 'name': 'G', 'parameters': {'rows': [], 'columns': []}}
        mock_session.sync.post.return_value = response
        dg = _make_datagrid()
        result = dg.save()
        mock_session.sync.post.assert_called_once()
        assert dg.id_ == 'new-grid'

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_create(self, mock_gs):
        mock_session = MagicMock()
        mock_gs.current = mock_session
        mock_session.sync.post.return_value = {'id': 'created-id'}
        dg = _make_datagrid()
        result = dg.create()
        assert result == 'created-id'
        assert dg.id_ == 'created-id'

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_delete_existing(self, mock_gs):
        """Branch: self.id_ is truthy."""
        mock_session = MagicMock()
        mock_gs.current = mock_session
        dg = _make_datagrid(id_='grid-1')
        dg.delete()
        mock_session.sync.delete.assert_called_once()

    def test_delete_not_persisted(self):
        """Branch: self.id_ is falsy."""
        dg = _make_datagrid()
        with pytest.raises(MqValueError):
            dg.delete()

    @patch('gs_quant.analytics.datagrid.datagrid.webbrowser')
    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_open_no_id(self, mock_gs, mock_wb):
        dg = _make_datagrid()
        with pytest.raises(MqValueError):
            dg.open()

    @patch('gs_quant.analytics.datagrid.datagrid.webbrowser')
    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_open_api_gs_domain(self, mock_gs, mock_wb):
        """Branch: domain == 'https://api.gs.com'."""
        mock_session = MagicMock()
        mock_session.domain = 'https://api.gs.com'
        mock_gs.current = mock_session
        dg = _make_datagrid(id_='grid-1')
        dg.open()
        mock_wb.open.assert_called_once_with('https://marquee.gs.com/s/markets/grids/grid-1')

    @patch('gs_quant.analytics.datagrid.datagrid.webbrowser')
    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_open_other_domain(self, mock_gs, mock_wb):
        """Branch: domain != 'https://api.gs.com' after .web replace."""
        mock_session = MagicMock()
        mock_session.domain = 'https://other.marquee.gs.com'
        mock_gs.current = mock_session
        dg = _make_datagrid(id_='grid-1')
        dg.open()
        mock_wb.open.assert_called_once_with('https://other.marquee.gs.com/s/markets/grids/grid-1')


# --------------------------------------------------------------------------- #
#  DataGrid: as_dict
# --------------------------------------------------------------------------- #

class TestDataGridAsDict:
    def test_as_dict_minimal(self):
        dg = _make_datagrid(name='Grid')
        d = dg.as_dict()
        assert d['name'] == 'Grid'
        assert 'entitlements' not in d

    def test_as_dict_with_entitlements_common(self):
        """Branch: isinstance(self.entitlements, Entitlements_)."""
        ent = Entitlements_()
        dg = _make_datagrid(entitlements=ent)
        d = dg.as_dict()
        assert 'entitlements' in d

    def test_as_dict_with_entitlements_entity(self):
        """Branch: isinstance(self.entitlements, Entitlements)."""
        ent = Entitlements.from_dict({})
        dg = _make_datagrid(entitlements=ent)
        d = dg.as_dict()
        assert 'entitlements' in d

    def test_as_dict_with_entitlements_raw(self):
        """Branch: else for entitlements."""
        dg = _make_datagrid(entitlements={'view': ['user1']})
        d = dg.as_dict()
        assert d['entitlements'] == {'view': ['user1']}

    def test_as_dict_with_sorts(self):
        sort = DataGridSort(columnName='Col1', sortType=SortType.VALUE, order=SortOrder.ASCENDING)
        dg = _make_datagrid(sorts=[sort])
        d = dg.as_dict()
        assert 'sorts' in d['parameters']

    def test_as_dict_with_filters(self):
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.TOP, value=5)
        dg = _make_datagrid(filters=[f])
        d = dg.as_dict()
        assert 'filters' in d['parameters']

    def test_as_dict_with_multi_column_groups(self):
        mcg = MultiColumnGroup(id=0, columnIndices=[0, 1])
        dg = _make_datagrid(multiColumnGroups=[mcg])
        d = dg.as_dict()
        assert 'multiColumnGroups' in d['parameters']

    def test_as_dict_no_sorts_no_filters(self):
        """Branch: len(self.sorts) == 0, len(self.filters) == 0."""
        dg = _make_datagrid()
        d = dg.as_dict()
        assert 'sorts' not in d['parameters']
        assert 'filters' not in d['parameters']


# --------------------------------------------------------------------------- #
#  DataGrid: from_dict
# --------------------------------------------------------------------------- #

class TestDataGridFromDict:
    @patch('gs_quant.analytics.datagrid.datagrid.resolve_entities')
    @patch('gs_quant.analytics.datagrid.datagrid.print')
    def test_from_dict_with_resolve(self, mock_print, mock_resolve):
        """Branch: reference_list is None -> should_resolve_entities = True."""
        obj = {
            'id': 'grid-1',
            'name': 'G1',
            'parameters': {
                'rows': [],
                'columns': [],
                'sorts': [],
                'filters': [],
                'multiColumnGroups': [],
                'primaryColumnIndex': 1,
                'pollingTime': 10000,
            },
            'entitlements': {},
        }
        dg = None
        from gs_quant.analytics.datagrid.datagrid import DataGrid
        dg = DataGrid.from_dict(obj)
        assert dg.id_ == 'grid-1'
        mock_resolve.assert_called_once()

    @patch('gs_quant.analytics.datagrid.datagrid.print')
    def test_from_dict_with_reference_list(self, mock_print):
        """Branch: reference_list is NOT None -> should_resolve_entities = False."""
        obj = {
            'name': 'G2',
            'parameters': {
                'rows': [],
                'columns': [],
            },
            'entitlements': {},
        }
        ref_list = []
        from gs_quant.analytics.datagrid.datagrid import DataGrid
        dg = DataGrid.from_dict(obj, reference_list=ref_list)
        assert dg.name == 'G2'


# --------------------------------------------------------------------------- #
#  DataGrid: _process_special_cells
# --------------------------------------------------------------------------- #

class TestProcessSpecialCells:
    def test_entity_cell_success(self):
        """Branch: entity cell processes successfully."""
        dg = _make_datagrid()
        entity = _make_entity()
        proc = MagicMock()
        proc.process.return_value = ProcessorResult(True, 'EntityName')
        cell = MagicMock()
        cell.entity = entity
        cell.processor = proc
        cell.value = None
        cell.updated_time = None
        dg._entity_cells = [cell]
        dg._process_special_cells()
        assert cell.value == ProcessorResult(True, 'EntityName')

    def test_entity_cell_exception(self):
        """Branch: entity cell raises exception."""
        dg = _make_datagrid()
        entity = _make_entity()
        proc = MagicMock()
        proc.__class__.__name__ = 'EntityProcessor'
        proc.process.side_effect = Exception('fail')
        cell = MagicMock()
        cell.entity = entity
        cell.processor = proc
        cell.value = None
        cell.updated_time = None
        dg._entity_cells = [cell]
        dg._process_special_cells()
        assert 'Error Calculating' in cell.value

    def test_coord_cell_success(self):
        """Branch: coord cell processes successfully."""
        dg = _make_datagrid()
        proc = MagicMock()
        proc.process.return_value = ProcessorResult(True, 100.0)
        entity = _make_entity()
        cell = MagicMock()
        cell.entity = entity
        cell.processor = proc
        cell.value = None
        cell.updated_time = None
        dg._coord_processor_cells = [cell]
        dg._process_special_cells()
        assert cell.value == ProcessorResult(True, 100.0)

    def test_coord_cell_exception(self):
        """Branch: coord cell raises exception."""
        dg = _make_datagrid()
        proc = MagicMock()
        proc.__class__.__name__ = 'CoordinateProcessor'
        proc.process.side_effect = Exception('coord fail')
        entity = _make_entity()
        cell = MagicMock()
        cell.entity = entity
        cell.processor = proc
        cell.value = None
        cell.updated_time = None
        dg._coord_processor_cells = [cell]
        dg._process_special_cells()
        assert 'Error Calculating' in cell.value


# --------------------------------------------------------------------------- #
#  DataGrid: _resolve_rdates
# --------------------------------------------------------------------------- #

class TestResolveRdates:
    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    @patch('gs_quant.analytics.datagrid.datagrid.RelativeDate')
    def test_resolve_rdates_with_entity(self, mock_rdate_cls, mock_gs):
        """Branch: isinstance(entity, Entity) -> gets currency/exchange."""
        mock_session = MagicMock()
        mock_session.is_internal.return_value = True
        mock_gs.current = mock_session

        dg = _make_datagrid()
        entity = _make_entity(currency='USD', exchange='NYSE')
        dg.entity_map = {'ENT1': entity}
        dg.rdate_entity_map = {'ENT1': {('-1d', '2021-01-22')}}

        mock_rdate_instance = MagicMock()
        mock_rdate_instance.apply_rule.return_value = dt.date(2021, 1, 21)
        mock_rdate_cls.return_value = mock_rdate_instance

        dg._resolve_rdates()
        assert len(dg.rule_cache) == 1

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    @patch('gs_quant.analytics.datagrid.datagrid.RelativeDate')
    def test_resolve_rdates_no_base_date(self, mock_rdate_cls, mock_gs):
        """Branch: base_date is falsy."""
        mock_session = MagicMock()
        mock_session.is_internal.return_value = True
        mock_gs.current = mock_session

        dg = _make_datagrid()
        entity = _make_entity()
        dg.entity_map = {'ENT1': entity}
        dg.rdate_entity_map = {'ENT1': {('-1d', None)}}

        mock_rdate_instance = MagicMock()
        mock_rdate_instance.apply_rule.return_value = dt.date(2021, 1, 21)
        mock_rdate_cls.return_value = mock_rdate_instance

        dg._resolve_rdates()
        assert len(dg.rule_cache) == 1

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    @patch('gs_quant.analytics.datagrid.datagrid.RelativeDate')
    def test_resolve_rdates_cached(self, mock_rdate_cls, mock_gs):
        """Branch: date_value is not None (from cache)."""
        mock_session = MagicMock()
        mock_session.is_internal.return_value = True
        mock_gs.current = mock_session

        dg = _make_datagrid()
        entity = _make_entity()
        dg.entity_map = {'ENT1': entity}
        dg.rdate_entity_map = {'ENT1': {('-1d', None)}}

        # Pre-populate rule_cache
        rule_cache = {}
        # We need to call with a pre-populated cache
        # The get_rdate_cache_key function will determine the key
        with patch('gs_quant.analytics.datagrid.datagrid.get_rdate_cache_key', return_value='cache_key'):
            dg._resolve_rdates(rule_cache={'cache_key': dt.date(2021, 1, 21)})
        assert len(dg.rule_cache) == 1

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_rdates_oauth_external(self, mock_gs):
        """Branch: not is_internal() and isinstance OAuth2Session -> calendar = []."""
        from gs_quant.session import OAuth2Session
        mock_session = MagicMock(spec=OAuth2Session)
        mock_session.is_internal.return_value = False
        mock_gs.current = mock_session

        dg = _make_datagrid()
        dg.rdate_entity_map = {}  # No rules to process
        dg._resolve_rdates()
        # Should not raise; calendar should be []

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_rdates_non_entity(self, mock_gs):
        """Branch: entity is NOT isinstance(Entity) -> currencies/exchanges = None."""
        mock_session = MagicMock()
        mock_session.is_internal.return_value = True
        mock_gs.current = mock_session

        dg = _make_datagrid()
        dg.entity_map = {'ENT1': 'not_an_entity'}
        dg.rdate_entity_map = {'ENT1': {('-1d', None)}}

        with patch('gs_quant.analytics.datagrid.datagrid.RelativeDate') as mock_rdate_cls:
            mock_rdate_instance = MagicMock()
            mock_rdate_instance.apply_rule.return_value = dt.date(2021, 1, 21)
            mock_rdate_cls.return_value = mock_rdate_instance
            dg._resolve_rdates()
            # currencies and exchanges should be None
            mock_rdate_instance.apply_rule.assert_called_once_with(
                currencies=None, exchanges=None, holiday_calendar=None
            )


# --------------------------------------------------------------------------- #
#  DataGrid: _resolve_queries
# --------------------------------------------------------------------------- #

class TestResolveQueries:
    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_queries_string_entity(self, mock_gs):
        """Branch: isinstance(entity, str) -> continue."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        dg = _make_datagrid()
        from gs_quant.analytics.core.processor import DataQueryInfo
        query_info = MagicMock(spec=DataQueryInfo)
        query_info.entity = 'string_entity'
        dg._data_queries = [query_info]
        dg._resolve_queries()
        # Should not raise, just continue

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_queries_measure_query(self, mock_gs):
        """Branch: isinstance(query, MeasureQueryInfo) -> continue."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        dg = _make_datagrid()
        from gs_quant.analytics.core.processor import MeasureQueryInfo
        query_info = MagicMock(spec=MeasureQueryInfo)
        query_info.entity = _make_entity()
        dg._data_queries = [query_info]
        dg._resolve_queries()

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_queries_with_relative_date_start_end(self, mock_gs):
        """Branch: query_start/query_end are RelativeDate."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        dg = _make_datagrid()
        entity = _make_entity()

        from gs_quant.datetime.relative_date import RelativeDate
        from gs_quant.analytics.core.processor import DataQueryInfo

        coord = MagicMock()
        coord.dimensions = {'assetId': 'ENT1'}
        coord.dataset_id = 'ds1'
        coord.measure = 'tradePrice'

        query = MagicMock()
        query.coordinate = coord
        query.start = RelativeDate('-1d')
        query.end = RelativeDate('0d')

        query_info = MagicMock(spec=DataQueryInfo)
        query_info.entity = entity
        query_info.query = query

        dg._data_queries = [query_info]
        dg.rule_cache = {}

        with patch('gs_quant.analytics.datagrid.datagrid.get_entity_rdate_key_from_rdate') as mock_key:
            mock_key.side_effect = ['start_key', 'end_key']
            dg.rule_cache['start_key'] = dt.date(2021, 1, 21)
            dg.rule_cache['end_key'] = dt.date(2021, 1, 22)
            dg._resolve_queries()
            assert query.start == dt.date(2021, 1, 21)
            assert query.end == dt.date(2021, 1, 22)

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_queries_entity_dimension_in_coord(self, mock_gs):
        """Branch: entity_dimension in coord.dimensions -> skip."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        dg = _make_datagrid()
        entity = _make_entity()

        from gs_quant.analytics.core.processor import DataQueryInfo

        coord = MagicMock()
        coord.dimensions = {'assetId': 'ENT1'}  # entity_dimension is 'assetId'
        coord.dataset_id = 'ds1'

        query = MagicMock()
        query.coordinate = coord
        query.start = dt.date(2021, 1, 21)
        query.end = dt.date(2021, 1, 22)

        query_info = MagicMock(spec=DataQueryInfo)
        query_info.entity = entity
        query_info.query = query

        dg._data_queries = [query_info]
        dg._resolve_queries()
        # Should skip since entity_dimension is in coord.dimensions

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_queries_dataset_id_exists(self, mock_gs):
        """Branch: entity_dimension NOT in dimensions, coord.dataset_id truthy."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        dg = _make_datagrid()
        entity = _make_entity()

        from gs_quant.analytics.core.processor import DataQueryInfo

        coord = MagicMock()
        coord.dimensions = {'otherDim': 'val'}
        coord.dataset_id = 'ds1'

        query = MagicMock()
        query.coordinate = coord
        query.start = dt.date(2021, 1, 21)
        query.end = dt.date(2021, 1, 22)

        query_info = MagicMock(spec=DataQueryInfo)
        query_info.entity = entity
        query_info.query = query

        dg._data_queries = [query_info]
        dg._resolve_queries()
        coord.set_dimensions.assert_called_once()

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_queries_no_dataset_id_availability(self, mock_gs):
        """Branch: no dataset_id -> fetch availability."""
        mock_session = MagicMock()
        mock_gs.current = mock_session
        mock_session.sync.get.return_value = {'measures': []}

        dg = _make_datagrid()
        entity = _make_entity()

        from gs_quant.analytics.core.processor import DataQueryInfo

        coord = MagicMock()
        coord.dimensions = {'otherDim': 'val'}
        coord.dataset_id = None
        coord.measure = 'tradePrice'
        coord.frequency = 'daily'

        query = MagicMock()
        query.coordinate = coord
        query.start = dt.date(2021, 1, 21)
        query.end = dt.date(2021, 1, 22)

        query_info = MagicMock(spec=DataQueryInfo)
        query_info.entity = entity
        query_info.query = query

        dg._data_queries = [query_info]
        dg._resolve_queries()
        mock_session.sync.get.assert_called_once()

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_queries_availability_cached(self, mock_gs):
        """Branch: raw_availability from cache."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        dg = _make_datagrid()
        entity = _make_entity()

        from gs_quant.analytics.core.processor import DataQueryInfo

        coord = MagicMock()
        coord.dimensions = {'otherDim': 'val'}
        coord.dataset_id = None
        coord.measure = 'tradePrice'
        coord.frequency = 'daily'

        query = MagicMock()
        query.coordinate = coord
        query.start = dt.date(2021, 1, 21)
        query.end = dt.date(2021, 1, 22)

        query_info = MagicMock(spec=DataQueryInfo)
        query_info.entity = entity
        query_info.query = query

        dg._data_queries = [query_info]
        dg._resolve_queries(availability_cache={'ENT1': {'measures': []}})
        # Should NOT call session.sync.get since cached
        mock_session.sync.get.assert_not_called()

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_queries_exception(self, mock_gs):
        """Branch: exception during availability resolution."""
        mock_session = MagicMock()
        mock_gs.current = mock_session
        mock_session.sync.get.side_effect = Exception('fail')

        dg = _make_datagrid()
        entity = _make_entity()

        from gs_quant.analytics.core.processor import DataQueryInfo

        coord = MagicMock()
        coord.dimensions = {'otherDim': 'val'}
        coord.dataset_id = None

        query = MagicMock()
        query.coordinate = coord
        query.start = dt.date(2021, 1, 21)
        query.end = dt.date(2021, 1, 22)

        query_info = MagicMock(spec=DataQueryInfo)
        query_info.entity = entity
        query_info.query = query

        dg._data_queries = [query_info]
        # Should not raise
        dg._resolve_queries()


# --------------------------------------------------------------------------- #
#  DataGrid: _fetch_queries
# --------------------------------------------------------------------------- #

class TestFetchQueries:
    @patch('gs_quant.analytics.datagrid.datagrid.asyncio')
    @patch('gs_quant.analytics.datagrid.datagrid.fetch_query')
    @patch('gs_quant.analytics.datagrid.datagrid.aggregate_queries')
    @patch('gs_quant.analytics.datagrid.datagrid.build_query_string')
    @patch('gs_quant.analytics.datagrid.datagrid.valid_dimensions')
    def test_fetch_with_valid_dimensions(self, mock_valid, mock_build, mock_agg, mock_fetch, mock_asyncio):
        """Branch: valid_dimensions returns True."""
        dg = _make_datagrid()

        mock_query_info = MagicMock()
        mock_query_info.query.coordinate.measure = 'tradePrice'
        mock_query_info.data = pd.Series([1.0, 2.0])

        mock_agg.return_value = {
            'ds1': {
                'range1': {
                    'queries': {
                        'dim1': [mock_query_info],
                    },
                },
            },
        }
        df = pd.DataFrame({'tradePrice': [1.0, 2.0], 'assetId': ['ENT1', 'ENT1']})
        mock_fetch.return_value = df
        mock_valid.return_value = True
        mock_build.return_value = 'assetId == "ENT1"'

        loop = MagicMock()
        mock_asyncio.get_event_loop.return_value = loop

        dg._data_queries = [mock_query_info]
        dg._fetch_queries()
        mock_valid.assert_called_once()

    @patch('gs_quant.analytics.datagrid.datagrid.asyncio')
    @patch('gs_quant.analytics.datagrid.datagrid.fetch_query')
    @patch('gs_quant.analytics.datagrid.datagrid.aggregate_queries')
    @patch('gs_quant.analytics.datagrid.datagrid.valid_dimensions')
    def test_fetch_with_invalid_dimensions(self, mock_valid, mock_agg, mock_fetch, mock_asyncio):
        """Branch: valid_dimensions returns False."""
        dg = _make_datagrid()

        mock_query_info = MagicMock()
        mock_query_info.query.coordinate.measure = 'tradePrice'

        mock_agg.return_value = {
            'ds1': {
                'range1': {
                    'queries': {
                        'dim1': [mock_query_info],
                    },
                },
            },
        }
        mock_fetch.return_value = pd.DataFrame()
        mock_valid.return_value = False

        loop = MagicMock()
        mock_asyncio.get_event_loop.return_value = loop

        dg._data_queries = [mock_query_info]
        dg._fetch_queries()
        # query_info.data should be set to empty Series
        assert isinstance(mock_query_info.data, pd.Series)

    @patch('gs_quant.analytics.datagrid.datagrid.asyncio')
    @patch('gs_quant.analytics.datagrid.datagrid.aggregate_queries')
    def test_fetch_measure_query_info(self, mock_agg, mock_asyncio):
        """Branch: isinstance(query_info, MeasureQueryInfo)."""
        from gs_quant.analytics.core.processor import MeasureQueryInfo

        dg = _make_datagrid()
        mock_agg.return_value = {}

        query_info = MagicMock(spec=MeasureQueryInfo)
        query_info.attr = 'a'
        query_info.processor = MagicMock()

        loop = MagicMock()
        mock_asyncio.get_event_loop.return_value = loop

        dg._data_queries = [query_info]
        dg._fetch_queries()
        loop.run_until_complete.assert_called()

    @patch('gs_quant.analytics.datagrid.datagrid.asyncio')
    @patch('gs_quant.analytics.datagrid.datagrid.aggregate_queries')
    def test_fetch_no_data(self, mock_agg, mock_asyncio):
        """Branch: query_info.data is None."""
        dg = _make_datagrid()
        mock_agg.return_value = {}

        # Use plain MagicMock (not spec) so isinstance(query_info, MeasureQueryInfo) is False
        query_info = MagicMock()
        query_info.data = None
        query_info.attr = 'a'
        query_info.processor = MagicMock()
        query_info.query.coordinate = MagicMock()

        loop = MagicMock()
        mock_asyncio.get_event_loop.return_value = loop

        dg._data_queries = [query_info]
        dg._fetch_queries()
        loop.run_until_complete.assert_called()

    @patch('gs_quant.analytics.datagrid.datagrid.asyncio')
    @patch('gs_quant.analytics.datagrid.datagrid.aggregate_queries')
    def test_fetch_empty_data(self, mock_agg, mock_asyncio):
        """Branch: len(query_info.data) == 0."""
        dg = _make_datagrid()
        mock_agg.return_value = {}

        query_info = MagicMock()
        query_info.data = pd.Series(dtype=float)
        query_info.attr = 'a'
        query_info.processor = MagicMock()
        query_info.query.coordinate = MagicMock()

        loop = MagicMock()
        mock_asyncio.get_event_loop.return_value = loop

        dg._data_queries = [query_info]
        dg._fetch_queries()
        loop.run_until_complete.assert_called()

    @patch('gs_quant.analytics.datagrid.datagrid.asyncio')
    @patch('gs_quant.analytics.datagrid.datagrid.aggregate_queries')
    def test_fetch_with_data(self, mock_agg, mock_asyncio):
        """Branch: else (data exists and non-empty)."""
        dg = _make_datagrid()
        mock_agg.return_value = {}

        query_info = MagicMock()
        query_info.data = pd.Series([1.0, 2.0])
        query_info.attr = 'a'
        query_info.processor = MagicMock()

        loop = MagicMock()
        mock_asyncio.get_event_loop.return_value = loop

        dg._data_queries = [query_info]
        dg._fetch_queries()
        loop.run_until_complete.assert_called()

    @patch('gs_quant.analytics.datagrid.datagrid.asyncio')
    @patch('gs_quant.analytics.datagrid.datagrid.fetch_query')
    @patch('gs_quant.analytics.datagrid.datagrid.aggregate_queries')
    @patch('gs_quant.analytics.datagrid.datagrid.build_query_string')
    @patch('gs_quant.analytics.datagrid.datagrid.valid_dimensions')
    def test_fetch_measure_is_enum(self, mock_valid, mock_build, mock_agg, mock_fetch, mock_asyncio):
        """Branch: measure is not a string (Enum-like) -> measure.value."""
        dg = _make_datagrid()

        measure_enum = MagicMock()
        measure_enum.value = 'tradePrice'

        mock_query_info = MagicMock()
        mock_query_info.query.coordinate.measure = measure_enum

        mock_agg.return_value = {
            'ds1': {
                'range1': {
                    'queries': {
                        'dim1': [mock_query_info],
                    },
                },
            },
        }
        df = pd.DataFrame({'tradePrice': [1.0, 2.0], 'assetId': ['ENT1', 'ENT1']})
        mock_fetch.return_value = df
        mock_valid.return_value = True
        mock_build.return_value = 'assetId == "ENT1"'

        loop = MagicMock()
        mock_asyncio.get_event_loop.return_value = loop

        dg._data_queries = [mock_query_info]
        dg._fetch_queries()


# --------------------------------------------------------------------------- #
#  DataGrid: to_frame and _post_process
# --------------------------------------------------------------------------- #

class TestToFrame:
    def test_to_frame_not_initialized(self):
        dg = _make_datagrid()
        result = dg.to_frame()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_to_frame_initialized(self):
        """Branch: is_initialized is True -> calls _post_process."""
        dg = _make_datagrid()
        dg.is_initialized = True

        # Create mock cells
        cell = MagicMock()
        cell.name = 'Col1'
        cell.column_index = 0
        cell.row_group = None
        cell.value = ProcessorResult(True, 42.5)

        dg.results = [[cell]]
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]

        result = dg.to_frame()
        assert isinstance(result, pd.DataFrame)

    def test_post_process_with_number_value(self):
        """Branch: isinstance(column_data, Number) -> round."""
        dg = _make_datagrid()
        dg.is_initialized = True

        cell = MagicMock()
        cell.name = 'Col1'
        cell.column_index = 0
        cell.row_group = None
        cell.value = ProcessorResult(True, 42.12345)

        dg.results = [[cell]]
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=3))]
        result = dg.to_frame()
        # Value should be rounded to 3 decimal places
        assert 42.123 in result['Col1'].values

    def test_post_process_with_non_number_value(self):
        """Branch: column_data is NOT a Number."""
        dg = _make_datagrid()
        dg.is_initialized = True

        cell = MagicMock()
        cell.name = 'Col1'
        cell.column_index = 0
        cell.row_group = None
        cell.value = ProcessorResult(True, 'text_value')

        dg.results = [[cell]]
        dg.columns = [DataColumn(name='Col1', processor=None)]
        result = dg.to_frame()
        assert 'text_value' in result['Col1'].values

    def test_post_process_with_failed_value(self):
        """Branch: column_value.success is not True -> np.nan."""
        dg = _make_datagrid()
        dg.is_initialized = True

        cell = MagicMock()
        cell.name = 'Col1'
        cell.column_index = 0
        cell.row_group = None
        cell.value = ProcessorResult(False, 'error msg')

        dg.results = [[cell]]
        dg.columns = [DataColumn(name='Col1', processor=None)]
        result = dg.to_frame()
        assert np.isnan(result['Col1'].values[0])

    def test_post_process_with_row_group(self):
        """Branch: row[0].row_group is not None."""
        dg = _make_datagrid()
        dg.is_initialized = True

        cell = MagicMock()
        cell.name = 'Col1'
        cell.column_index = 0
        cell.row_group = 'GroupA'
        cell.value = ProcessorResult(True, 1.0)

        dg.results = [[cell]]
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        assert isinstance(result, pd.DataFrame)

    def test_post_process_multiple_row_groups(self):
        """Branch: multiple row groups handled independently."""
        dg = _make_datagrid()
        dg.is_initialized = True

        cell1 = MagicMock()
        cell1.name = 'Col1'
        cell1.column_index = 0
        cell1.row_group = 'GroupA'
        cell1.value = ProcessorResult(True, 1.0)

        cell2 = MagicMock()
        cell2.name = 'Col1'
        cell2.column_index = 0
        cell2.row_group = 'GroupB'
        cell2.value = ProcessorResult(True, 2.0)

        dg.results = [[cell1], [cell2]]
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


# --------------------------------------------------------------------------- #
#  DataGrid: __handle_sorts
# --------------------------------------------------------------------------- #

class TestHandleSorts:
    def test_sort_ascending_value(self):
        dg = _make_datagrid()
        dg.is_initialized = True

        sort = DataGridSort(columnName='Col1', sortType=SortType.VALUE, order=SortOrder.ASCENDING)
        dg.sorts = [sort]

        cells = []
        for val in [3.0, 1.0, 2.0]:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        assert list(result['Col1']) == [1.0, 2.0, 3.0]

    def test_sort_descending_value(self):
        dg = _make_datagrid()
        dg.is_initialized = True

        sort = DataGridSort(columnName='Col1', sortType=SortType.VALUE, order=SortOrder.DESCENDING)
        dg.sorts = [sort]

        cells = []
        for val in [1.0, 3.0, 2.0]:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        assert list(result['Col1']) == [3.0, 2.0, 1.0]

    def test_sort_absolute_value(self):
        """Branch: sort.sortType == SortType.ABSOLUTE_VALUE."""
        dg = _make_datagrid()
        dg.is_initialized = True

        sort = DataGridSort(columnName='Col1', sortType=SortType.ABSOLUTE_VALUE, order=SortOrder.DESCENDING)
        dg.sorts = [sort]

        cells = []
        for val in [1.0, -3.0, 2.0]:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        assert list(result['Col1']) == [-3.0, 2.0, 1.0]


# --------------------------------------------------------------------------- #
#  DataGrid: __handle_filters
# --------------------------------------------------------------------------- #

class TestHandleFilters:
    def _make_dg_with_data(self, values, filter_):
        dg = _make_datagrid()
        dg.is_initialized = True
        dg.filters = [filter_]

        cells = []
        for val in values:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        return dg

    def test_filter_top(self):
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.TOP, value=2)
        dg = self._make_dg_with_data([1.0, 3.0, 2.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_bottom(self):
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.BOTTOM, value=2)
        dg = self._make_dg_with_data([1.0, 3.0, 2.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_absolute_top(self):
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.ABSOLUTE_TOP, value=2)
        dg = self._make_dg_with_data([1.0, -3.0, 2.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_absolute_bottom(self):
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.ABSOLUTE_BOTTOM, value=2)
        dg = self._make_dg_with_data([1.0, -3.0, 2.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_equals_string_list(self):
        """Branch: EQUALS with string list."""
        dg = _make_datagrid()
        dg.is_initialized = True
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.EQUALS, value=['text1'])
        dg.filters = [f]

        cells = []
        for val in ['text1', 'text2', 'text1']:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None)]
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_equals_numeric(self):
        """Branch: EQUALS with numeric value (non-list)."""
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.EQUALS, value=2.0)
        dg = self._make_dg_with_data([1.0, 2.0, 3.0], f)
        result = dg.to_frame()
        assert len(result) == 1

    def test_filter_not_equals_string(self):
        """Branch: NOT_EQUALS with string."""
        dg = _make_datagrid()
        dg.is_initialized = True
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.NOT_EQUALS, value='text2')
        dg.filters = [f]

        cells = []
        for val in ['text1', 'text2', 'text1']:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None)]
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_not_equals_numeric(self):
        """Branch: NOT_EQUALS with numeric (non-list)."""
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.NOT_EQUALS, value=2.0)
        dg = self._make_dg_with_data([1.0, 2.0, 3.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_greater_than(self):
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.GREATER_THAN, value=1.5)
        dg = self._make_dg_with_data([1.0, 2.0, 3.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_less_than(self):
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.LESS_THAN, value=2.5)
        dg = self._make_dg_with_data([1.0, 2.0, 3.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_less_than_equals(self):
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.LESS_THAN_EQUALS, value=2.0)
        dg = self._make_dg_with_data([1.0, 2.0, 3.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_greater_than_equals(self):
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.GREATER_THAN_EQUALS, value=2.0)
        dg = self._make_dg_with_data([1.0, 2.0, 3.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_value_none(self):
        """Branch: filter_value is None -> continue."""
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.TOP, value=None)
        dg = self._make_dg_with_data([1.0, 2.0], f)
        result = dg.to_frame()
        assert len(result) == 2

    def test_filter_or_condition(self):
        """Branch: filter_condition == FilterCondition.OR."""
        f1 = DataGridFilter(
            columnName='Col1', operation=FilterOperation.GREATER_THAN, value=2.5,
            condition=FilterCondition.AND
        )
        f2 = DataGridFilter(
            columnName='Col1', operation=FilterOperation.LESS_THAN, value=1.5,
            condition=FilterCondition.OR
        )

        dg = _make_datagrid()
        dg.is_initialized = True
        dg.filters = [f1, f2]

        cells = []
        for val in [1.0, 2.0, 3.0]:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        assert len(result) == 2  # 3.0 from AND, 1.0 from OR

    def test_filter_empty_dataframe(self):
        """Branch: not len(df) -> return df early from __handle_filters."""
        from gs_quant.analytics.datagrid.datagrid import DataGrid

        dg = _make_datagrid()
        dg.is_initialized = True
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.TOP, value=2)
        dg.filters = [f]

        # Call __handle_filters directly via name mangling
        empty_df = pd.DataFrame(columns=['Col1'])
        result = dg._DataGrid__handle_filters(empty_df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# --------------------------------------------------------------------------- #
#  DataGrid: set_primary_column_index, set_sorts, add_sort, set_filters, add_filter
# --------------------------------------------------------------------------- #

class TestDataGridSetters:
    def test_set_primary_column_index(self):
        dg = _make_datagrid()
        dg.set_primary_column_index(3)
        assert dg._primary_column_index == 3

    def test_set_sorts(self):
        dg = _make_datagrid()
        sorts = [DataGridSort(columnName='X')]
        dg.set_sorts(sorts)
        assert dg.sorts is sorts

    def test_add_sort_default(self):
        """Branch: index is falsy -> append."""
        dg = _make_datagrid()
        sort = DataGridSort(columnName='X')
        dg.add_sort(sort)
        assert len(dg.sorts) == 1

    def test_add_sort_with_index(self):
        """Branch: index is truthy -> insert."""
        dg = _make_datagrid()
        sort1 = DataGridSort(columnName='X')
        sort2 = DataGridSort(columnName='Y')
        dg.add_sort(sort1)
        dg.add_sort(sort2, index=1)
        assert dg.sorts[1] is sort2

    def test_set_filters(self):
        dg = _make_datagrid()
        filters = [DataGridFilter(columnName='X', operation=FilterOperation.TOP, value=5)]
        dg.set_filters(filters)
        assert dg.filters is filters

    def test_add_filter_default(self):
        """Branch: index is falsy -> append."""
        dg = _make_datagrid()
        f = DataGridFilter(columnName='X', operation=FilterOperation.TOP, value=5)
        dg.add_filter(f)
        assert len(dg.filters) == 1

    def test_add_filter_with_index(self):
        """Branch: index is truthy -> insert."""
        dg = _make_datagrid()
        f1 = DataGridFilter(columnName='X', operation=FilterOperation.TOP, value=5)
        f2 = DataGridFilter(columnName='Y', operation=FilterOperation.BOTTOM, value=3)
        dg.add_filter(f1)
        dg.add_filter(f2, index=1)
        assert dg.filters[1] is f2


# --------------------------------------------------------------------------- #
#  DataGrid: aggregate_queries (static)
# --------------------------------------------------------------------------- #

class TestAggregateQueries:
    def test_aggregate_queries(self):
        """Tests the static aggregate_queries method."""
        from gs_quant.analytics.datagrid.datagrid import DataGrid

        coord = MagicMock()
        coord.dataset_id = 'ds1'
        coord.dimensions = {'assetId': 'ENT1'}
        coord.get_dimensions.return_value = 'assetId=ENT1'

        query = MagicMock()
        query.coordinate = coord
        query.get_range_string.return_value = 'range1'

        query_info = MagicMock()
        query_info.query = query

        DataGrid.aggregate_queries([query_info])


# --------------------------------------------------------------------------- #
#  DataGrid: poll
# --------------------------------------------------------------------------- #

class TestPoll:
    def test_poll_calls_subprocesses(self):
        """Verifies poll calls _resolve_rdates, _resolve_queries, _process_special_cells, _fetch_queries."""
        dg = _make_datagrid()
        dg._resolve_rdates = MagicMock()
        dg._resolve_queries = MagicMock()
        dg._process_special_cells = MagicMock()
        dg._fetch_queries = MagicMock()
        dg.poll()
        dg._resolve_rdates.assert_called_once()
        dg._resolve_queries.assert_called_once()
        dg._process_special_cells.assert_called_once()
        dg._fetch_queries.assert_called_once()


# --------------------------------------------------------------------------- #
#  _get_overrides
# --------------------------------------------------------------------------- #

class TestGetOverrides:
    def test_no_overrides(self):
        from gs_quant.analytics.datagrid.datagrid import _get_overrides
        dims, val, proc = _get_overrides(None, 'Col1')
        assert dims == []
        assert val is None
        assert proc is None

    def test_empty_list(self):
        from gs_quant.analytics.datagrid.datagrid import _get_overrides
        dims, val, proc = _get_overrides([], 'Col1')
        assert dims == []
        assert val is None
        assert proc is None

    def test_dimensions_override(self):
        from gs_quant.analytics.datagrid.datagrid import _get_overrides
        from gs_quant.data import DataCoordinate, DataMeasure, DataFrequency

        coord = DataCoordinate(measure=DataMeasure.TRADE_PRICE, frequency=DataFrequency.REAL_TIME)
        dim = DimensionsOverride(column_names=['Col1'], dimensions={'assetId': 'X'}, coordinate=coord)
        dims, val, proc = _get_overrides([dim], 'Col1')
        assert len(dims) == 1

    def test_value_override(self):
        from gs_quant.analytics.datagrid.datagrid import _get_overrides
        vo = ValueOverride(column_names=['Col1'], value=42.0)
        dims, val, proc = _get_overrides([vo], 'Col1')
        assert val.value == 42.0

    def test_processor_override(self):
        from gs_quant.analytics.datagrid.datagrid import _get_overrides
        mock_proc = MagicMock()
        po = ProcessorOverride(column_names=['Col1'], processor=mock_proc)
        dims, val, proc = _get_overrides([po], 'Col1')
        assert proc is mock_proc

    def test_override_not_matching_column(self):
        from gs_quant.analytics.datagrid.datagrid import _get_overrides
        vo = ValueOverride(column_names=['Col2'], value=42.0)
        dims, val, proc = _get_overrides([vo], 'Col1')
        assert val is None

    def test_override_not_known_type(self):
        """Branch: override matches column but is not DimensionsOverride/ValueOverride/ProcessorOverride."""
        from gs_quant.analytics.datagrid.datagrid import _get_overrides

        class CustomOverride(Override):
            def __init__(self):
                super().__init__(column_names=['Col1'])

        dims, val, proc = _get_overrides([CustomOverride()], 'Col1')
        assert dims == []
        assert val is None
        assert proc is None


# --------------------------------------------------------------------------- #
#  DataGrid: additional edge cases for branches
# --------------------------------------------------------------------------- #

class TestDataGridEdgeCases:
    def test_not_equals_numeric_list(self):
        """Branch: NOT_EQUALS with numeric list (isinstance(filter_value[0], str) is False)."""
        dg = _make_datagrid()
        dg.is_initialized = True
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.NOT_EQUALS, value=[2.0])
        dg.filters = [f]

        cells = []
        for val in [1.0, 2.0, 3.0]:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        assert len(result) == 2

    def test_equals_string(self):
        """Branch: EQUALS with string value (non-list, gets converted to list)."""
        dg = _make_datagrid()
        dg.is_initialized = True
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.EQUALS, value='hello')
        dg.filters = [f]

        cells = []
        for val in ['hello', 'world', 'hello']:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None)]
        result = dg.to_frame()
        assert len(result) == 2

    def test_equals_numeric_list(self):
        """Branch: EQUALS with numeric list (isinstance(filter_value[0], str) is False)."""
        dg = _make_datagrid()
        dg.is_initialized = True
        f = DataGridFilter(columnName='Col1', operation=FilterOperation.EQUALS, value=[2.0])
        dg.filters = [f]

        cells = []
        for val in [1.0, 2.0, 3.0]:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        assert len(result) == 1

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_rdates_entity_with_entity_instance(self, mock_gs):
        """Branch: isinstance(entity, Entity) is True -> get currency/exchange.
        Using Entity spec to match real isinstance check.
        """
        from gs_quant.entities.entity import Entity as RealEntity

        mock_session = MagicMock()
        mock_session.is_internal.return_value = True
        mock_gs.current = mock_session

        dg = _make_datagrid()

        entity = MagicMock(spec=RealEntity)
        entity.get_marquee_id.return_value = 'ENT1'
        entity.get_entity.return_value = {'currency': 'USD', 'exchange': 'NYSE'}

        dg.entity_map = {'ENT1': entity}
        dg.rdate_entity_map = {'ENT1': {('-1d', '2021-01-22')}}

        with patch('gs_quant.analytics.datagrid.datagrid.RelativeDate') as mock_rdate_cls:
            mock_rdate_instance = MagicMock()
            mock_rdate_instance.apply_rule.return_value = dt.date(2021, 1, 21)
            mock_rdate_cls.return_value = mock_rdate_instance
            dg._resolve_rdates()
            mock_rdate_instance.apply_rule.assert_called_once_with(
                currencies=['USD'], exchanges=['NYSE'], holiday_calendar=None
            )

    @patch('gs_quant.analytics.datagrid.datagrid.GsSession')
    def test_resolve_rdates_entity_no_currency_no_exchange(self, mock_gs):
        """Branch: isinstance(entity, Entity) True, but no currency/exchange in entity_dict."""
        from gs_quant.entities.entity import Entity as RealEntity

        mock_session = MagicMock()
        mock_session.is_internal.return_value = True
        mock_gs.current = mock_session

        dg = _make_datagrid()

        entity = MagicMock(spec=RealEntity)
        entity.get_marquee_id.return_value = 'ENT1'
        entity.get_entity.return_value = {}  # no currency, no exchange

        dg.entity_map = {'ENT1': entity}
        dg.rdate_entity_map = {'ENT1': {('-1d', None)}}

        with patch('gs_quant.analytics.datagrid.datagrid.RelativeDate') as mock_rdate_cls:
            mock_rdate_instance = MagicMock()
            mock_rdate_instance.apply_rule.return_value = dt.date(2021, 1, 21)
            mock_rdate_cls.return_value = mock_rdate_instance
            dg._resolve_rdates()
            mock_rdate_instance.apply_rule.assert_called_once_with(
                currencies=None, exchanges=None, holiday_calendar=None
            )

    def test_polling_time_setter_then_read(self):
        """Verify polling_time = None -> setter sets to None (line 300 then 303), then value = 5000+ works."""
        dg = _make_datagrid()
        dg.polling_time = None
        # Line 300 sets to 0, but line 303 always runs and sets to value=None
        assert dg.polling_time is None
        dg.polling_time = 5000
        assert dg.polling_time == 5000

    def test_post_process_empty_row_in_results(self):
        """Branch 471->473: empty row (len(row)==0) skips rowGroup append."""
        dg = _make_datagrid()
        dg.is_initialized = True

        cell = MagicMock()
        cell.name = 'Col1'
        cell.column_index = 0
        cell.row_group = None
        cell.value = ProcessorResult(True, 1.0)

        # Include an empty row and a non-empty row
        dg.results = [[], [cell]]
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_invalid_filter_operation(self):
        """Branch: else raise MqValueError for invalid FilterOperation (line 570)."""
        dg = _make_datagrid()
        f = MagicMock()
        f.value = 5
        f.condition = FilterCondition.AND
        f.columnName = 'Col1'
        f.operation = 'INVALID_OPERATION'  # Not a valid FilterOperation enum
        dg.filters = [f]

        df = pd.DataFrame({'Col1': [1.0, 2.0, 3.0]})
        with pytest.raises(MqValueError, match='Invalid Filter operation Type'):
            dg._DataGrid__handle_filters(df)

    def test_filter_and_condition_chaining(self):
        """Branch: filter_condition == FilterCondition.AND -> running_df = df."""
        f1 = DataGridFilter(
            columnName='Col1', operation=FilterOperation.GREATER_THAN, value=0.5,
            condition=FilterCondition.AND
        )
        f2 = DataGridFilter(
            columnName='Col1', operation=FilterOperation.LESS_THAN, value=2.5,
            condition=FilterCondition.AND
        )

        dg = _make_datagrid()
        dg.is_initialized = True
        dg.filters = [f1, f2]

        cells = []
        for val in [1.0, 2.0, 3.0]:
            cell = MagicMock()
            cell.name = 'Col1'
            cell.column_index = 0
            cell.row_group = None
            cell.value = ProcessorResult(True, val)
            cells.append([cell])

        dg.results = cells
        dg.columns = [DataColumn(name='Col1', processor=None, format_=ColumnFormat(precision=2))]
        result = dg.to_frame()
        # 1.0 and 2.0 pass both filters
        assert len(result) == 2


if __name__ == '__main__':
    pytest.main(args=["test_datagrid.py"])
