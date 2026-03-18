"""
Tests for gs_quant/analytics/common/helpers.py, constants.py, enumerators.py
Covers all helper functions, constants, and ScaleShape enum.
"""

import datetime as dt

import pytest
from unittest.mock import MagicMock, patch

from gs_quant.analytics.common.helpers import (
    is_of_builtin_type,
    resolve_entities,
    get_rdate_cache_key,
    get_entity_rdate_key,
    get_entity_rdate_key_from_rdate,
)
from gs_quant.analytics.common.constants import (
    DATAGRID_HELP_MSG,
    DATA_CELL_NOT_CALCULATED,
    CELL_GRAPH,
    QUERIES_TO_PROCESSORS,
    DATA_COORDINATE,
    PROCESSOR,
    PROCESSOR_NAME,
    ENTITY,
    ENTITY_ID,
    ENTITY_TYPE,
    DATE,
    DATETIME,
    TYPE,
    LIST,
    VALUE,
    PARAMETER,
    PARAMETERS,
    DATA_ROW,
    REFERENCE,
    RELATIVE_DATE,
)
from gs_quant.analytics.common.enumerators import ScaleShape
from gs_quant.datetime.relative_date import RelativeDate
from gs_quant.errors import MqValueError, MqRequestError


# ---------------------------------------------------------------------------
# helpers.py: is_of_builtin_type
# ---------------------------------------------------------------------------
class TestIsOfBuiltinType:
    @pytest.mark.parametrize('value', [42, 3.14, 'hello', True, None, [1, 2], {'a': 1}])
    def test_builtin_types_return_true(self, value):
        assert is_of_builtin_type(value) is True

    def test_custom_class_returns_false(self):
        class Custom:
            pass
        assert is_of_builtin_type(Custom()) is False

    def test_enum_returns_false(self):
        assert is_of_builtin_type(ScaleShape.DIAMOND) is False


# ---------------------------------------------------------------------------
# helpers.py: get_rdate_cache_key
# ---------------------------------------------------------------------------
class TestGetRdateCacheKey:
    def test_basic(self):
        result = get_rdate_cache_key('-1d', '2021-01-01', ['USD'], ['NYSE'])
        assert result == "-1d::2021-01-01::['USD']::['NYSE']"

    def test_empty_lists(self):
        result = get_rdate_cache_key('-2w', None, [], [])
        assert result == '-2w::None::[]::[]'


# ---------------------------------------------------------------------------
# helpers.py: get_entity_rdate_key
# ---------------------------------------------------------------------------
class TestGetEntityRdateKey:
    def test_basic(self):
        result = get_entity_rdate_key('entity_123', '-1d', '2021-01-01')
        assert result == 'entity_123::-1d::2021-01-01'

    def test_none_base_date(self):
        result = get_entity_rdate_key('eid', '-3m', None)
        assert result == 'eid::-3m::None'


# ---------------------------------------------------------------------------
# helpers.py: get_entity_rdate_key_from_rdate
# ---------------------------------------------------------------------------
class TestGetEntityRdateKeyFromRdate:
    def test_with_base_date(self):
        rdate = MagicMock(spec=RelativeDate)
        rdate.rule = '-1d'
        rdate.base_date_passed_in = True
        rdate.base_date = dt.date(2021, 6, 15)
        result = get_entity_rdate_key_from_rdate('eid', rdate)
        assert result == 'eid::-1d::2021-06-15'

    def test_without_base_date(self):
        rdate = MagicMock(spec=RelativeDate)
        rdate.rule = '-2w'
        rdate.base_date_passed_in = False
        result = get_entity_rdate_key_from_rdate('eid', rdate)
        assert result == 'eid::-2w::None'

    def test_empty_entity_id(self):
        rdate = MagicMock(spec=RelativeDate)
        rdate.rule = '-1m'
        rdate.base_date_passed_in = False
        result = get_entity_rdate_key_from_rdate('', rdate)
        assert result == '::-1m::None'


# ---------------------------------------------------------------------------
# helpers.py: resolve_entities
# ---------------------------------------------------------------------------
class TestResolveEntities:
    def test_data_row_reference(self):
        """When TYPE is DATA_ROW, sets entity on the reference object."""
        mock_entity = MagicMock()
        ref_obj = MagicMock()
        reference_list = [
            {TYPE: DATA_ROW, ENTITY_ID: 'eid1', ENTITY_TYPE: 'Asset', REFERENCE: ref_obj}
        ]
        with patch('gs_quant.analytics.common.helpers.Entity') as MockEntity:
            MockEntity.get.return_value = mock_entity
            resolve_entities(reference_list)
        assert ref_obj.entity == mock_entity

    def test_processor_reference(self):
        """When TYPE is PROCESSOR, sets attribute on processor and updates children."""
        mock_entity = MagicMock()
        ref_obj = MagicMock()
        child_info = MagicMock()
        ref_obj.children = {'my_param': child_info}
        ref_obj.__class__.__name__ = 'TestProcessor'

        reference_list = [
            {TYPE: PROCESSOR, ENTITY_ID: 'eid2', ENTITY_TYPE: 'Country',
             PARAMETER: 'my_param', REFERENCE: ref_obj}
        ]
        with patch('gs_quant.analytics.common.helpers.Entity') as MockEntity:
            MockEntity.get.return_value = mock_entity
            resolve_entities(reference_list)

        assert ref_obj.my_param == mock_entity
        assert child_info.entity == mock_entity

    def test_processor_reference_missing_child_raises(self):
        """When parameter not in children, raises MqValueError."""
        mock_entity = MagicMock()
        ref_obj = MagicMock()
        ref_obj.children = {}  # no children
        ref_obj.__class__.__name__ = 'TestProcessor'

        reference_list = [
            {TYPE: PROCESSOR, ENTITY_ID: 'eid3', ENTITY_TYPE: 'Asset',
             PARAMETER: 'missing', REFERENCE: ref_obj}
        ]
        with patch('gs_quant.analytics.common.helpers.Entity') as MockEntity:
            MockEntity.get.return_value = mock_entity
            with pytest.raises(MqValueError):
                resolve_entities(reference_list)

    def test_entity_from_cache(self):
        """When entity_id is in entity_cache, should use cached value."""
        cached_entity = MagicMock()
        ref_obj = MagicMock()
        reference_list = [
            {TYPE: DATA_ROW, ENTITY_ID: 'cached_id', ENTITY_TYPE: 'Asset', REFERENCE: ref_obj}
        ]
        resolve_entities(reference_list, entity_cache={'cached_id': cached_entity})
        assert ref_obj.entity == cached_entity

    def test_entity_fetch_error_falls_back_to_id(self):
        """When Entity.get raises MqRequestError, falls back to entity_id string."""
        ref_obj = MagicMock()
        reference_list = [
            {TYPE: DATA_ROW, ENTITY_ID: 'bad_id', ENTITY_TYPE: 'Asset', REFERENCE: ref_obj}
        ]
        with patch('gs_quant.analytics.common.helpers.Entity') as MockEntity:
            MockEntity.get.side_effect = MqRequestError(400, 'Not found')
            resolve_entities(reference_list)
        assert ref_obj.entity == 'bad_id'

    def test_empty_reference_list(self):
        """Should handle empty list without error."""
        resolve_entities([])

    def test_none_entity_cache_defaults(self):
        """When entity_cache is None, should default to empty dict."""
        ref_obj = MagicMock()
        reference_list = [
            {TYPE: DATA_ROW, ENTITY_ID: 'eid', ENTITY_TYPE: 'Asset', REFERENCE: ref_obj}
        ]
        with patch('gs_quant.analytics.common.helpers.Entity') as MockEntity:
            MockEntity.get.return_value = MagicMock()
            resolve_entities(reference_list, entity_cache=None)


# ---------------------------------------------------------------------------
# constants.py
# ---------------------------------------------------------------------------
class TestConstants:
    def test_constant_values(self):
        assert DATA_COORDINATE == 'dataCoordinate'
        assert PROCESSOR == 'processor'
        assert PROCESSOR_NAME == 'processorName'
        assert ENTITY == 'entity'
        assert ENTITY_ID == 'entityId'
        assert ENTITY_TYPE == 'entityType'
        assert DATE == 'date'
        assert DATETIME == 'datetime'
        assert TYPE == 'type'
        assert LIST == 'list'
        assert VALUE == 'value'
        assert PARAMETER == 'parameter'
        assert PARAMETERS == 'parameters'
        assert DATA_ROW == 'dataRow'
        assert REFERENCE == 'reference'
        assert RELATIVE_DATE == 'relativeDate'
        assert CELL_GRAPH == 'cell_graph'
        assert QUERIES_TO_PROCESSORS == 'queries_to_processors'

    def test_help_message_is_string(self):
        assert isinstance(DATAGRID_HELP_MSG, str)
        assert len(DATAGRID_HELP_MSG) > 0

    def test_not_calculated_message(self):
        assert DATA_CELL_NOT_CALCULATED == 'Cell has not been calculated'


# ---------------------------------------------------------------------------
# enumerators.py: ScaleShape
# ---------------------------------------------------------------------------
class TestScaleShape:
    def test_values(self):
        assert ScaleShape.DIAMOND.value == 'diamond'
        assert ScaleShape.PIPE.value == 'pipe'
        assert ScaleShape.BAR.value == 'bar'

    def test_from_value(self):
        assert ScaleShape('diamond') == ScaleShape.DIAMOND
        assert ScaleShape('pipe') == ScaleShape.PIPE
        assert ScaleShape('bar') == ScaleShape.BAR

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError):
            ScaleShape('invalid')

    def test_members_count(self):
        assert len(ScaleShape) == 3

    def test_is_enum(self):
        from enum import Enum
        assert issubclass(ScaleShape, Enum)
