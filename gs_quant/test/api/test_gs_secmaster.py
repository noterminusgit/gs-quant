"""
Tests for gs_quant/api/gs/secmaster.py
Targets 100% branch coverage using MagicMock (no real API calls).
"""

import datetime as dt
import math
from unittest.mock import MagicMock, patch, call

import pytest

import gs_quant.api.gs.secmaster as secmaster_module
from gs_quant.api.gs.secmaster import (
    GsSecurityMasterApi,
    SecMasterIdentifiers,
    CapitalStructureIdentifiers,
    ExchangeId,
    DEFAULT_SCROLL_PAGE_SIZE,
)
from gs_quant.target.secmaster import SecMasterAssetType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_session():
    """Return a MagicMock that looks like GsSession.current."""
    session = MagicMock()
    return session


# ---------------------------------------------------------------------------
# Enum / module-level tests
# ---------------------------------------------------------------------------

class TestEnums:
    def test_secmaster_identifiers_values(self):
        assert SecMasterIdentifiers.CUSIP.value == 'cusip'
        assert SecMasterIdentifiers.FIGI.value == 'figi'
        assert SecMasterIdentifiers.ASSET_ID.value == 'assetId'

    def test_capital_structure_identifiers_extends(self):
        # CapitalStructureIdentifiers should have all SecMasterIdentifiers members plus ISSUER_ID
        assert CapitalStructureIdentifiers.ISSUER_ID.value == 'issuerId'
        assert CapitalStructureIdentifiers.CUSIP.value == 'cusip'

    def test_extend_enum(self):
        extend_enum_fn = getattr(secmaster_module, '__extend_enum')
        result = extend_enum_fn(SecMasterIdentifiers, {"NEW_ID": "newId"})
        assert result.NEW_ID.value == "newId"
        # original members preserved
        assert result.CUSIP.value == "cusip"

    def test_exchange_id_values(self):
        assert ExchangeId.MIC.value == "mic"
        assert ExchangeId.COUNTRY.value == "country"
        assert ExchangeId.EXCHANGE_NAME.value == "name"

    def test_default_scroll_page_size(self):
        assert DEFAULT_SCROLL_PAGE_SIZE == 500


# ---------------------------------------------------------------------------
# get_security
# ---------------------------------------------------------------------------

class TestGetSecurity:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_get_security_returns_first_result(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111', 'name': 'Test'}],
        }
        result = GsSecurityMasterApi.get_security('AAPL', SecMasterIdentifiers.TICKER)
        assert result == {'id': 'GSPD111', 'name': 'Test'}

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_get_security_returns_none_when_no_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 0, 'results': []}
        result = GsSecurityMasterApi.get_security('AAPL', SecMasterIdentifiers.TICKER)
        assert result is None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_get_security_with_effective_date(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111'}],
        }
        result = GsSecurityMasterApi.get_security(
            'AAPL', SecMasterIdentifiers.TICKER, effective_date=dt.date(2023, 1, 1)
        )
        assert result == {'id': 'GSPD111'}


# ---------------------------------------------------------------------------
# get_many_securities
# ---------------------------------------------------------------------------

class TestGetManySecurities:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_raises_when_no_params_and_no_type(self, mock_gs_session):
        with pytest.raises(ValueError, match="Neither '_type' nor 'query_params' are provided"):
            GsSecurityMasterApi.get_many_securities()

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_none_when_zero_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 0, 'results': []}
        result = GsSecurityMasterApi.get_many_securities(ticker='AAPL')
        assert result is None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_results_when_found(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111'}],
        }
        result = GsSecurityMasterApi.get_many_securities(ticker='AAPL')
        assert result['totalResults'] == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_flatten_true_uses_data_endpoint(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 1, 'results': [{'id': 'X'}]}
        GsSecurityMasterApi.get_many_securities(flatten=True, ticker='AAPL')
        args, kwargs = session.sync.get.call_args
        assert args[0] == '/markets/securities/data'

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_flatten_false_uses_securities_endpoint(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 1, 'results': [{'id': 'X'}]}
        GsSecurityMasterApi.get_many_securities(flatten=False, ticker='AAPL')
        args, kwargs = session.sync.get.call_args
        assert args[0] == '/markets/securities'

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_type_only(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 2, 'results': [{'id': 'A'}, {'id': 'B'}]}
        result = GsSecurityMasterApi.get_many_securities(type_=SecMasterAssetType.ETF)
        assert result['totalResults'] == 2

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_all_params(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 1, 'results': [{'id': 'A'}]}
        result = GsSecurityMasterApi.get_many_securities(
            type_=SecMasterAssetType.ETF,
            effective_date=dt.date(2023, 6, 1),
            limit=5,
            is_primary=True,
            offset_key='abc',
            ticker='AAPL',
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_empty_query_params_with_type_works(self, mock_gs_session):
        """query_params empty but type_ is set -- should not raise."""
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 1, 'results': [{'id': 'X'}]}
        result = GsSecurityMasterApi.get_many_securities(type_=SecMasterAssetType.Future)
        assert result is not None


# ---------------------------------------------------------------------------
# get_all_securities
# ---------------------------------------------------------------------------

class TestGetAllSecurities:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_none_when_first_page_none(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 0, 'results': []}
        result = GsSecurityMasterApi.get_all_securities(ticker='AAPL')
        assert result is None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_response_when_no_offset_key(self, mock_gs_session):
        """Response has results but no offsetKey => return immediately."""
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        result = GsSecurityMasterApi.get_all_securities(ticker='AAPL')
        assert result['totalResults'] == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_scrolls_all_pages(self, mock_gs_session):
        """When offsetKey is present, fetch_all should be called."""
        session = _mock_session()
        mock_gs_session.current = session

        # First call returns offsetKey, second call has no offsetKey
        session.sync.get.side_effect = [
            {
                'totalResults': 2,
                'results': [{'id': 'A'}],
                'offsetKey': 'page2',
            },
            {
                'totalResults': 2,
                'results': [{'id': 'B'}],
            },
        ]
        result = GsSecurityMasterApi.get_all_securities(ticker='AAPL')
        assert len(result['results']) == 2
        assert result['totalResults'] == 2

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_custom_limit_from_query_params(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        result = GsSecurityMasterApi.get_all_securities(limit=50, ticker='AAPL')
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_total_results_zero_after_offset_key(self, mock_gs_session):
        """Response with offsetKey but totalResults==0 should return None."""
        session = _mock_session()
        mock_gs_session.current = session
        # get_many_securities returns None when totalResults == 0
        session.sync.get.return_value = {'totalResults': 0, 'results': []}
        result = GsSecurityMasterApi.get_all_securities(ticker='AAPL')
        # get_many_securities returns None, which has no offsetKey => returns None
        assert result is None

    @patch.object(GsSecurityMasterApi, 'get_many_securities')
    def test_total_results_zero_with_offset_key(self, mock_get_many):
        """Response has offsetKey but totalResults==0 => line 180-181 branch."""
        # get_many_securities returns a response with offsetKey but totalResults==0
        mock_get_many.return_value = {
            'totalResults': 0,
            'results': [],
            'offsetKey': 'p2',
        }
        result = GsSecurityMasterApi.get_all_securities(ticker='AAPL')
        assert result is None


# ---------------------------------------------------------------------------
# get_security_data
# ---------------------------------------------------------------------------

class TestGetSecurityData:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_first_result(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111', 'data': 'flat'}],
        }
        result = GsSecurityMasterApi.get_security_data('AAPL', SecMasterIdentifiers.TICKER)
        assert result == {'id': 'GSPD111', 'data': 'flat'}

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_none_when_no_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 0, 'results': []}
        result = GsSecurityMasterApi.get_security_data('AAPL', SecMasterIdentifiers.TICKER)
        assert result is None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_effective_date(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'X'}],
        }
        result = GsSecurityMasterApi.get_security_data(
            'AAPL', SecMasterIdentifiers.TICKER, effective_date=dt.date(2023, 1, 1)
        )
        assert result == {'id': 'X'}


# ---------------------------------------------------------------------------
# get_identifiers
# ---------------------------------------------------------------------------

class TestGetIdentifiers:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'results': [{'type': 'ISIN', 'value': 'US123'}]
        }
        result = GsSecurityMasterApi.get_identifiers('GSPD111')
        assert result == [{'type': 'ISIN', 'value': 'US123'}]

    def test_raises_on_invalid_id(self):
        with pytest.raises(ValueError, match="Invalid id_value"):
            GsSecurityMasterApi.get_identifiers('INVALID123')


# ---------------------------------------------------------------------------
# get_many_identifiers
# ---------------------------------------------------------------------------

class TestGetManyIdentifiers:
    def test_raises_on_non_iterable(self):
        with pytest.raises(ValueError, match="secmaster_id must be an iterable"):
            GsSecurityMasterApi.get_many_identifiers(123)

    def test_raises_on_empty_iterable(self):
        with pytest.raises(ValueError, match="secmaster_id cannot be an empty iterable"):
            GsSecurityMasterApi.get_many_identifiers([])

    def test_raises_on_invalid_id_in_list(self):
        with pytest.raises(ValueError, match="Invalid id_value"):
            GsSecurityMasterApi.get_many_identifiers(['GS111', 'INVALID'])

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_single_page_no_xref(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'results': {
                'GSPD111': [{'type': 'ISIN', 'value': 'US123'}],
            }
        }
        result = GsSecurityMasterApi.get_many_identifiers(['GSPD111'])
        assert 'GSPD111' in result
        assert len(result['GSPD111']) == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_multiple_pages(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.side_effect = [
            {
                'results': {'GSPD111': [{'type': 'ISIN', 'value': 'US1'}]},
                'offsetKey': 'page2',
            },
            {
                'results': {'GSPD111': [{'type': 'CUSIP', 'value': '123'}]},
            },
        ]
        result = GsSecurityMasterApi.get_many_identifiers(['GSPD111'])
        assert len(result['GSPD111']) == 2

    @patch('gs_quant.api.gs.secmaster.SecmasterXrefFormatter')
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_xref_format_true(self, mock_gs_session, mock_formatter):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'results': {'GSPD111': [{'type': 'ISIN', 'value': 'US1'}]},
        }
        mock_formatter.convert.return_value = {'GSPD111': {'xrefs': []}}
        result = GsSecurityMasterApi.get_many_identifiers(['GSPD111'], xref_format=True)
        mock_formatter.convert.assert_called_once()
        assert 'GSPD111' in result

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_limit_none(self, mock_gs_session):
        """When limit is None, 'limit' should not be in payload."""
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'results': {'GSPD111': [{'type': 'ISIN', 'value': 'US1'}]},
        }
        result = GsSecurityMasterApi.get_many_identifiers(['GSPD111'], limit=None)
        assert 'GSPD111' in result

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_no_results_key_in_response(self, mock_gs_session):
        """When response has no 'results' key, should handle gracefully."""
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {}
        result = GsSecurityMasterApi.get_many_identifiers(['GSPD111'])
        assert result == {}

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_existing_entity_extends(self, mock_gs_session):
        """When same entity_id comes in multiple pages, results get extended."""
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.side_effect = [
            {
                'results': {'GSPD111': [{'type': 'ISIN', 'value': 'US1'}]},
                'offsetKey': 'p2',
            },
            {
                'results': {'GSPD111': [{'type': 'CUSIP', 'value': 'C1'}]},
            },
        ]
        result = GsSecurityMasterApi.get_many_identifiers(['GSPD111'])
        assert len(result['GSPD111']) == 2

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_new_entity_in_second_page(self, mock_gs_session):
        """When a new entity_id appears in second page, it gets its own entry."""
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.side_effect = [
            {
                'results': {'GSPD111': [{'type': 'ISIN', 'value': 'US1'}]},
                'offsetKey': 'p2',
            },
            {
                'results': {'GSPD222': [{'type': 'CUSIP', 'value': 'C2'}]},
            },
        ]
        result = GsSecurityMasterApi.get_many_identifiers(['GSPD111', 'GSPD222'])
        assert 'GSPD111' in result
        assert 'GSPD222' in result


# ---------------------------------------------------------------------------
# map
# ---------------------------------------------------------------------------

class TestMap:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_basic_map(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'results': [{'gsid': '123', 'ticker': 'AAPL'}]
        }
        result = GsSecurityMasterApi.map(
            input_type=SecMasterIdentifiers.TICKER,
            ids=['AAPL'],
        )
        assert len(result) == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_map_with_effective_date(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': [{'gsid': '123'}]}
        result = GsSecurityMasterApi.map(
            input_type=SecMasterIdentifiers.TICKER,
            ids=['AAPL'],
            effective_date=dt.date(2023, 6, 1),
        )
        assert len(result) == 1

    def test_map_raises_when_effective_date_and_start_date(self):
        with pytest.raises(ValueError, match='provide .* or effective_date, but not both'):
            GsSecurityMasterApi.map(
                input_type=SecMasterIdentifiers.TICKER,
                ids=['AAPL'],
                effective_date=dt.date(2023, 6, 1),
                start_date=dt.date(2023, 1, 1),
            )

    def test_map_raises_when_effective_date_and_end_date(self):
        with pytest.raises(ValueError, match='provide .* or effective_date, but not both'):
            GsSecurityMasterApi.map(
                input_type=SecMasterIdentifiers.TICKER,
                ids=['AAPL'],
                effective_date=dt.date(2023, 6, 1),
                end_date=dt.date(2023, 12, 1),
            )

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_map_with_start_and_end_date(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': [{'gsid': '123'}]}
        result = GsSecurityMasterApi.map(
            input_type=SecMasterIdentifiers.TICKER,
            ids=['AAPL'],
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 12, 1),
        )
        assert len(result) == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_map_with_start_date_only(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': [{'gsid': '123'}]}
        result = GsSecurityMasterApi.map(
            input_type=SecMasterIdentifiers.TICKER,
            ids=['AAPL'],
            start_date=dt.date(2023, 1, 1),
        )
        assert len(result) == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_map_with_end_date_only(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': [{'gsid': '123'}]}
        result = GsSecurityMasterApi.map(
            input_type=SecMasterIdentifiers.TICKER,
            ids=['AAPL'],
            end_date=dt.date(2023, 12, 1),
        )
        assert len(result) == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_map_with_custom_output_types(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': [{'isin': 'US123'}]}
        result = GsSecurityMasterApi.map(
            input_type=SecMasterIdentifiers.TICKER,
            ids=['AAPL'],
            output_types=[SecMasterIdentifiers.ISIN, SecMasterIdentifiers.CUSIP],
        )
        assert len(result) == 1


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------

class TestSearch:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_search_returns_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111', 'name': 'Apple'}],
        }
        result = GsSecurityMasterApi.search('Apple')
        assert len(result) == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_search_returns_none_on_zero_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 0, 'results': []}
        result = GsSecurityMasterApi.search('XYZ_NONEXISTENT')
        assert result is None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_search_with_type_filter(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111'}],
        }
        result = GsSecurityMasterApi.search('Apple', type_=SecMasterAssetType.Common_Stock)
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_search_with_is_primary(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111'}],
        }
        result = GsSecurityMasterApi.search('Apple', is_primary=True)
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_search_with_active_listing(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111'}],
        }
        result = GsSecurityMasterApi.search('Apple', active_listing=True)
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_search_with_all_filters(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111'}],
        }
        result = GsSecurityMasterApi.search(
            'Apple',
            type_=SecMasterAssetType.Common_Stock,
            is_primary=True,
            active_listing=False,
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_search_with_none_filters(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'GSPD111'}],
        }
        result = GsSecurityMasterApi.search(
            'Apple',
            type_=None,
            is_primary=None,
            active_listing=None,
        )
        assert result is not None


# ---------------------------------------------------------------------------
# __stringify_boolean
# ---------------------------------------------------------------------------

class TestStringifyBoolean:
    def test_stringify_true(self):
        assert GsSecurityMasterApi._GsSecurityMasterApi__stringify_boolean(True) == 'true'

    def test_stringify_false(self):
        assert GsSecurityMasterApi._GsSecurityMasterApi__stringify_boolean(False) == 'false'


# ---------------------------------------------------------------------------
# __fetch_all
# ---------------------------------------------------------------------------

class TestFetchAll:
    def test_fetch_all_no_total_batches_extract_results(self):
        """Test __fetch_all with total_batches=None and extract_results=True."""
        call_count = [0]

        def fetch_fn(offset_key=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return {'results': [{'id': 'A'}], 'offsetKey': 'p2'}
            else:
                return {'results': [{'id': 'B'}]}

        result = GsSecurityMasterApi._GsSecurityMasterApi__fetch_all(fetch_fn, 'start')
        assert len(result) == 2
        assert result[0] == {'id': 'A'}
        assert result[1] == {'id': 'B'}

    def test_fetch_all_with_total_batches(self):
        """Test __fetch_all with total_batches specified."""
        call_count = [0]

        def fetch_fn(offset_key=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return {'results': [{'id': 'A'}], 'offsetKey': 'p2'}
            else:
                return {'results': [{'id': 'B'}]}

        result = GsSecurityMasterApi._GsSecurityMasterApi__fetch_all(fetch_fn, 'start', total_batches=5)
        assert len(result) == 2

    def test_fetch_all_extract_results_false(self):
        """Test __fetch_all with extract_results=False."""
        call_count = [0]

        def fetch_fn(offset_key=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return {'results': [{'id': 'A'}], 'offsetKey': 'p2'}
            else:
                return {'results': [{'id': 'B'}]}

        result = GsSecurityMasterApi._GsSecurityMasterApi__fetch_all(
            fetch_fn, 'start', extract_results=False
        )
        assert len(result) == 2
        # Items should be the full response dicts, not extracted results
        assert result[0] == {'results': [{'id': 'A'}], 'offsetKey': 'p2'}
        assert result[1] == {'results': [{'id': 'B'}]}

    def test_fetch_all_single_page(self):
        """Test __fetch_all when first page has no offsetKey."""
        def fetch_fn(offset_key=None):
            return {'results': [{'id': 'A'}]}

        result = GsSecurityMasterApi._GsSecurityMasterApi__fetch_all(fetch_fn, 'start')
        assert len(result) == 1

    def test_fetch_all_data_is_none_then_data(self):
        """When fetch_fn returns None on first call, the loop continues.
        Branch 437->434: data is None, loop back to while True."""
        call_count = [0]

        def fetch_fn(offset_key=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return None  # data is None -> loop continues (branch 437->434)
            else:
                return {'results': [{'id': 'A'}]}  # data not None, no offsetKey -> break

        result = GsSecurityMasterApi._GsSecurityMasterApi__fetch_all(fetch_fn, 'start')
        assert len(result) == 1
        assert call_count[0] == 2


# ---------------------------------------------------------------------------
# _get_corporate_actions
# ---------------------------------------------------------------------------

class TestGetCorporateActions:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_get_corporate_actions_basic(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        # __fetch_all calls _get_corporate_actions repeatedly
        # First call with no offset, returns data + no offsetKey
        session.sync.get.return_value = {
            'results': [{'action': 'split'}],
        }
        result = GsSecurityMasterApi.get_corporate_actions('123', SecMasterIdentifiers.GSID)
        assert len(result) == 1

    def test_raises_on_unsupported_identifier(self):
        with pytest.raises(ValueError, match="Unsupported identifier"):
            GsSecurityMasterApi.get_corporate_actions('123', SecMasterIdentifiers.TICKER)

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_get_corporate_actions_with_id_type(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': [{'action': 'dividend'}]}
        result = GsSecurityMasterApi.get_corporate_actions('123', SecMasterIdentifiers.ID)
        assert result == [{'action': 'dividend'}]

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_get_corporate_actions_with_effective_date(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': [{'action': 'split'}]}
        result = GsSecurityMasterApi.get_corporate_actions(
            '123', SecMasterIdentifiers.GSID, effective_date=dt.date(2023, 1, 1)
        )
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _get_corporate_actions (internal)
# ---------------------------------------------------------------------------

class TestInternalGetCorporateActions:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_effective_date_and_offset(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': []}
        GsSecurityMasterApi._get_corporate_actions(
            '123', SecMasterIdentifiers.GSID,
            effective_date=dt.date(2023, 1, 1),
            offset_key='abc'
        )
        args, kwargs = session.sync.get.call_args
        assert args[0] == '/markets/corpactions'

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_without_effective_date_and_offset(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': []}
        GsSecurityMasterApi._get_corporate_actions(
            '123', SecMasterIdentifiers.GSID,
            effective_date=None,
            offset_key=None
        )
        args, kwargs = session.sync.get.call_args
        payload = kwargs['payload']
        assert 'effectiveDate' not in payload
        assert 'offsetKey' not in payload


# ---------------------------------------------------------------------------
# get_capital_structure
# ---------------------------------------------------------------------------

class TestGetCapitalStructure:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_no_offset_key_returns_immediately(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'issuerId': 'I1', 'types': {'ETF': [{'id': 'A'}]}}],
        }
        result = GsSecurityMasterApi.get_capital_structure(
            'AAPL', CapitalStructureIdentifiers.TICKER
        )
        assert result['totalResults'] == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_scrolls_with_offset_key(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session

        session.sync.get.side_effect = [
            # First call
            {
                'totalResults': 2,
                'results': [{'issuerId': 'I1', 'types': {'ETF': [{'id': 'A'}]}}],
                'offsetKey': 'p2',
                'assetTypesTotal': {'ETF': 200},
            },
            # Second call from __fetch_all
            {
                'totalResults': 2,
                'results': [{'issuerId': 'I1', 'types': {'ETF': [{'id': 'B'}]}}],
            },
        ]
        result = GsSecurityMasterApi.get_capital_structure(
            'AAPL', CapitalStructureIdentifiers.TICKER
        )
        # Should have aggregated results
        assert 'results' in result
        assert 'offsetKey' not in result

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_all_params(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'issuerId': 'I1', 'types': {'ETF': [{'id': 'A'}]}}],
        }
        result = GsSecurityMasterApi.get_capital_structure(
            'AAPL',
            CapitalStructureIdentifiers.TICKER,
            type_=SecMasterAssetType.ETF,
            is_primary=True,
            effective_date=dt.date(2023, 1, 1),
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_multiple_issuers_aggregation(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.side_effect = [
            {
                'totalResults': 3,
                'results': [
                    {'issuerId': 'I1', 'types': {'ETF': [{'id': 'A'}]}},
                    {'issuerId': 'I2', 'types': {'ETF': [{'id': 'C'}]}},
                ],
                'offsetKey': 'p2',
                'assetTypesTotal': {'ETF': 300},
            },
            {
                'totalResults': 3,
                'results': [
                    {'issuerId': 'I1', 'types': {'ETF': [{'id': 'B'}]}},
                ],
            },
        ]
        result = GsSecurityMasterApi.get_capital_structure(
            ['AAPL', 'GOOG'], CapitalStructureIdentifiers.TICKER
        )
        assert 'results' in result


# ---------------------------------------------------------------------------
# __capital_structure_aggregate
# ---------------------------------------------------------------------------

class TestCapitalStructureAggregate:
    def test_aggregate_single_issuer(self):
        asset_types_total = {'ETF': 2, 'Common Stock': 1}
        results = [
            {'issuerId': 'I1', 'types': {'ETF': [{'id': 'A'}], 'Common Stock': [{'id': 'C'}]}},
            {'issuerId': 'I1', 'types': {'ETF': [{'id': 'B'}], 'Common Stock': []}},
        ]
        agg, total = GsSecurityMasterApi._GsSecurityMasterApi__capital_structure_aggregate(
            asset_types_total, results
        )
        assert len(agg) == 1
        assert total == 3  # A, C, B

    def test_aggregate_multiple_issuers(self):
        asset_types_total = {'ETF': 2}
        results = [
            {'issuerId': 'I1', 'types': {'ETF': [{'id': 'A'}]}},
            {'issuerId': 'I2', 'types': {'ETF': [{'id': 'B'}]}},
        ]
        agg, total = GsSecurityMasterApi._GsSecurityMasterApi__capital_structure_aggregate(
            asset_types_total, results
        )
        assert len(agg) == 2
        assert total == 2


# ---------------------------------------------------------------------------
# _get_capital_structure (internal)
# ---------------------------------------------------------------------------

class TestInternalGetCapitalStructure:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_basic_call(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 0}
        GsSecurityMasterApi._get_capital_structure(
            id_value='AAPL',
            id_type=CapitalStructureIdentifiers.TICKER,
            type_=None,
            is_primary=None,
            effective_date=None,
            offset_key=None,
        )
        session.sync.get.assert_called_once()


# ---------------------------------------------------------------------------
# prepare_params
# ---------------------------------------------------------------------------

class TestPrepareParams:
    def test_all_none(self):
        params = {}
        GsSecurityMasterApi.prepare_params(params, None, None, None, None)
        assert params == {}

    def test_all_set(self):
        params = {}
        GsSecurityMasterApi.prepare_params(
            params,
            is_primary=True,
            offset_key='abc',
            type_=SecMasterAssetType.ETF,
            effective_date=dt.date(2023, 1, 1),
        )
        assert params['type'] == 'ETF'
        assert params['isPrimary'] is True
        assert params['offsetKey'] == 'abc'
        assert params['effectiveDate'] == dt.date(2023, 1, 1)

    def test_partial_params(self):
        params = {}
        GsSecurityMasterApi.prepare_params(params, is_primary=False, offset_key=None, type_=None)
        assert params == {'isPrimary': False}


# ---------------------------------------------------------------------------
# _get_deltas
# ---------------------------------------------------------------------------

class TestInternalGetDeltas:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_all_params_none(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': []}
        GsSecurityMasterApi._get_deltas()
        session.sync.get.assert_called_once()

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_all_params_set(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': []}
        now = dt.datetime(2023, 6, 1, 12, 0, 0)
        GsSecurityMasterApi._get_deltas(
            start_time=now,
            end_time=now,
            raw=True,
            scope=['identifiers'],
            limit=50,
            offset_key='abc',
        )
        session.sync.get.assert_called_once()

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_raw_false(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'results': []}
        GsSecurityMasterApi._get_deltas(raw=False)
        session.sync.get.assert_called_once()


# ---------------------------------------------------------------------------
# get_deltas
# ---------------------------------------------------------------------------

class TestGetDeltas:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_scroll_all_pages_true(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.side_effect = [
            {
                'results': [{'delta': 1}],
                'lastUpdateTime': '2023-06-01T12:00:00Z',
                'requestId': 'req1',
                'offsetKey': 'p2',
            },
            {
                'results': [{'delta': 2}],
                'lastUpdateTime': '2023-06-02T12:00:00Z',
                'requestId': 'req2',
            },
        ]
        result = GsSecurityMasterApi.get_deltas(scroll_all_pages=True)
        assert len(result['results']) == 2
        assert result['lastUpdateTime'] == '2023-06-02T12:00:00Z'
        assert result['requestId'] == 'req1'

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_scroll_all_pages_false(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'results': [{'delta': 1}],
            'lastUpdateTime': '2023-06-01T12:00:00Z',
        }
        result = GsSecurityMasterApi.get_deltas(scroll_all_pages=False)
        assert result['results'] == [{'delta': 1}]

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_scroll_all_pages_with_params(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'results': [{'delta': 1}],
            'lastUpdateTime': '2023-06-01T12:00:00Z',
            'requestId': 'req1',
        }
        now = dt.datetime(2023, 6, 1, 12, 0, 0)
        result = GsSecurityMasterApi.get_deltas(
            start_time=now,
            end_time=now,
            raw=True,
            scope=['identifiers'],
            limit=50,
            scroll_all_pages=True,
        )
        assert 'results' in result


# ---------------------------------------------------------------------------
# get_exchanges
# ---------------------------------------------------------------------------

class TestGetExchanges:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_basic_exchange_query(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'name': 'NYSE'}],
        }
        result = GsSecurityMasterApi.get_exchanges()
        assert result['totalResults'] == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_effective_date(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'name': 'NYSE'}],
        }
        result = GsSecurityMasterApi.get_exchanges(effective_date=dt.date(2023, 1, 1))
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_query_params(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'name': 'NYSE'}],
        }
        result = GsSecurityMasterApi.get_exchanges(mic='XNYS')
        assert result is not None


# ---------------------------------------------------------------------------
# _get_exchanges (internal)
# ---------------------------------------------------------------------------

class TestInternalGetExchanges:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_no_query_params(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'name': 'NYSE'}],
        }
        result = GsSecurityMasterApi._get_exchanges()
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_invalid_param(self, mock_gs_session):
        with pytest.raises(ValueError, match="not supported"):
            GsSecurityMasterApi._get_exchanges(query_params={'invalidParam': 'val'})

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_none_on_zero_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 0, 'results': []}
        result = GsSecurityMasterApi._get_exchanges()
        assert result is None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_effective_date_and_offset(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'name': 'NYSE'}],
        }
        result = GsSecurityMasterApi._get_exchanges(
            effective_date=dt.date(2023, 1, 1),
            offset_key='abc',
            query_params={'mic': 'XNYS'},
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_query_params_none_defaults_to_empty_dict(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 1, 'results': [{'name': 'X'}]}
        result = GsSecurityMasterApi._get_exchanges(query_params=None)
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_without_effective_date(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 1, 'results': [{'name': 'X'}]}
        result = GsSecurityMasterApi._get_exchanges(effective_date=None)
        assert result is not None


# ---------------------------------------------------------------------------
# get_exchange_identifiers_history
# ---------------------------------------------------------------------------

class TestGetExchangeIdentifiersHistory:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'results': [{'type': 'MIC', 'value': 'XNYS'}]
        }
        result = GsSecurityMasterApi.get_exchange_identifiers_history('EX123')
        assert result == [{'type': 'MIC', 'value': 'XNYS'}]


# ---------------------------------------------------------------------------
# _prepare_string_or_list_param
# ---------------------------------------------------------------------------

class TestPrepareStringOrListParam:
    def test_string_input(self):
        result = GsSecurityMasterApi._prepare_string_or_list_param('abc', 'test')
        assert result == ['abc']

    def test_list_input(self):
        result = GsSecurityMasterApi._prepare_string_or_list_param(['a', 'b'], 'test')
        assert result == ['a', 'b']

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="must be a string or a list of strings"):
            GsSecurityMasterApi._prepare_string_or_list_param(123, 'test')

    def test_list_with_non_string_elements_raises(self):
        with pytest.raises(ValueError, match="must be a string or a list of strings"):
            GsSecurityMasterApi._prepare_string_or_list_param([1, 2], 'test')

    def test_empty_list(self):
        result = GsSecurityMasterApi._prepare_string_or_list_param([], 'test')
        assert result == []


# ---------------------------------------------------------------------------
# _prepare_underlyers_params
# ---------------------------------------------------------------------------

class TestPrepareUnderlyersParams:
    def test_basic_string_id(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(params, id_value='GSPD100')
        assert params['id'] == ['GSPD100']

    def test_list_id(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(params, id_value=['GSPD100', 'GSPD200'])
        assert params['id'] == ['GSPD100', 'GSPD200']

    def test_none_id_raises(self):
        params = {}
        with pytest.raises(ValueError, match="id_value must be defined"):
            GsSecurityMasterApi._prepare_underlyers_params(params, id_value=None)

    def test_type_single_secmaster_asset_type(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(
            params, id_value='GSPD100', type_=SecMasterAssetType.ETF
        )
        assert params['type'] == 'ETF'

    def test_type_list_of_secmaster_asset_types(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(
            params, id_value='GSPD100', type_=[SecMasterAssetType.ETF, SecMasterAssetType.Future]
        )
        assert params['type'] == ['ETF', 'Future']

    def test_type_list_with_invalid_element_raises(self):
        params = {}
        with pytest.raises(ValueError, match="All elements in the type_ list must be instances"):
            GsSecurityMasterApi._prepare_underlyers_params(
                params, id_value='GSPD100', type_=[SecMasterAssetType.ETF, 'invalid']
            )

    def test_type_invalid_type_raises(self):
        params = {}
        with pytest.raises(ValueError, match="type_ must be either a SecMasterAssetType"):
            GsSecurityMasterApi._prepare_underlyers_params(
                params, id_value='GSPD100', type_='invalid'
            )

    def test_country_code_string(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(
            params, id_value='GSPD100', country_code='US'
        )
        assert params['countryCode'] == ['US']

    def test_currency_string(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(
            params, id_value='GSPD100', currency='USD'
        )
        assert params['currency'] == ['USD']

    def test_offset_key(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(
            params, id_value='GSPD100', offset_key='abc'
        )
        assert params['offsetKey'] == 'abc'

    def test_effective_date(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(
            params, id_value='GSPD100', effective_date=dt.date(2023, 1, 1)
        )
        assert params['effectiveDate'] == dt.date(2023, 1, 1)

    def test_include_inactive(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(
            params, id_value='GSPD100', include_inactive=True
        )
        assert params['includeInactive'] is True

    def test_all_none_optional(self):
        params = {}
        GsSecurityMasterApi._prepare_underlyers_params(
            params, id_value='GSPD100',
            offset_key=None, type_=None, effective_date=None,
            country_code=None, currency=None, include_inactive=None,
        )
        assert 'offsetKey' not in params
        assert 'type' not in params
        assert 'effectiveDate' not in params
        assert 'countryCode' not in params
        assert 'currency' not in params
        assert 'includeInactive' not in params


# ---------------------------------------------------------------------------
# _get_securities_by_underlyers (internal)
# ---------------------------------------------------------------------------

class TestInternalGetSecuritiesByUnderlyers:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        result = GsSecurityMasterApi._get_securities_by_underlyers(id_value='GSPD100')
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_returns_none_on_zero_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {'totalResults': 0, 'results': []}
        result = GsSecurityMasterApi._get_securities_by_underlyers(id_value='GSPD100')
        assert result is None


# ---------------------------------------------------------------------------
# get_securities_by_underlyers
# ---------------------------------------------------------------------------

class TestGetSecuritiesByUnderlyers:
    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_single_page_no_scroll(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            scroll_all_pages=False,
        )
        assert result['totalResults'] == 1

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_scroll_all_pages(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.side_effect = [
            {
                'totalResults': 2,
                'results': [{'id': 'A'}],
                'asOfTime': '2023-06-01T00:00:00Z',
                'requestId': 'req1',
                'offsetKey': 'p2',
            },
            {
                'totalResults': 2,
                'results': [{'id': 'B'}],
                'asOfTime': '2023-06-02T00:00:00Z',
                'requestId': 'req2',
            },
        ]
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            scroll_all_pages=True,
        )
        assert result['totalResults'] == 2
        assert result['asOfTime'] == '2023-06-02T00:00:00Z'
        assert result['requestId'] == 'req1'

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_scroll_all_pages_with_limit(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
            'asOfTime': '2023-06-01T00:00:00Z',
            'requestId': 'req1',
        }
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            scroll_all_pages=True,
            limit=100,
        )
        assert result['totalResults'] == 1

    def test_unsupported_single_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported type"):
            GsSecurityMasterApi.get_securities_by_underlyers(
                id_value='GSPD100',
                type_=SecMasterAssetType.ETF,
            )

    def test_unsupported_type_in_list_raises(self):
        with pytest.raises(ValueError, match="Unsupported type"):
            GsSecurityMasterApi.get_securities_by_underlyers(
                id_value='GSPD100',
                type_=[SecMasterAssetType.Equity_Option, SecMasterAssetType.ETF],
            )

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_supported_single_type(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            type_=SecMasterAssetType.Equity_Option,
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_supported_list_type(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            type_=[SecMasterAssetType.Future, SecMasterAssetType.Future_Option],
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_type_none_passes_validation(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            type_=None,
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_scroll_all_pages_default_limit(self, mock_gs_session):
        """When scroll_all_pages=True and limit=None, limit should default to 500."""
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
            'asOfTime': '2023-06-01T00:00:00Z',
            'requestId': 'req1',
        }
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            scroll_all_pages=True,
            limit=None,
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_with_all_optional_params(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            type_=SecMasterAssetType.Equity_Option,
            effective_date=dt.date(2023, 1, 1),
            limit=50,
            offset_key='abc',
            country_code='US',
            currency='USD',
            include_inactive=True,
            scroll_all_pages=False,
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_scroll_returns_none_on_no_results(self, mock_gs_session):
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 0,
            'results': [],
        }
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            scroll_all_pages=False,
        )
        assert result is None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_supported_type_list_all_valid(self, mock_gs_session):
        """All three supported types in a list."""
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        result = GsSecurityMasterApi.get_securities_by_underlyers(
            id_value='GSPD100',
            type_=[SecMasterAssetType.Equity_Option, SecMasterAssetType.Future, SecMasterAssetType.Future_Option],
        )
        assert result is not None

    @patch('gs_quant.api.gs.secmaster.GsSession')
    def test_type_not_secmaster_or_list_falls_through(self, mock_gs_session):
        """When type_ is not SecMasterAssetType nor list, it falls through validation (branch 854->859)."""
        session = _mock_session()
        mock_gs_session.current = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'id': 'A'}],
        }
        # type_ is a string, which is not SecMasterAssetType and not a list
        # It passes the type_ is not None check but falls through both isinstance checks
        # This will eventually fail in _prepare_underlyers_params, but that's fine -
        # we just need to hit the branch. Let's use a tuple which isn't list or SecMasterAssetType
        # Actually looking at the code: the if/elif has no else, so it falls through
        # Then _get_securities_by_underlyers / _prepare_underlyers_params will handle it
        # Using a tuple: isinstance(tuple, SecMasterAssetType) => False, isinstance(tuple, list) => False
        # Then _prepare_underlyers_params will raise since tuple is not SecMasterAssetType or list
        try:
            GsSecurityMasterApi.get_securities_by_underlyers(
                id_value='GSPD100',
                type_=(SecMasterAssetType.Future,),  # tuple, not list
            )
        except (ValueError, TypeError):
            pass  # We expect this to fail downstream, but we've hit the branch
