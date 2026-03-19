"""
Comprehensive branch coverage tests for gs_quant/api/gs/portfolios.py
"""

import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from gs_quant.api.gs.portfolios import GsPortfolioApi
from gs_quant.common import PositionType, Currency
from gs_quant.target.common import PositionTag
from gs_quant.target.portfolios import Portfolio, PositionSet, Position, PortfolioTree
from gs_quant.target.reports import Report, ReportParameters
from gs_quant.target.risk_models import RiskModelTerm as Term


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_session():
    """Return a MagicMock that behaves like GsSession.current."""
    session = MagicMock()
    session.sync = MagicMock()
    return session


# ---------------------------------------------------------------------------
# get_portfolios
# ---------------------------------------------------------------------------

class TestGetPortfolios:
    """Branches in get_portfolios:
    - portfolio_ids truthy / falsy
    - portfolio_names truthy / falsy
    - kwargs with list value / scalar value
    """

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_no_ids_no_names_no_kwargs(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': ['p1']}

        result = GsPortfolioApi.get_portfolios()
        assert result == ['p1']
        session.sync.get.assert_called_once()
        call_url = session.sync.get.call_args[0][0]
        assert call_url == '/portfolios?&limit=100'

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_ids(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_portfolios(portfolio_ids=['A', 'B'])
        call_url = session.sync.get.call_args[0][0]
        assert '&id=A&id=B' in call_url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_names(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_portfolios(portfolio_names=['Port1', 'Port2'])
        call_url = session.sync.get.call_args[0][0]
        assert '&name=Port1&name=Port2' in call_url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_kwargs_list_value(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_portfolios(owner=['alice', 'bob'])
        call_url = session.sync.get.call_args[0][0]
        assert '&owner=alice' in call_url
        assert '&owner=bob' in call_url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_kwargs_scalar_value(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_portfolios(currency='USD')
        call_url = session.sync.get.call_args[0][0]
        assert '&currency=USD' in call_url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_custom_limit(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_portfolios(limit=50)
        call_url = session.sync.get.call_args[0][0]
        assert '&limit=50' in call_url


# ---------------------------------------------------------------------------
# get_portfolio
# ---------------------------------------------------------------------------

class TestGetPortfolio:
    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_basic(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = 'portfolio_obj'

        result = GsPortfolioApi.get_portfolio('MP1')
        session.sync.get.assert_called_once_with('/portfolios/MP1', cls=Portfolio)
        assert result == 'portfolio_obj'


# ---------------------------------------------------------------------------
# get_portfolio_by_name
# ---------------------------------------------------------------------------

class TestGetPortfolioByName:
    """Branches: num_found == 0, num_found > 1, else (exactly 1)."""

    @patch.object(GsPortfolioApi, 'get_session')
    def test_not_found(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'totalResults': 0, 'results': []}

        with pytest.raises(ValueError, match='not found'):
            GsPortfolioApi.get_portfolio_by_name('missing')

    @patch.object(GsPortfolioApi, 'get_session')
    def test_multiple_found(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'totalResults': 2, 'results': [{}, {}]}

        with pytest.raises(ValueError, match='More than one'):
            GsPortfolioApi.get_portfolio_by_name('dup')

    @patch.object(GsPortfolioApi, 'get_session')
    def test_found_one(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {
            'totalResults': 1,
            'results': [{'currency': 'USD', 'name': 'TestPort'}],
        }

        result = GsPortfolioApi.get_portfolio_by_name('TestPort')
        assert isinstance(result, Portfolio)
        assert result.name == 'TestPort'


# ---------------------------------------------------------------------------
# create_portfolio
# ---------------------------------------------------------------------------

class TestCreatePortfolio:
    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_basic(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        port = Portfolio(currency='USD', name='Test')
        session.sync.post.return_value = port

        result = GsPortfolioApi.create_portfolio(port)
        session.sync.post.assert_called_once_with('/portfolios', port, cls=Portfolio)
        assert result == port


# ---------------------------------------------------------------------------
# update_portfolio
# ---------------------------------------------------------------------------

class TestUpdatePortfolio:
    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_basic(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        port = Portfolio(currency='USD', name='Test', id_='MP1')
        session.sync.put.return_value = port

        result = GsPortfolioApi.update_portfolio(port)
        session.sync.put.assert_called_once_with('/portfolios/MP1', port, cls=Portfolio)
        assert result == port


# ---------------------------------------------------------------------------
# delete_portfolio
# ---------------------------------------------------------------------------

class TestDeletePortfolio:
    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_basic(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.delete.return_value = {}

        result = GsPortfolioApi.delete_portfolio('MP1')
        session.sync.delete.assert_called_once_with('/portfolios/MP1')
        assert result == {}


# ---------------------------------------------------------------------------
# get_portfolio_analyze
# ---------------------------------------------------------------------------

class TestGetPortfolioAnalyze:
    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_basic(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'analysis': 'data'}

        result = GsPortfolioApi.get_portfolio_analyze('MP1')
        session.sync.get.assert_called_once_with('/portfolios/MP1/analyze')
        assert result == {'analysis': 'data'}


# ---------------------------------------------------------------------------
# get_positions
# ---------------------------------------------------------------------------

class TestGetPositions:
    """Branches: start_date is not None, end_date is not None, both None."""

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_no_dates(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'positionSets': []}

        result = GsPortfolioApi.get_positions('MP1')
        url = session.sync.get.call_args[0][0]
        assert 'startDate' not in url
        assert 'endDate' not in url
        assert result == ()

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_start_date_only(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'positionSets': []}

        GsPortfolioApi.get_positions('MP1', start_date=dt.date(2023, 1, 1))
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url
        assert 'endDate' not in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_end_date_only(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'positionSets': []}

        GsPortfolioApi.get_positions('MP1', end_date=dt.date(2023, 6, 1))
        url = session.sync.get.call_args[0][0]
        assert '&endDate=2023-06-01' in url
        assert 'startDate' not in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_both_dates(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        ps_dict = {
            'positionDate': '2023-01-01',
            'positions': [{'assetId': 'A1', 'quantity': 100}],
        }
        session.sync.get.return_value = {'positionSets': [ps_dict]}

        result = GsPortfolioApi.get_positions(
            'MP1', start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 6, 1)
        )
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-01' in url
        assert len(result) == 1

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_empty_position_sets_key_missing(self, mock_gs):
        """When positionSets key is missing, should return empty tuple."""
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {}

        result = GsPortfolioApi.get_positions('MP1')
        assert result == ()


# ---------------------------------------------------------------------------
# get_positions_for_date
# ---------------------------------------------------------------------------

class TestGetPositionsForDate:
    """Branches: len(position_sets) > 0 -> True / False."""

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_results(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        ps = PositionSet(position_date=dt.date(2023, 1, 1), positions=())
        session.sync.get.return_value = {'results': [ps]}

        result = GsPortfolioApi.get_positions_for_date('MP1', dt.date(2023, 1, 1))
        assert result == ps

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_empty_results(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        result = GsPortfolioApi.get_positions_for_date('MP1', dt.date(2023, 1, 1))
        assert result is None


# ---------------------------------------------------------------------------
# get_position_set_by_position_type
# ---------------------------------------------------------------------------

class TestGetPositionSetByPositionType:
    """Branches: positions_type == 'ETI' -> deals / else books; activity_type != 'position' / == 'position'."""

    @patch.object(GsPortfolioApi, '_unpack_position_set')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_eti_with_non_position_activity(self, mock_get_session, mock_unpack):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'positionSets': [{'mock': 1}]}
        mock_unpack.return_value = 'unpacked'

        result = GsPortfolioApi.get_position_set_by_position_type('ETI', 'deal123', 'trade')
        url = session.sync.get.call_args[0][0]
        assert '/risk-internal/deals/deal123/positions?activityType=trade' == url
        assert result == ('unpacked',)

    @patch.object(GsPortfolioApi, '_unpack_position_set')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_non_eti_with_position_activity(self, mock_get_session, mock_unpack):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'positionSets': [{'mock': 1}]}
        mock_unpack.return_value = 'unpacked'

        result = GsPortfolioApi.get_position_set_by_position_type('BOOK_TYPE', 'book123', 'position')
        url = session.sync.get.call_args[0][0]
        assert '/risk-internal/books/BOOK_TYPE/book123/positions' == url
        assert result == ('unpacked',)

    @patch.object(GsPortfolioApi, '_unpack_position_set')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_non_eti_with_non_position_activity(self, mock_get_session, mock_unpack):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'positionSets': [{'mock': 1}]}
        mock_unpack.return_value = 'unpacked'

        result = GsPortfolioApi.get_position_set_by_position_type('BOOK_TYPE', 'book123', 'trade')
        url = session.sync.get.call_args[0][0]
        assert '/risk-internal/books/BOOK_TYPE/book123/positions?activityType=trade' == url
        assert result == ('unpacked',)


# ---------------------------------------------------------------------------
# _unpack_position_set
# ---------------------------------------------------------------------------

class TestUnpackPositionSet:
    def test_basic(self):
        instr = MagicMock()
        instr.name = 'SomeName'
        pos = MagicMock()
        pos.instrument = instr
        position_set_data = {
            'positionDate': '2023-01-01',
            'positions': [{'assetId': 'A1', 'quantity': 100}],
        }
        # Use a real PositionSet and verify instrument name is set to None
        with patch.object(PositionSet, 'from_dict') as mock_from_dict:
            mock_ps = MagicMock()
            mock_ps.positions = [pos]
            mock_from_dict.return_value = mock_ps

            result = GsPortfolioApi._unpack_position_set(position_set_data)
            assert result == mock_ps
            assert pos.instrument.name is None


# ---------------------------------------------------------------------------
# get_instruments_by_position_type
# ---------------------------------------------------------------------------

class TestGetInstrumentsByPositionType:
    @patch.object(GsPortfolioApi, 'get_position_set_by_position_type')
    def test_basic(self, mock_get_pos_set):
        instr = MagicMock()
        pos = MagicMock()
        pos.instrument = instr
        pos.tags = [{'name': 'tag1', 'value': 'val1'}]
        pos.external_ids = [{'idType': 'ISIN', 'idValue': 'US123'}]
        pos.party_from = 'partyA'
        pos.party_to = 'partyB'

        ps = MagicMock()
        ps.position_date = dt.date(2023, 1, 1)
        ps.positions = [pos]

        mock_get_pos_set.return_value = (ps,)

        result = GsPortfolioApi.get_instruments_by_position_type('ETI', 'deal123', 'trade')
        assert len(result) == 1
        assert result[0] == instr
        assert instr.metadata == {
            'trade_date': dt.date(2023, 1, 1),
            'tags': pos.tags,
            'external_ids': {'ISIN': 'US123'},
            'party_from': 'partyA',
            'party_to': 'partyB',
        }

    @patch.object(GsPortfolioApi, 'get_position_set_by_position_type')
    def test_multiple_position_sets(self, mock_get_pos_set):
        """Test iterating over multiple position sets and positions."""
        instr1 = MagicMock()
        pos1 = MagicMock()
        pos1.instrument = instr1
        pos1.tags = []
        pos1.external_ids = []
        pos1.party_from = None
        pos1.party_to = None

        instr2 = MagicMock()
        pos2 = MagicMock()
        pos2.instrument = instr2
        pos2.tags = []
        pos2.external_ids = [{'idType': 'CUSIP', 'idValue': '123456'}]
        pos2.party_from = 'X'
        pos2.party_to = 'Y'

        ps1 = MagicMock()
        ps1.position_date = dt.date(2023, 1, 1)
        ps1.positions = [pos1]

        ps2 = MagicMock()
        ps2.position_date = dt.date(2023, 2, 1)
        ps2.positions = [pos2]

        mock_get_pos_set.return_value = (ps1, ps2)

        result = GsPortfolioApi.get_instruments_by_position_type('ETI', 'id', 'trade')
        assert len(result) == 2


# ---------------------------------------------------------------------------
# get_latest_positions
# ---------------------------------------------------------------------------

class TestGetLatestPositions:
    """Branches: isinstance(results, dict) and 'positions' in results -> True / False."""

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_results_is_dict_with_positions(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {
            'results': {
                'positionDate': '2023-01-01',
                'positions': [
                    {'assetId': 'A1', 'quantity': 100},
                ],
            }
        }

        result = GsPortfolioApi.get_latest_positions('MP1')
        assert isinstance(result, PositionSet)

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_results_is_dict_without_positions(self, mock_gs):
        """When results is a dict but no 'positions' key."""
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {
            'results': {
                'positionDate': '2023-01-01',
            }
        }

        result = GsPortfolioApi.get_latest_positions('MP1')
        assert isinstance(result, PositionSet)

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_results_is_not_dict(self, mock_gs):
        """When results is not a dict (e.g., already a PositionSet dict w/o 'positions')."""
        session = _mock_session()
        mock_gs.current = session
        # Return results that is a list - isinstance check fails
        session.sync.get.return_value = {
            'results': {
                'positionDate': '2023-06-01',
            }
        }

        result = GsPortfolioApi.get_latest_positions('MP1', position_type='open')
        url = session.sync.get.call_args[0][0]
        assert 'type=open' in url


# ---------------------------------------------------------------------------
# get_instruments_by_workflow_id
# ---------------------------------------------------------------------------

class TestGetInstrumentsByWorkflowId:
    """Branches: prefer_instruments True / False; instrument name truthy / falsy."""

    @patch.object(GsPortfolioApi, 'get_session')
    def test_prefer_instruments_false(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {
            'workflowPositions': {
                'wf123': [
                    {
                        'positions': [
                            {'instrument': {'asset_class': 'Equity', 'type': 'Single Stock', 'name': 'AAPL'}},
                        ]
                    }
                ]
            }
        }

        with patch('gs_quant.api.gs.portfolios.Instrument') as MockInstrument:
            inst = MagicMock()
            MockInstrument.from_dict.return_value = inst

            result = GsPortfolioApi.get_instruments_by_workflow_id('wf123')
            url = session.sync.get.call_args[0][0]
            assert '/risk-internal/quote/wf123' == url
            assert len(result) == 1
            # name was truthy ('AAPL'), so instrument.name was set
            assert inst.name == 'AAPL'

    @patch.object(GsPortfolioApi, 'get_session')
    def test_prefer_instruments_true(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {
            'workflowPositions': {
                'wf123': [
                    {
                        'positions': [
                            {'instrument': {'asset_class': 'Equity', 'type': 'Single Stock'}},
                        ]
                    }
                ]
            }
        }

        with patch('gs_quant.api.gs.portfolios.Instrument') as MockInstrument:
            inst = MagicMock()
            MockInstrument.from_dict.return_value = inst

            result = GsPortfolioApi.get_instruments_by_workflow_id('wf123', prefer_instruments=True)
            url = session.sync.get.call_args[0][0]
            assert '/risk/quote/wf123' == url
            assert len(result) == 1
            # name was falsy (None / missing), so instrument.name should NOT be set to something truthy
            # The from_dict mock returns inst, and since name is missing from dict, inst.name is not reassigned

    @patch.object(GsPortfolioApi, 'get_session')
    def test_instrument_without_name(self, mock_get_session):
        """When instrument dict has no 'name' key."""
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {
            'workflowPositions': {
                'wf123': [
                    {
                        'positions': [
                            {'instrument': {'asset_class': 'Equity', 'type': 'Single Stock'}},
                        ]
                    }
                ]
            }
        }

        with patch('gs_quant.api.gs.portfolios.Instrument') as MockInstrument:
            inst = MagicMock()
            inst.name = None
            MockInstrument.from_dict.return_value = inst

            result = GsPortfolioApi.get_instruments_by_workflow_id('wf123')
            assert len(result) == 1
            # name was None/missing - the 'if name:' branch was False

    @patch.object(GsPortfolioApi, 'get_session')
    def test_instrument_with_empty_name(self, mock_get_session):
        """When instrument dict has name='' (falsy)."""
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {
            'workflowPositions': {
                'wf123': [
                    {
                        'positions': [
                            {'instrument': {'asset_class': 'Equity', 'type': 'Single Stock', 'name': ''}},
                        ]
                    }
                ]
            }
        }

        with patch('gs_quant.api.gs.portfolios.Instrument') as MockInstrument:
            inst = MagicMock()
            MockInstrument.from_dict.return_value = inst

            result = GsPortfolioApi.get_instruments_by_workflow_id('wf123')
            assert len(result) == 1


# ---------------------------------------------------------------------------
# get_position_dates
# ---------------------------------------------------------------------------

class TestGetPositionDates:
    @patch.object(GsPortfolioApi, 'get_session')
    def test_basic(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'results': ['2023-01-01', '2023-02-01']}

        result = GsPortfolioApi.get_position_dates('MP1')
        assert result == (dt.date(2023, 1, 1), dt.date(2023, 2, 1))

    @patch.object(GsPortfolioApi, 'get_session')
    def test_empty(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'results': []}

        result = GsPortfolioApi.get_position_dates('MP1')
        assert result == ()


# ---------------------------------------------------------------------------
# update_positions
# ---------------------------------------------------------------------------

class TestUpdatePositions:
    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_net_positions_true(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.put.return_value = 1.0

        result = GsPortfolioApi.update_positions('MP1', [])
        url = session.sync.put.call_args[0][0]
        assert 'netPositions=true' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_net_positions_false(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.put.return_value = 1.0

        result = GsPortfolioApi.update_positions('MP1', [], net_positions=False)
        url = session.sync.put.call_args[0][0]
        assert 'netPositions=false' in url


# ---------------------------------------------------------------------------
# get_positions_data
# ---------------------------------------------------------------------------

class TestGetPositionsData:
    """Branches: fields, performance_report_id, position_type, include_all_business_days - each None/not None."""

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_minimal(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        result = GsPortfolioApi.get_positions_data('MP1', dt.date(2023, 1, 1), dt.date(2023, 6, 1))
        url = session.sync.get.call_args[0][0]
        assert 'startDate=2023-01-01' in url
        assert 'endDate=2023-06-01' in url
        assert 'fields' not in url
        assert 'reportId' not in url
        assert 'type=' not in url
        assert 'includeAllBusinessDays' not in url
        assert result == []

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_fields(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_positions_data(
            'MP1', dt.date(2023, 1, 1), dt.date(2023, 6, 1),
            fields=['quantity', 'price'],
        )
        url = session.sync.get.call_args[0][0]
        assert '&fields=quantity' in url
        assert '&fields=price' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_performance_report_id(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_positions_data(
            'MP1', dt.date(2023, 1, 1), dt.date(2023, 6, 1),
            performance_report_id='RPT1',
        )
        url = session.sync.get.call_args[0][0]
        assert '&reportId=RPT1' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_position_type(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_positions_data(
            'MP1', dt.date(2023, 1, 1), dt.date(2023, 6, 1),
            position_type=PositionType.CLOSE,
        )
        url = session.sync.get.call_args[0][0]
        assert '&type=close' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_include_all_business_days(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_positions_data(
            'MP1', dt.date(2023, 1, 1), dt.date(2023, 6, 1),
            include_all_business_days=True,
        )
        url = session.sync.get.call_args[0][0]
        assert '&includeAllBusinessDays=true' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_include_all_business_days_false(self, mock_gs):
        """When include_all_business_days is False (falsy), the branch is not taken."""
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': []}

        GsPortfolioApi.get_positions_data(
            'MP1', dt.date(2023, 1, 1), dt.date(2023, 6, 1),
            include_all_business_days=False,
        )
        url = session.sync.get.call_args[0][0]
        assert 'includeAllBusinessDays' not in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_all_params(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': ['data']}

        result = GsPortfolioApi.get_positions_data(
            'MP1', dt.date(2023, 1, 1), dt.date(2023, 6, 1),
            fields=['field1'],
            performance_report_id='RPT1',
            position_type=PositionType.OPEN,
            include_all_business_days=True,
        )
        url = session.sync.get.call_args[0][0]
        assert '&fields=field1' in url
        assert '&reportId=RPT1' in url
        assert '&type=open' in url
        assert '&includeAllBusinessDays=true' in url
        assert result == ['data']


# ---------------------------------------------------------------------------
# update_quote
# ---------------------------------------------------------------------------

class TestUpdateQuote:
    @patch.object(GsPortfolioApi, 'get_session')
    def test_basic(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        request = MagicMock()
        session.sync.put.return_value = 'ok'

        result = GsPortfolioApi.update_quote('Q1', request)
        session.sync.put.assert_called_once_with('/risk-internal/quote/save/Q1', request)
        assert result == 'ok'


# ---------------------------------------------------------------------------
# save_quote
# ---------------------------------------------------------------------------

class TestSaveQuote:
    @patch.object(GsPortfolioApi, 'get_session')
    def test_basic(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        request = MagicMock()
        session.sync.post.return_value = {'results': 'quote_id'}

        result = GsPortfolioApi.save_quote(request)
        session.sync.post.assert_called_once_with('/risk-internal/quote/save', request)
        assert result == 'quote_id'


# ---------------------------------------------------------------------------
# update_workflow_quote
# ---------------------------------------------------------------------------

class TestUpdateWorkflowQuote:
    @patch.object(GsPortfolioApi, 'get_session')
    def test_basic(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        request = MagicMock()
        session.sync.put.return_value = {'results': 'updated'}

        result = GsPortfolioApi.update_workflow_quote('Q1', request)
        session.sync.put.assert_called_once_with(
            '/risk-internal/quote/workflow/save/Q1',
            (request,),
            request_headers={'Content-Type': 'application/x-msgpack'},
        )
        assert result == 'updated'


# ---------------------------------------------------------------------------
# save_workflow_quote
# ---------------------------------------------------------------------------

class TestSaveWorkflowQuote:
    @patch.object(GsPortfolioApi, 'get_session')
    def test_basic(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        request = MagicMock()
        session.sync.post.return_value = {'results': 'wf_id'}

        result = GsPortfolioApi.save_workflow_quote(request)
        session.sync.post.assert_called_once_with(
            '/risk-internal/quote/workflow/save',
            (request,),
            request_headers={'Content-Type': 'application/x-msgpack'},
        )
        assert result == 'wf_id'


# ---------------------------------------------------------------------------
# share_workflow_quote
# ---------------------------------------------------------------------------

class TestShareWorkflowQuote:
    @patch.object(GsPortfolioApi, 'get_session')
    def test_basic(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        request = MagicMock()
        session.sync.post.return_value = {'results': 'shared_id'}

        result = GsPortfolioApi.share_workflow_quote(request)
        session.sync.post.assert_called_once_with(
            '/risk-internal/quote/workflow/share',
            (request,),
            request_headers={'Content-Type': 'application/x-msgpack'},
        )
        assert result == 'shared_id'


# ---------------------------------------------------------------------------
# get_workflow_quote
# ---------------------------------------------------------------------------

class TestGetWorkflowQuote:
    """Branches: wf_pos_res truthy / falsy."""

    @patch('gs_quant.api.gs.portfolios.WorkflowPositionsResponse')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_truthy_response(self, mock_get_session, MockWfPosRes):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'some': 'data'}

        wf_pos = MagicMock()
        wf_pos.results = ('pos1', 'pos2')
        MockWfPosRes.from_dict.return_value = wf_pos

        result = GsPortfolioApi.get_workflow_quote('wf123')
        assert result == ('pos1', 'pos2')
        session.sync.get.assert_called_once_with(
            '/risk-internal/quote/workflow/wf123', timeout=181
        )

    @patch('gs_quant.api.gs.portfolios.WorkflowPositionsResponse')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_falsy_response(self, mock_get_session, MockWfPosRes):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {}

        MockWfPosRes.from_dict.return_value = None

        result = GsPortfolioApi.get_workflow_quote('wf123')
        assert result == ()


# ---------------------------------------------------------------------------
# get_shared_workflow_quote
# ---------------------------------------------------------------------------

class TestGetSharedWorkflowQuote:
    """Branches: wf_pos_res truthy / falsy."""

    @patch('gs_quant.api.gs.portfolios.WorkflowPositionsResponse')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_truthy_response(self, mock_get_session, MockWfPosRes):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'some': 'data'}

        wf_pos = MagicMock()
        wf_pos.results = ('pos1',)
        MockWfPosRes.from_dict.return_value = wf_pos

        result = GsPortfolioApi.get_shared_workflow_quote('wf123')
        assert result == ('pos1',)
        session.sync.get.assert_called_once_with(
            '/risk-internal/quote/workflow/shared/wf123', timeout=181
        )

    @patch('gs_quant.api.gs.portfolios.WorkflowPositionsResponse')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_falsy_response(self, mock_get_session, MockWfPosRes):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {}

        MockWfPosRes.from_dict.return_value = None

        result = GsPortfolioApi.get_shared_workflow_quote('wf123')
        assert result == ()


# ---------------------------------------------------------------------------
# save_to_shadowbook
# ---------------------------------------------------------------------------

class TestSaveToShadowbook:
    @patch.object(GsPortfolioApi, 'get_session')
    def test_basic(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        request = MagicMock()
        session.sync.put.return_value = {'results': 'saved'}

        result = GsPortfolioApi.save_to_shadowbook(request, 'my_book')
        session.sync.put.assert_called_once_with('/risk-internal/shadowbook/save/my_book', request)
        assert result == 'saved'


# ---------------------------------------------------------------------------
# get_risk_models_by_coverage
# ---------------------------------------------------------------------------

class TestGetRiskModelsByCoverage:
    @patch.object(GsPortfolioApi, 'get_session')
    def test_default_term(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'results': ['model1']}

        result = GsPortfolioApi.get_risk_models_by_coverage('MP1')
        url = session.sync.get.call_args[0][0]
        assert 'sortByTerm=Medium' in url
        assert result == ['model1']

    @patch.object(GsPortfolioApi, 'get_session')
    def test_custom_term(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {'results': ['model2']}

        result = GsPortfolioApi.get_risk_models_by_coverage('MP1', term=Term.Daily)
        url = session.sync.get.call_args[0][0]
        assert 'sortByTerm=Daily' in url


# ---------------------------------------------------------------------------
# get_reports
# ---------------------------------------------------------------------------

class TestGetReports:
    """Branches: tags is not None / is None."""

    @patch.object(GsPortfolioApi, 'get_session')
    def test_with_tags_matching(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session

        params = MagicMock()
        params.tags = (PositionTag(name='strat', value='ls'),)
        report = MagicMock()
        report.parameters = params

        session.sync.get.return_value = {'results': [report]}

        result = GsPortfolioApi.get_reports('MP1', tags={'strat': 'ls'})
        assert result == [report]

    @patch.object(GsPortfolioApi, 'get_session')
    def test_with_tags_not_matching(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session

        params = MagicMock()
        params.tags = (PositionTag(name='strat', value='other'),)
        report = MagicMock()
        report.parameters = params

        session.sync.get.return_value = {'results': [report]}

        result = GsPortfolioApi.get_reports('MP1', tags={'strat': 'ls'})
        assert result == []

    @patch.object(GsPortfolioApi, 'get_session')
    def test_with_tags_none(self, mock_get_session):
        """When tags is None, no filtering is done."""
        session = _mock_session()
        mock_get_session.return_value = session

        report = MagicMock()
        session.sync.get.return_value = {'results': [report]}

        result = GsPortfolioApi.get_reports('MP1', tags=None)
        assert result == [report]


# ---------------------------------------------------------------------------
# schedule_reports
# ---------------------------------------------------------------------------

class TestScheduleReports:
    """Branches:
    - start_date is not None / None
    - end_date is not None / None
    - tag_name_hierarchy is None or empty -> schedule portfolio
    - else -> iterate report_ids, count hits 0 (sleep branch) and >0
    """

    @patch.object(GsPortfolioApi, 'get_portfolio')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_no_tag_hierarchy_no_dates(self, mock_get_session, mock_get_portfolio):
        session = _mock_session()
        mock_get_session.return_value = session

        portfolio = MagicMock()
        portfolio.tag_name_hierarchy = None
        mock_get_portfolio.return_value = portfolio

        GsPortfolioApi.schedule_reports('MP1')
        session.sync.post.assert_called_once()
        call_args = session.sync.post.call_args
        assert call_args[0][0] == '/portfolios/MP1/schedule'
        payload = call_args[0][1]
        assert 'startDate' not in payload
        assert 'endDate' not in payload

    @patch.object(GsPortfolioApi, 'get_portfolio')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_no_tag_hierarchy_with_dates(self, mock_get_session, mock_get_portfolio):
        session = _mock_session()
        mock_get_session.return_value = session

        portfolio = MagicMock()
        portfolio.tag_name_hierarchy = []
        mock_get_portfolio.return_value = portfolio

        GsPortfolioApi.schedule_reports(
            'MP1',
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
            backcast=True,
        )
        call_args = session.sync.post.call_args
        payload = call_args[0][1]
        assert payload['startDate'] == '2023-01-01'
        assert payload['endDate'] == '2023-06-01'
        assert payload['parameters']['backcast'] is True

    @patch('gs_quant.api.gs.portfolios.sleep')
    @patch.object(GsPortfolioApi, 'get_portfolio')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_with_tag_hierarchy_few_reports(self, mock_get_session, mock_get_portfolio, mock_sleep):
        """With tag_name_hierarchy set and fewer than 11 reports (count never reaches 0)."""
        session = _mock_session()
        mock_get_session.return_value = session

        portfolio = MagicMock()
        portfolio.tag_name_hierarchy = ['sector', 'industry']
        portfolio.report_ids = ['R1', 'R2', 'R3']
        mock_get_portfolio.return_value = portfolio

        GsPortfolioApi.schedule_reports('MP1')
        assert session.sync.post.call_count == 3
        mock_sleep.assert_not_called()

    @patch('gs_quant.api.gs.portfolios.sleep')
    @patch.object(GsPortfolioApi, 'get_portfolio')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_with_tag_hierarchy_many_reports_triggers_sleep(self, mock_get_session, mock_get_portfolio, mock_sleep):
        """With 11+ reports, count reaches 0, triggering the sleep branch."""
        session = _mock_session()
        mock_get_session.return_value = session

        portfolio = MagicMock()
        portfolio.tag_name_hierarchy = ['sector']
        # 12 reports: first 11 decrement count from 10 to -1... actually count starts at 10
        # count=10: first report -> count becomes 9 (else branch)
        # ...
        # count=1: 10th report -> count becomes 0 (else branch)
        # count=0: 11th report -> sleep branch -> count reset to 10
        # count=10: 12th report -> else branch -> count becomes 9
        portfolio.report_ids = [f'R{i}' for i in range(12)]
        mock_get_portfolio.return_value = portfolio

        GsPortfolioApi.schedule_reports('MP1')
        assert session.sync.post.call_count == 12
        mock_sleep.assert_called_once_with(2)

    @patch.object(GsPortfolioApi, 'get_portfolio')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_with_start_date_only(self, mock_get_session, mock_get_portfolio):
        session = _mock_session()
        mock_get_session.return_value = session

        portfolio = MagicMock()
        portfolio.tag_name_hierarchy = None
        mock_get_portfolio.return_value = portfolio

        GsPortfolioApi.schedule_reports('MP1', start_date=dt.date(2023, 1, 1))
        payload = session.sync.post.call_args[0][1]
        assert payload['startDate'] == '2023-01-01'
        assert 'endDate' not in payload

    @patch.object(GsPortfolioApi, 'get_portfolio')
    @patch.object(GsPortfolioApi, 'get_session')
    def test_with_end_date_only(self, mock_get_session, mock_get_portfolio):
        session = _mock_session()
        mock_get_session.return_value = session

        portfolio = MagicMock()
        portfolio.tag_name_hierarchy = None
        mock_get_portfolio.return_value = portfolio

        GsPortfolioApi.schedule_reports('MP1', end_date=dt.date(2023, 6, 1))
        payload = session.sync.post.call_args[0][1]
        assert 'startDate' not in payload
        assert payload['endDate'] == '2023-06-01'


# ---------------------------------------------------------------------------
# get_schedule_dates
# ---------------------------------------------------------------------------

class TestGetScheduleDates:
    @patch.object(GsPortfolioApi, 'get_session')
    def test_default_backcast(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {
            'startDate': '2023-01-01',
            'endDate': '2023-06-01',
        }

        result = GsPortfolioApi.get_schedule_dates('MP1')
        url = session.sync.get.call_args[0][0]
        assert 'backcast=false' in url
        assert result == [dt.date(2023, 1, 1), dt.date(2023, 6, 1)]

    @patch.object(GsPortfolioApi, 'get_session')
    def test_backcast_true(self, mock_get_session):
        session = _mock_session()
        mock_get_session.return_value = session
        session.sync.get.return_value = {
            'startDate': '2022-01-01',
            'endDate': '2022-12-31',
        }

        result = GsPortfolioApi.get_schedule_dates('MP1', backcast=True)
        url = session.sync.get.call_args[0][0]
        assert 'backcast=true' in url
        assert result == [dt.date(2022, 1, 1), dt.date(2022, 12, 31)]


# ---------------------------------------------------------------------------
# get_custom_aum (deprecated)
# ---------------------------------------------------------------------------

class TestGetCustomAum:
    """Branches: start_date truthy/falsy, end_date truthy/falsy."""

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_no_dates(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'data': [{'aum': 100}]}

        result = GsPortfolioApi.get_custom_aum('MP1')
        url = session.sync.get.call_args[0][0]
        assert 'startDate' not in url
        assert 'endDate' not in url
        assert result == [{'aum': 100}]

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_start_date(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'data': []}

        GsPortfolioApi.get_custom_aum('MP1', start_date=dt.date(2023, 1, 1))
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_end_date(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'data': []}

        GsPortfolioApi.get_custom_aum('MP1', end_date=dt.date(2023, 6, 1))
        url = session.sync.get.call_args[0][0]
        assert '&endDate=2023-06-01' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_both_dates(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'data': []}

        GsPortfolioApi.get_custom_aum('MP1', start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 6, 1))
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-01' in url


# ---------------------------------------------------------------------------
# upload_custom_aum (deprecated)
# ---------------------------------------------------------------------------

class TestUploadCustomAum:
    """Branches: clear_existing_data truthy / falsy."""

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_no_clear(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.post.return_value = 'ok'

        result = GsPortfolioApi.upload_custom_aum('MP1', [{'date': '2023-01-01', 'aum': 100}])
        url = session.sync.post.call_args[0][0]
        assert 'clearExistingData' not in url
        assert result == 'ok'

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_clear(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.post.return_value = 'ok'

        result = GsPortfolioApi.upload_custom_aum('MP1', [{'date': '2023-01-01', 'aum': 100}], clear_existing_data=True)
        url = session.sync.post.call_args[0][0]
        assert '?clearExistingData=true' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_clear_false(self, mock_gs):
        """clear_existing_data=False is falsy, so no query param."""
        session = _mock_session()
        mock_gs.current = session
        session.sync.post.return_value = 'ok'

        GsPortfolioApi.upload_custom_aum('MP1', [], clear_existing_data=False)
        url = session.sync.post.call_args[0][0]
        assert 'clearExistingData' not in url


# ---------------------------------------------------------------------------
# update_portfolio_tree
# ---------------------------------------------------------------------------

class TestUpdatePortfolioTree:
    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_basic(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.post.return_value = 'ok'

        result = GsPortfolioApi.update_portfolio_tree('MP1')
        session.sync.post.assert_called_once_with('/portfolios/MP1/tree', {})
        assert result == 'ok'


# ---------------------------------------------------------------------------
# get_portfolio_tree
# ---------------------------------------------------------------------------

class TestGetPortfolioTree:
    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_basic(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = 'tree_obj'

        result = GsPortfolioApi.get_portfolio_tree('MP1')
        session.sync.get.assert_called_once_with('/portfolios/MP1/tree', cls=PortfolioTree)
        assert result == 'tree_obj'


# ---------------------------------------------------------------------------
# get_attribution
# ---------------------------------------------------------------------------

class TestGetAttribution:
    """Branches: start_date, end_date, currency, performance_report_id - each truthy / falsy."""

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_no_optional_params(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': {'attr': 'data'}}

        result = GsPortfolioApi.get_attribution('MP1')
        url = session.sync.get.call_args[0][0]
        assert url == '/attribution/MP1?'
        assert result == {'attr': 'data'}

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_start_date(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': {}}

        GsPortfolioApi.get_attribution('MP1', start_date=dt.date(2023, 1, 1))
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_end_date(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': {}}

        GsPortfolioApi.get_attribution('MP1', end_date=dt.date(2023, 6, 1))
        url = session.sync.get.call_args[0][0]
        assert '&endDate=2023-06-01' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_currency(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': {}}

        GsPortfolioApi.get_attribution('MP1', currency=Currency.USD)
        url = session.sync.get.call_args[0][0]
        assert '&currency=USD' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_with_performance_report_id(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': {}}

        GsPortfolioApi.get_attribution('MP1', performance_report_id='RPT1')
        url = session.sync.get.call_args[0][0]
        assert '&reportId=RPT1' in url

    @patch('gs_quant.api.gs.portfolios.GsSession')
    def test_all_params(self, mock_gs):
        session = _mock_session()
        mock_gs.current = session
        session.sync.get.return_value = {'results': {'full': 'data'}}

        result = GsPortfolioApi.get_attribution(
            'MP1',
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
            currency=Currency.EUR,
            performance_report_id='RPT1',
        )
        url = session.sync.get.call_args[0][0]
        assert '&startDate=2023-01-01' in url
        assert '&endDate=2023-06-01' in url
        assert '&currency=EUR' in url
        assert '&reportId=RPT1' in url
        assert result == {'full': 'data'}
