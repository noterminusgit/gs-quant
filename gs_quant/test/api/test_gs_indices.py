"""
Tests for gs_quant.api.gs.indices - GsIndexApi
Target: 100% branch coverage
"""

import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from gs_quant.api.gs.indices import GsIndexApi
from gs_quant.common import PositionType
from gs_quant.target.indices import (
    CustomBasketsCreateInputs,
    CustomBasketsRebalanceInputs,
    CustomBasketsRebalanceAction,
    CustomBasketsResponse,
    CustomBasketsEditInputs,
    CustomBasketsBackcastInputs,
    CustomBasketRiskParams,
    CustomBasketsRiskScheduleInputs,
    ISelectResponse,
    ISelectRequest,
    ISelectRebalance,
    ISelectActionRequest,
    IndicesDynamicConstructInputs,
    IndicesRebalanceInputs,
    IndicesEditInputs,
    IndicesBackcastInputs,
    DynamicConstructionResponse,
    ApprovalCustomBasketResponse,
)


def _mock_session():
    session = MagicMock()
    return session


class TestGsIndexApiCreate:
    def test_create_custom_basket(self):
        mock_session = _mock_session()
        inputs = CustomBasketsCreateInputs()
        mock_session.sync.post.return_value = MagicMock()
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.create(inputs)
            mock_session.sync.post.assert_called_once_with(
                '/indices', payload=inputs, cls=CustomBasketsResponse
            )

    def test_create_dynamic_construction(self):
        mock_session = _mock_session()
        inputs = IndicesDynamicConstructInputs()
        mock_session.sync.post.return_value = MagicMock()
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.create(inputs)
            mock_session.sync.post.assert_called_once_with(
                '/indices', payload=inputs, cls=DynamicConstructionResponse
            )


class TestGsIndexApiEdit:
    def test_edit(self):
        mock_session = _mock_session()
        inputs = CustomBasketsEditInputs()
        mock_session.sync.post.return_value = MagicMock()
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.edit('idx1', inputs)
            call_args = mock_session.sync.post.call_args
            assert call_args[0][0] == '/indices/idx1/edit'
            assert isinstance(call_args[1]['payload'], IndicesEditInputs)
            assert call_args[1]['cls'] == CustomBasketsResponse


class TestGsIndexApiRebalance:
    def test_rebalance_custom_basket(self):
        """Branch: not isinstance(inputs, ISelectRequest) -> wraps in IndicesRebalanceInputs"""
        mock_session = _mock_session()
        inputs = CustomBasketsRebalanceInputs()
        mock_session.sync.post.return_value = MagicMock()
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.rebalance('idx1', inputs)
            call_args = mock_session.sync.post.call_args
            assert call_args[0][0] == '/indices/idx1/rebalance'
            assert isinstance(call_args[1]['payload'], IndicesRebalanceInputs)
            assert call_args[1]['cls'] == CustomBasketsResponse

    def test_rebalance_iselect_rebalance(self):
        """Branch: ISelectRebalance -> not isinstance(inputs, ISelectRequest) is True -> wraps"""
        mock_session = _mock_session()
        inputs = ISelectRebalance()
        mock_session.sync.post.return_value = MagicMock()
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.rebalance('idx2', inputs)
            call_args = mock_session.sync.post.call_args
            assert isinstance(call_args[1]['payload'], IndicesRebalanceInputs)
            assert call_args[1]['cls'] == ISelectResponse

    def test_rebalance_iselect_request(self):
        """Branch: isinstance(inputs, ISelectRequest) -> uses inputs directly"""
        mock_session = _mock_session()
        inputs = ISelectRequest()
        mock_session.sync.post.return_value = MagicMock()
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.rebalance('idx3', inputs)
            call_args = mock_session.sync.post.call_args
            assert call_args[1]['payload'] is inputs
            assert call_args[1]['cls'] == ISelectResponse


class TestGsIndexApiCancelRebalance:
    def test_cancel_rebalance_custom_basket(self):
        mock_session = _mock_session()
        inputs = CustomBasketsRebalanceAction()
        mock_session.sync.post.return_value = {}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsIndexApi.cancel_rebalance('idx1', inputs)
            call_args = mock_session.sync.post.call_args
            assert call_args[0][0] == '/indices/idx1/rebalance/cancel'
            # The _response_cls maps to typing.Dict (not builtin dict)
            from typing import Dict
            assert call_args[1]['cls'] == Dict

    def test_cancel_rebalance_iselect(self):
        mock_session = _mock_session()
        inputs = ISelectActionRequest()
        mock_session.sync.post.return_value = MagicMock()
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.cancel_rebalance('idx2', inputs)
            call_args = mock_session.sync.post.call_args
            assert call_args[1]['cls'] == ISelectResponse


class TestGsIndexApiMisc:
    def test_last_rebalance_data(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'data': 'value'}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsIndexApi.last_rebalance_data('idx1')
            mock_session.sync.get.assert_called_once_with('/indices/idx1/rebalance/data/last')
            assert result == {'data': 'value'}

    def test_last_rebalance_approval(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = MagicMock(spec=ApprovalCustomBasketResponse)
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.last_rebalance_approval('idx1')
            mock_session.sync.get.assert_called_once_with(
                '/indices/idx1/rebalance/approvals/last', cls=ApprovalCustomBasketResponse
            )

    def test_initial_price(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'price': 100.0}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsIndexApi.initial_price('idx1', dt.date(2023, 1, 15))
            mock_session.sync.get.assert_called_once_with(
                '/indices/idx1/rebalance/initialprice/2023-01-15'
            )
            assert result == {'price': 100.0}

    def test_validate_ticker(self):
        mock_session = _mock_session()
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.validate_ticker('MYTICK')
            mock_session.sync.post.assert_called_once_with(
                '/indices/validate', payload={'ticker': 'MYTICK'}
            )

    def test_backcast(self):
        mock_session = _mock_session()
        inputs = CustomBasketsBackcastInputs()
        mock_session.sync.post.return_value = MagicMock()
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.backcast('idx1', inputs)
            call_args = mock_session.sync.post.call_args
            assert call_args[0][0] == '/indices/idx1/backcast'
            assert isinstance(call_args[1]['payload'], IndicesBackcastInputs)
            assert call_args[1]['cls'] == CustomBasketsResponse
            assert call_args[1]['timeout'] == 240

    def test_update_risk_reports(self):
        mock_session = _mock_session()
        # The source does CustomBasketsRiskScheduleInputs(risk_models=inputs)
        # where risk_models is Tuple[CustomBasketRiskParams, ...].
        # Passing a tuple works since the dataclass coercion expects an iterable.
        inputs = (CustomBasketRiskParams(risk_model='MODEL1'),)
        mock_session.sync.post.return_value = {}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.update_risk_reports('idx1', inputs)
            call_args = mock_session.sync.post.call_args
            assert call_args[0][0] == '/indices/idx1/risk/reports'
            assert isinstance(call_args[1]['payload'], CustomBasketsRiskScheduleInputs)


class TestGsIndexApiPositionsData:
    def test_get_positions_data_no_fields_no_type(self):
        """Branch: fields is None, position_type is None"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': [{'a': 1}]}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsIndexApi.get_positions_data('asset1', dt.date(2023, 1, 1), dt.date(2023, 6, 30))
            url = mock_session.sync.get.call_args[0][0]
            assert 'asset1' in url
            assert 'startDate=2023-01-01' in url
            assert 'endDate=2023-06-30' in url
            assert '&fields=' not in url
            assert '&type=' not in url
            assert result == [{'a': 1}]

    def test_get_positions_data_with_fields(self):
        """Branch: fields is not None"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsIndexApi.get_positions_data(
                'asset1', dt.date(2023, 1, 1), dt.date(2023, 6, 30), fields=['field1', 'field2']
            )
            url = mock_session.sync.get.call_args[0][0]
            assert '&fields=field1' in url
            assert '&fields=field2' in url

    def test_get_positions_data_with_position_type(self):
        """Branch: position_type is not None"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.get_positions_data(
                'asset1', dt.date(2023, 1, 1), dt.date(2023, 6, 30), position_type=PositionType.CLOSE
            )
            url = mock_session.sync.get.call_args[0][0]
            assert '&type=close' in url

    def test_get_positions_data_with_fields_and_type(self):
        """Branch: both fields and position_type provided"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': [{'x': 1}]}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsIndexApi.get_positions_data(
                'asset1', dt.date(2023, 1, 1), dt.date(2023, 6, 30),
                fields=['f1'], position_type=PositionType.CLOSE
            )
            url = mock_session.sync.get.call_args[0][0]
            assert '&fields=f1' in url
            assert '&type=close' in url


class TestGsIndexApiLastPositionsData:
    def test_get_last_positions_data_no_fields_no_type(self):
        """Branch: fields is None, position_type is None, params empty -> no query string"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': [{'b': 2}]}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsIndexApi.get_last_positions_data('asset1')
            url = mock_session.sync.get.call_args[0][0]
            assert url == '/indices/asset1/positions/last/data'
            assert result == [{'b': 2}]

    def test_get_last_positions_data_with_fields(self):
        """Branch: fields is not None -> params has content -> len(params) > 0"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.get_last_positions_data('asset1', fields=['f1', 'f2'])
            url = mock_session.sync.get.call_args[0][0]
            assert '?' in url
            assert '&fields=f1' in url
            assert '&fields=f2' in url

    def test_get_last_positions_data_with_position_type(self):
        """Branch: position_type is not None"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.get_last_positions_data('asset1', position_type=PositionType.CLOSE)
            url = mock_session.sync.get.call_args[0][0]
            assert '?' in url
            assert '&type=close' in url

    def test_get_last_positions_data_with_fields_and_type(self):
        """Branch: both fields and position_type"""
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': []}
        with patch('gs_quant.api.gs.indices.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsIndexApi.get_last_positions_data(
                'asset1', fields=['f1'], position_type=PositionType.CLOSE
            )
            url = mock_session.sync.get.call_args[0][0]
            assert '?' in url
            assert '&fields=f1' in url
            assert '&type=close' in url
