"""
Branch coverage tests for gs_quant/api/gs/backtests_xasset/json_encoders/response_encoders.py
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders import (
    encode_response_obj,
    decode_leg_refs,
    decode_risk_measure_refs,
    decode_result_tuple,
    decode_basic_bt_measure_dict,
    decode_basic_bt_transactions,
)
from gs_quant.common import RiskMeasure, Currency, CurrencyName
from gs_quant.api.gs.backtests_xasset.response_datatypes.backtest_datatypes import (
    Transaction,
    TransactionDirection,
)
from gs_quant.target.backtests import FlowVolBacktestMeasure


class TestEncodeResponseObj:
    """Test encode_response_obj function."""

    def test_risk_measure(self):
        """RiskMeasure is encoded via encode_risk_measure."""
        rm = MagicMock(spec=RiskMeasure)
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.encode_risk_measure'
        ) as mock_enc:
            mock_enc.return_value = {'name': 'DollarPrice'}
            result = encode_response_obj(rm)
            mock_enc.assert_called_once_with(rm)

    def test_series(self):
        """Series is encoded via encode_series_result."""
        s = pd.Series([1.0, 2.0], name='test')
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.encode_series_result'
        ) as mock_enc:
            mock_enc.return_value = {'values': [1.0, 2.0]}
            result = encode_response_obj(s)
            mock_enc.assert_called_once()

    def test_dataframe(self):
        """DataFrame is encoded via encode_dataframe_result."""
        df = pd.DataFrame({'a': [1, 2]})
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.encode_dataframe_result'
        ) as mock_enc:
            mock_enc.return_value = {'values': [[1], [2]]}
            result = encode_response_obj(df)
            mock_enc.assert_called_once()

    def test_other_object(self):
        """Other objects call to_dict."""
        mock_obj = MagicMock()
        mock_obj.to_dict.return_value = {'key': 'value'}
        result = encode_response_obj(mock_obj)
        assert result == {'key': 'value'}


class TestDecodeLegRefs:
    """Test decode_leg_refs function."""

    def test_basic(self):
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.decode_inst'
        ) as mock_decode:
            mock_decode.return_value = MagicMock()
            result = decode_leg_refs({'leg1': {'type': 'IRSwap'}, 'leg2': {'type': 'IRCap'}})
            assert len(result) == 2
            assert 'leg1' in result
            assert 'leg2' in result


class TestDecodeRiskMeasureRefs:
    """Test decode_risk_measure_refs function."""

    def test_basic(self):
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.decode_risk_measure'
        ) as mock_decode:
            mock_rm = MagicMock(spec=RiskMeasure)
            mock_decode.return_value = mock_rm
            result = decode_risk_measure_refs({'rm1': {'name': 'DollarPrice'}})
            assert len(result) == 1
            assert 'rm1' in result


class TestDecodeResultTuple:
    """Test decode_result_tuple function."""

    def test_basic(self):
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.decode_risk_result'
        ) as mock_decode:
            mock_decode.return_value = MagicMock()
            result = decode_result_tuple(({'refs': {}, 'result': {}},))
            assert len(result) == 1


class TestDecodeBasicBtMeasureDict:
    """Test decode_basic_bt_measure_dict function."""

    def test_basic(self):
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.decode_risk_result_with_data'
        ) as mock_decode:
            mock_decode.return_value = MagicMock()
            data = {
                'PNL_spot': {'2020-01-01': {'type': 'float', 'result': 100.0}}
            }
            result = decode_basic_bt_measure_dict(data)
            assert FlowVolBacktestMeasure.PNL_spot in result


class TestDecodeBasicBtTransactions:
    """Test decode_basic_bt_transactions function."""

    def test_with_currency_enum(self):
        """Currency string that matches Currency enum."""
        data = {
            '2020-01-01': [
                {
                    'portfolio': [],
                    'portfolio_price': 100.0,
                    'cost': 1.0,
                    'currency': 'USD',
                    'direction': 'Entry',
                    'quantity': 1.0,
                }
            ]
        }
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.decode_inst_tuple'
        ) as mock_decode:
            mock_decode.return_value = ()
            result = decode_basic_bt_transactions(data)
            key = dt.date(2020, 1, 1)
            assert key in result
            assert len(result[key]) == 1
            txn = result[key][0]
            assert isinstance(txn, Transaction)
            assert txn.currency == Currency.USD

    def test_with_currency_name_enum(self):
        """Currency string that matches CurrencyName enum."""
        data = {
            '2020-01-01': [
                {
                    'portfolio': [],
                    'portfolio_price': 100.0,
                    'cost': 1.0,
                    'currency': 'United States Dollar',
                    'direction': 'Exit',
                    'quantity': 1.0,
                }
            ]
        }
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.decode_inst_tuple'
        ) as mock_decode:
            mock_decode.return_value = ()
            result = decode_basic_bt_transactions(data)
            txn = result[dt.date(2020, 1, 1)][0]
            assert txn.currency == CurrencyName.United_States_Dollar

    def test_with_unknown_currency_string(self):
        """Currency string that matches neither enum returns as string."""
        data = {
            '2020-01-01': [
                {
                    'portfolio': [],
                    'portfolio_price': None,
                    'cost': None,
                    'currency': 'XYZ_UNKNOWN',
                    'direction': None,
                    'quantity': None,
                }
            ]
        }
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.decode_inst_tuple'
        ) as mock_decode:
            mock_decode.return_value = ()
            result = decode_basic_bt_transactions(data)
            txn = result[dt.date(2020, 1, 1)][0]
            assert txn.currency == 'XYZ_UNKNOWN'

    def test_with_no_currency(self):
        """No currency key results in None."""
        data = {
            '2020-01-01': [
                {
                    'portfolio': [],
                    'direction': None,
                }
            ]
        }
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.decode_inst_tuple'
        ) as mock_decode:
            mock_decode.return_value = ()
            result = decode_basic_bt_transactions(data)
            txn = result[dt.date(2020, 1, 1)][0]
            assert txn.currency is None

    def test_without_decode_instruments(self):
        """decode_instruments=False passes raw portfolio through."""
        data = {
            '2020-01-01': [
                {
                    'portfolio': [{'type': 'IRSwap'}],
                    'portfolio_price': 100.0,
                    'cost': 1.0,
                    'currency': 'USD',
                    'direction': 'Entry',
                    'quantity': 1.0,
                }
            ]
        }
        result = decode_basic_bt_transactions(data, decode_instruments=False)
        txn = result[dt.date(2020, 1, 1)][0]
        assert isinstance(txn, Transaction)
        # portfolio should be the raw list
        assert txn.portfolio == [{'type': 'IRSwap'}]

    def test_with_no_direction(self):
        """No direction key results in None."""
        data = {
            '2020-01-01': [
                {
                    'portfolio': [],
                }
            ]
        }
        with patch(
            'gs_quant.api.gs.backtests_xasset.json_encoders.response_encoders.decode_inst_tuple'
        ) as mock_decode:
            mock_decode.return_value = ()
            result = decode_basic_bt_transactions(data)
            txn = result[dt.date(2020, 1, 1)][0]
            assert txn.direction is None
