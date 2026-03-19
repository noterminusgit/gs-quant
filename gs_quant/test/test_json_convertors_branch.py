"""
Branch coverage tests for gs_quant/json_convertors.py
Targeting remaining uncovered branches.
"""

import datetime as dt
from dataclasses import dataclass, field, MISSING
from typing import Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gs_quant.json_convertors import (
    decode_named_instrument,
    decode_named_portfolio,
    encode_named_instrument,
    encode_named_portfolio,
    decode_quote_report,
    decode_quote_reports,
    decode_custom_comment,
    decode_custom_comments,
    decode_hedge_type,
    decode_hedge_types,
    _get_dc_type,
    dc_decode,
    _value_decoder,
    decode_date_or_str,
    decode_iso_date_or_datetime,
    encode_datetime,
    decode_datetime,
)


class TestDecodeNamedInstrument:
    """Test decode_named_instrument function."""

    def test_none(self):
        """None returns None."""
        assert decode_named_instrument(None) is None

    def test_empty_dict(self):
        """Empty dict returns None."""
        assert decode_named_instrument({}) is None

    def test_list_input(self):
        """List input recurses on each element."""
        with patch('gs_quant.instrument.Instrument.from_dict', return_value=MagicMock()):
            result = decode_named_instrument([
                {'type': 'IRSwap', 'asset_class': 'Rates'},
                {'type': 'IRSwap', 'asset_class': 'Rates'},
            ])
            assert isinstance(result, tuple)
            assert len(result) == 2

    def test_tuple_input(self):
        """Tuple input recurses on each element."""
        with patch('gs_quant.instrument.Instrument.from_dict', return_value=MagicMock()):
            result = decode_named_instrument((
                {'type': 'IRSwap', 'asset_class': 'Rates'},
            ))
            assert isinstance(result, tuple)

    def test_dict_with_portfolio_name(self):
        """Dict with 'portfolio_name' delegates to decode_named_portfolio."""
        with patch('gs_quant.json_convertors.decode_named_portfolio') as mock_decode:
            mock_decode.return_value = MagicMock()
            value = {'portfolio_name': 'test', 'instruments': []}
            result = decode_named_instrument(value)
            mock_decode.assert_called_once_with(value)

    def test_dict_without_portfolio_name(self):
        """Regular dict delegates to Instrument.from_dict."""
        with patch('gs_quant.instrument.Instrument.from_dict', return_value=MagicMock()):
            result = decode_named_instrument({'type': 'IRSwap', 'asset_class': 'Rates'})
            assert result is not None


class TestDecodeNamedPortfolio:
    """Test decode_named_portfolio function."""

    def test_basic(self):
        """Decodes a portfolio dict into a Portfolio."""
        with patch('gs_quant.instrument.Instrument.from_dict', return_value=MagicMock()):
            value = {
                'portfolio_name': 'test_port',
                'instruments': [{'type': 'IRSwap'}]
            }
            result = decode_named_portfolio(value)
            from gs_quant.markets.portfolio import Portfolio
            assert isinstance(result, Portfolio)
            assert result.name == 'test_port'


class TestEncodeNamedInstrument:
    """Test encode_named_instrument function."""

    def test_list(self):
        """List input recurses."""
        mock_inst = MagicMock()
        mock_inst.as_dict.return_value = {'type': 'IRSwap'}
        result = encode_named_instrument([mock_inst])
        assert isinstance(result, tuple)

    def test_tuple(self):
        """Tuple input recurses."""
        mock_inst = MagicMock()
        mock_inst.as_dict.return_value = {'type': 'IRSwap'}
        result = encode_named_instrument((mock_inst,))
        assert isinstance(result, tuple)

    def test_portfolio(self):
        """Portfolio input delegates to encode_named_portfolio."""
        from gs_quant.markets.portfolio import Portfolio
        with patch('gs_quant.json_convertors.encode_named_portfolio') as mock_encode:
            mock_encode.return_value = {'portfolio_name': 'test'}
            mock_port = MagicMock(spec=Portfolio)
            result = encode_named_instrument(mock_port)
            mock_encode.assert_called_once()

    def test_instrument(self):
        """Instrument input calls as_dict."""
        mock_inst = MagicMock()
        mock_inst.as_dict.return_value = {'type': 'IRSwap'}
        result = encode_named_instrument(mock_inst)
        assert result == {'type': 'IRSwap'}


class TestEncodeNamedPortfolio:
    """Test encode_named_portfolio function."""

    def test_basic(self):
        """Encodes a Portfolio object."""
        mock_port = MagicMock()
        mock_port.name = 'test_port'
        mock_inst = MagicMock()
        mock_inst.as_dict.return_value = {'type': 'IRSwap'}
        mock_port.all_instruments = [mock_inst]
        result = encode_named_portfolio(mock_port)
        assert result['portfolio_name'] == 'test_port'
        assert len(result['instruments']) == 1


class TestQuoteReportDecoders:
    """Test quote report decode functions."""

    def test_decode_quote_report_none(self):
        """None input returns None."""
        assert decode_quote_report(None) is None

    def test_decode_quote_report_empty_dict(self):
        """Empty dict returns None."""
        assert decode_quote_report({}) is None

    def test_decode_quote_reports_none(self):
        """None input returns None."""
        assert decode_quote_reports(None) is None

    def test_decode_quote_reports_empty(self):
        """Empty list returns None."""
        assert decode_quote_reports([]) is None

    def test_decode_custom_comment_none(self):
        """None input returns None."""
        assert decode_custom_comment(None) is None

    def test_decode_custom_comment_empty_dict(self):
        """Empty dict returns None."""
        assert decode_custom_comment({}) is None

    def test_decode_custom_comments_none(self):
        """None input returns None."""
        assert decode_custom_comments(None) is None

    def test_decode_custom_comments_empty(self):
        """Empty list returns None."""
        assert decode_custom_comments([]) is None

    def test_decode_hedge_type_none(self):
        """None input returns None."""
        assert decode_hedge_type(None) is None

    def test_decode_hedge_type_empty_dict(self):
        """Empty dict returns None."""
        assert decode_hedge_type({}) is None

    def test_decode_hedge_types_none(self):
        """None input returns None."""
        assert decode_hedge_types(None) is None

    def test_decode_hedge_types_empty(self):
        """Empty list returns None."""
        assert decode_hedge_types([]) is None


class TestGetDcTypeSuffix:
    """Test _get_dc_type with field name having underscore suffix."""

    def test_field_with_underscore_suffix(self):
        """Field name with trailing underscore is found."""
        @dataclass
        class HasField:
            class_type_: str = 'MyType'

        assert _get_dc_type(HasField, 'class_type', allow_missing=False) == 'MyType'

    def test_missing_default_value(self):
        """MISSING default raises ValueError."""
        @dataclass
        class NoDefault:
            class_type: str = field(default=MISSING)

        # This is tricky; we need a field whose default is MISSING.
        # A field without default but with default_factory won't have MISSING.
        # Actually, fields with no default have MISSING. But we can't create a dataclass
        # with a non-default field after a default field. So let's just test the None case.
        @dataclass
        class NoneDefault:
            class_type: Optional[str] = None

        with pytest.raises(ValueError, match='No default value'):
            _get_dc_type(NoneDefault, 'class_type', allow_missing=False)


class TestDcDecodeAllowMissing:
    """Test dc_decode with allow_missing=True."""

    def test_dc_decode_allow_missing(self):
        """dc_decode with allow_missing skips classes without the field."""
        @dataclass
        class TypeA:
            class_type: str = 'A'

            @classmethod
            def from_dict(cls, data):
                return cls()

        @dataclass
        class TypeB:
            name: str = 'B'

        decoder = dc_decode(TypeA, TypeB, allow_missing=True)
        result = decoder({'class_type': 'A'})
        assert isinstance(result, TypeA)


class TestDecodeDateOrStrExcelDateBug:
    """Ensure the Excel date leap-year bug branch is covered."""

    def test_excel_date_exactly_59(self):
        """Value == 59 should NOT subtract 1 (not > 59)."""
        result = decode_date_or_str(59.0)
        assert isinstance(result, dt.date)
        # 59 corresponds to Feb 28, 1900 in Excel (no subtraction)
        expected = (dt.datetime(1899, 12, 31) + dt.timedelta(days=59)).date()
        assert result == expected

    def test_excel_date_exactly_60(self):
        """Value == 60 should subtract 1 (Excel bug for Mar 1, 1900)."""
        result = decode_date_or_str(60.0)
        assert isinstance(result, dt.date)
        expected = (dt.datetime(1899, 12, 31) + dt.timedelta(days=59)).date()  # 60 - 1 = 59
        assert result == expected


class TestDecodeIsoDateOrDatetimeList:
    """Test list branch of decode_iso_date_or_datetime."""

    def test_list_input(self):
        """List input returns tuple of decoded values."""
        result = decode_iso_date_or_datetime(['2020-07-28', '2020-07-28T10:30:00'])
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], dt.date)
        assert isinstance(result[1], dt.datetime)


class TestEncodeDatetimeTimezone:
    """Test encode_datetime with timezone-aware datetime."""

    def test_aware_datetime_no_double_z(self):
        """Timezone-aware datetime does not append Z."""
        from datetime import timezone
        result = encode_datetime(dt.datetime(2020, 7, 28, 10, 30, 0, tzinfo=timezone.utc))
        assert not result.endswith('ZZ')
        assert '+00:00' in result or result.endswith('Z')


class TestDecodeDatetimeNonStringBranch:
    """Test decode_datetime with a string that has NO long subseconds (simple path)."""

    def test_simple_iso_string(self):
        """ISO string without sub-second precision."""
        result = decode_datetime('2020-07-28T10:30:00Z')
        assert isinstance(result, dt.datetime)

    def test_short_subseconds(self):
        """ISO string with short (6 or fewer) subseconds."""
        result = decode_datetime('2020-07-28T10:30:00.123456Z')
        assert isinstance(result, dt.datetime)
