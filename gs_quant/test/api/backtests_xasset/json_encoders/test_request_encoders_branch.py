"""
Branch coverage tests for gs_quant/api/gs/backtests_xasset/json_encoders/request_encoders.py
"""

from unittest.mock import MagicMock, patch

import pytest

from gs_quant.api.gs.backtests_xasset.json_encoders.request_encoders import (
    encode_request_object,
    legs_decoder,
    legs_encoder,
    enum_decode,
)
from gs_quant.base import EnumBase
from gs_quant.common import RiskMeasure


class TestEncodeRequestObject:
    """Test encode_request_object function."""

    def test_risk_measure(self):
        """RiskMeasure is encoded via encode_risk_measure."""
        rm = MagicMock(spec=RiskMeasure)
        with patch('gs_quant.api.gs.backtests_xasset.json_encoders.request_encoders.encode_risk_measure') as mock_enc:
            mock_enc.return_value = {'name': 'DollarPrice'}
            result = encode_request_object(rm)
            mock_enc.assert_called_once_with(rm)
            assert result == {'name': 'DollarPrice'}

    def test_instrument(self):
        """Instrument is encoded via to_dict."""
        from gs_quant.instrument import Instrument
        mock_inst = MagicMock(spec=Instrument)
        mock_inst.to_dict.return_value = {'type': 'IRSwap'}
        result = encode_request_object(mock_inst)
        assert result == {'type': 'IRSwap'}

    def test_tuple(self):
        """Tuple input recurses."""
        mock_inst = MagicMock()
        mock_inst.to_dict.return_value = {'type': 'IRSwap'}
        # The tuple branch: not RiskMeasure, not Instrument
        from gs_quant.instrument import Instrument
        mock_inst2 = MagicMock(spec=Instrument)
        mock_inst2.to_dict.return_value = {'type': 'IRCap'}
        result = encode_request_object((mock_inst2,))
        assert isinstance(result, tuple)

    def test_other_type_returns_none(self):
        """Non-matching type returns None (falls through)."""
        result = encode_request_object('some_string')
        assert result is None


class TestLegsDecoder:
    """Test legs_decoder function."""

    def test_none(self):
        assert legs_decoder(None) is None

    def test_basic_decoding(self):
        """Decodes instruments and assigns names."""
        mock_inst1 = MagicMock()
        mock_inst1.name = None
        mock_inst2 = MagicMock()
        mock_inst2.name = 'custom_name'

        with patch('gs_quant.api.gs.backtests_xasset.json_encoders.request_encoders.Instrument') as MockInst:
            MockInst.from_dict.side_effect = [mock_inst1, mock_inst2]
            result = legs_decoder([{'type': 'IRSwap'}, {'type': 'IRCap'}])
            assert len(result) == 2
            # mock_inst1 should get name 'leg_0'
            assert mock_inst1.name == 'leg_0'

    def test_name_collision(self):
        """When default name collides, index is incremented."""
        mock_inst1 = MagicMock()
        mock_inst1.name = 'leg_0'  # Pre-existing name
        mock_inst2 = MagicMock()
        mock_inst2.name = None

        with patch('gs_quant.api.gs.backtests_xasset.json_encoders.request_encoders.Instrument') as MockInst:
            MockInst.from_dict.side_effect = [mock_inst1, mock_inst2]
            result = legs_decoder([{'type': 'IRSwap'}, {'type': 'IRCap'}])
            assert len(result) == 2
            # mock_inst2 should skip 'leg_0' (already taken) and get 'leg_1'
            assert mock_inst2.name == 'leg_1'

    def test_all_named(self):
        """When all instruments have names, no names are assigned."""
        mock_inst1 = MagicMock()
        mock_inst1.name = 'a'
        mock_inst2 = MagicMock()
        mock_inst2.name = 'b'

        with patch('gs_quant.api.gs.backtests_xasset.json_encoders.request_encoders.Instrument') as MockInst:
            MockInst.from_dict.side_effect = [mock_inst1, mock_inst2]
            result = legs_decoder([{'type': 'IRSwap'}, {'type': 'IRCap'}])
            assert len(result) == 2
            assert mock_inst1.name == 'a'
            assert mock_inst2.name == 'b'

    def test_multiple_unnamed_no_collision(self):
        """Multiple unnamed instruments get sequential names."""
        mock_inst1 = MagicMock()
        mock_inst1.name = None
        mock_inst2 = MagicMock()
        mock_inst2.name = None
        mock_inst3 = MagicMock()
        mock_inst3.name = None

        with patch('gs_quant.api.gs.backtests_xasset.json_encoders.request_encoders.Instrument') as MockInst:
            MockInst.from_dict.side_effect = [mock_inst1, mock_inst2, mock_inst3]
            result = legs_decoder([{}, {}, {}])
            assert mock_inst1.name == 'leg_0'
            assert mock_inst2.name == 'leg_1'
            assert mock_inst3.name == 'leg_2'


class TestLegsEncoder:
    """Test legs_encoder function."""

    def test_basic(self):
        mock_inst = MagicMock()
        mock_inst.to_dict.return_value = {'type': 'IRSwap'}
        result = legs_encoder([mock_inst])
        assert result == [{'type': 'IRSwap'}]


class TestEnumDecode:
    """Test enum_decode factory."""

    def test_none_value(self):
        """None returns None."""
        from enum import Enum

        class MyEnum(EnumBase, Enum):
            A = 'a'

        decoder = enum_decode(MyEnum)
        assert decoder(None) is None

    def test_null_string(self):
        """'null' string returns None."""
        from enum import Enum

        class MyEnum(EnumBase, Enum):
            A = 'a'

        decoder = enum_decode(MyEnum)
        assert decoder('null') is None

    def test_enum_base_passthrough(self):
        """EnumBase value passes through."""
        from enum import Enum

        class MyEnum(EnumBase, Enum):
            A = 'a'

        decoder = enum_decode(MyEnum)
        result = decoder(MyEnum.A)
        assert result == MyEnum.A

    def test_valid_string(self):
        """Valid string is decoded."""
        from enum import Enum

        class MyEnum(EnumBase, Enum):
            A = 'a'

        decoder = enum_decode(MyEnum)
        result = decoder('a')
        assert result == MyEnum.A

    def test_invalid_string_raises(self):
        """Invalid string raises ValueError."""
        from enum import Enum

        class MyEnum(EnumBase, Enum):
            A = 'a'

        decoder = enum_decode(MyEnum)
        with pytest.raises(ValueError, match='Unable to decode'):
            decoder('invalid')

    def test_non_string_non_enum_raises(self):
        """Non-string, non-EnumBase raises ValueError."""
        from enum import Enum

        class MyEnum(EnumBase, Enum):
            A = 'a'

        decoder = enum_decode(MyEnum)
        with pytest.raises(ValueError, match='Unable to decode'):
            decoder(12345)
