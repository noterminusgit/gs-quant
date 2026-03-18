"""
Copyright 2019 Goldman Sachs.
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

import datetime as dt
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import pytest

from gs_quant.json_convertors import (
    encode_date_or_str,
    decode_optional_date,
    decode_optional_time,
    encode_optional_time,
    decode_date_tuple,
    encode_date_tuple,
    decode_iso_date_or_datetime,
    optional_from_isodatetime,
    optional_to_isodatetime,
    decode_dict_date_key,
    decode_dict_date_key_or_float,
    decode_dict_dict_date_key,
    decode_dict_date_value,
    decode_datetime_tuple,
    decode_date_or_str,
    encode_datetime,
    decode_datetime,
    decode_float_or_str,
    decode_instrument,
    encode_dictable,
    encode_named_dictable,
    encode_pandas_series,
    decode_pandas_series,
    _get_dc_type,
    _value_decoder,
    dc_decode,
)


class TestEncodeDateOrStr:
    def test_date(self):
        assert encode_date_or_str(dt.date(2020, 7, 28)) == '2020-07-28'

    def test_str_passthrough(self):
        assert encode_date_or_str('1m') == '1m'

    def test_none(self):
        assert encode_date_or_str(None) is None


class TestDecodeOptionalDate:
    def test_none(self):
        assert decode_optional_date(None) is None

    def test_already_date(self):
        d = dt.date(2020, 7, 28)
        assert decode_optional_date(d) is d

    def test_iso_format(self):
        assert decode_optional_date('2020-07-28') == dt.date(2020, 7, 28)

    def test_dmy_format(self):
        assert decode_optional_date('28Jul20') == dt.date(2020, 7, 28)

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            decode_optional_date('not-a-date')

    def test_non_string_non_date_raises(self):
        with pytest.raises(ValueError):
            decode_optional_date(12345)


class TestDecodeOptionalTime:
    def test_none(self):
        assert decode_optional_time(None) is None

    def test_already_time(self):
        t = dt.time(10, 30)
        assert decode_optional_time(t) is t

    def test_string(self):
        result = decode_optional_time('10:30:00')
        assert result == dt.time(10, 30, 0)

    def test_non_string_non_time_raises(self):
        with pytest.raises(ValueError):
            decode_optional_time(12345)


class TestEncodeOptionalTime:
    def test_time(self):
        assert encode_optional_time(dt.time(10, 30)) == '10:30:00'

    def test_str_passthrough(self):
        assert encode_optional_time('10:30') == '10:30'

    def test_none(self):
        assert encode_optional_time(None) is None


class TestDecodeDateTuple:
    def test_tuple(self):
        result = decode_date_tuple(('2020-07-28', '2020-07-29'))
        assert result == (dt.date(2020, 7, 28), dt.date(2020, 7, 29))

    def test_list(self):
        result = decode_date_tuple(['2020-07-28'])
        assert result == (dt.date(2020, 7, 28),)

    def test_non_iterable(self):
        assert decode_date_tuple('not-a-tuple') is None


class TestEncodeDateTuple:
    def test_with_dates(self):
        result = encode_date_tuple((dt.date(2020, 7, 28), '1m'))
        assert result == ('2020-07-28', '1m')

    def test_with_none_value(self):
        result = encode_date_tuple((dt.date(2020, 7, 28), None))
        assert result == ('2020-07-28', None)

    def test_none_input(self):
        assert encode_date_tuple(None) is None


class TestDecodeIsoDateOrDatetime:
    def test_tuple(self):
        result = decode_iso_date_or_datetime(('2020-07-28', '2020-07-28T10:30:00'))
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_date_passthrough(self):
        d = dt.date(2020, 7, 28)
        assert decode_iso_date_or_datetime(d) is d

    def test_datetime_passthrough(self):
        d = dt.datetime(2020, 7, 28, 10, 30)
        assert decode_iso_date_or_datetime(d) is d

    def test_date_string(self):
        result = decode_iso_date_or_datetime('2020-07-28')
        assert result == dt.date(2020, 7, 28)

    def test_datetime_string(self):
        result = decode_iso_date_or_datetime('2020-07-28T10:30:00')
        assert isinstance(result, dt.datetime)

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError):
            decode_iso_date_or_datetime(12345)


class TestOptionalFromIsodatetime:
    def test_none(self):
        assert optional_from_isodatetime(None) is None

    def test_datetime_passthrough(self):
        d = dt.datetime(2020, 7, 28, 10, 30)
        assert optional_from_isodatetime(d) is d

    def test_string_with_z(self):
        result = optional_from_isodatetime('2020-07-28T10:30:00Z')
        assert result == dt.datetime(2020, 7, 28, 10, 30, 0)

    def test_string_without_z(self):
        result = optional_from_isodatetime('2020-07-28T10:30:00')
        assert result == dt.datetime(2020, 7, 28, 10, 30, 0)


class TestOptionalToIsodatetime:
    def test_none(self):
        assert optional_to_isodatetime(None) is None

    def test_datetime(self):
        result = optional_to_isodatetime(dt.datetime(2020, 7, 28, 10, 30, 0))
        assert result == '2020-07-28T10:30:00Z'


class TestDecodeDictDateKey:
    def test_valid(self):
        result = decode_dict_date_key({'2020-07-28': 1.0})
        assert result == {dt.date(2020, 7, 28): 1.0}

    def test_none(self):
        assert decode_dict_date_key(None) is None


class TestDecodeDictDateKeyOrFloat:
    def test_none(self):
        assert decode_dict_date_key_or_float(None) is None

    def test_dict(self):
        result = decode_dict_date_key_or_float({'2020-07-28': 1.0})
        assert result == {dt.date(2020, 7, 28): 1.0}

    def test_float(self):
        result = decode_dict_date_key_or_float(1.5)
        assert result == 1.5

    def test_int(self):
        result = decode_dict_date_key_or_float(5)
        assert result == 5.0


class TestDecodeDictDictDateKey:
    def test_valid(self):
        result = decode_dict_dict_date_key({'tenor': {'2020-07-28': 1.0}})
        assert result == {'tenor': {dt.date(2020, 7, 28): 1.0}}

    def test_none(self):
        assert decode_dict_dict_date_key(None) is None

    def test_inner_none(self):
        result = decode_dict_dict_date_key({'tenor': None})
        assert result == {'tenor': None}


class TestDecodeDictDateValue:
    def test_valid(self):
        result = decode_dict_date_value({'key': '2020-07-28'})
        assert result == {'key': dt.date(2020, 7, 28)}

    def test_none(self):
        assert decode_dict_date_value(None) is None


class TestDecodeDatetimeTuple:
    def test_tuple(self):
        result = decode_datetime_tuple(('2020-07-28T10:30:00',))
        assert len(result) == 1
        assert isinstance(result[0], dt.datetime)

    def test_non_iterable(self):
        assert decode_datetime_tuple('not-a-tuple') is None


class TestDecodeDateOrStr:
    def test_none(self):
        assert decode_date_or_str(None) is None

    def test_date_passthrough(self):
        d = dt.date(2020, 7, 28)
        assert decode_date_or_str(d) is d

    def test_float_excel_date(self):
        # Excel date 44040 = 2020-07-28
        result = decode_date_or_str(44040.0)
        assert isinstance(result, dt.date)

    def test_float_before_excel_bug(self):
        # Value <= 59 should not subtract 1
        result = decode_date_or_str(30.0)
        assert isinstance(result, dt.date)

    def test_string_date(self):
        result = decode_date_or_str('2020-07-28')
        assert result == dt.date(2020, 7, 28)

    def test_string_tenor(self):
        result = decode_date_or_str('1m')
        assert result == '1m'

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError):
            decode_date_or_str([1, 2, 3])


class TestEncodeDatetime:
    def test_none(self):
        assert encode_datetime(None) is None

    def test_naive_datetime(self):
        result = encode_datetime(dt.datetime(2020, 7, 28, 10, 30, 0))
        assert result.endswith('Z')
        assert '2020-07-28' in result

    def test_aware_datetime(self):
        from datetime import timezone
        result = encode_datetime(dt.datetime(2020, 7, 28, 10, 30, 0, tzinfo=timezone.utc))
        assert not result.endswith('ZZ')  # Should not double Z

    def test_pandas_timestamp(self):
        ts = pd.Timestamp('2020-07-28T10:30:00')
        result = encode_datetime(ts)
        assert '2020-07-28' in result


class TestDecodeDatetime:
    def test_none(self):
        assert decode_datetime(None) is None

    def test_datetime_passthrough(self):
        d = dt.datetime(2020, 7, 28)
        assert decode_datetime(d) is d

    def test_int_millis(self):
        # 1595894400000 ms = 2020-07-28 00:00:00 UTC
        result = decode_datetime(1595894400000)
        assert isinstance(result, dt.datetime)

    def test_iso_string(self):
        result = decode_datetime('2020-07-28T10:30:00Z')
        assert isinstance(result, dt.datetime)

    def test_string_with_long_subseconds(self):
        result = decode_datetime('2020-07-28T10:30:00.1234567890Z')
        assert isinstance(result, dt.datetime)

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError):
            decode_datetime([1, 2])


class TestDecodeFloatOrStr:
    def test_none(self):
        assert decode_float_or_str(None) is None

    def test_float(self):
        assert decode_float_or_str(1.5) == 1.5

    def test_int(self):
        assert decode_float_or_str(5) == 5.0

    def test_numeric_string(self):
        assert decode_float_or_str('3.14') == 3.14

    def test_non_numeric_string(self):
        assert decode_float_or_str('ATM') == 'ATM'

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError):
            decode_float_or_str([1, 2])


class TestDecodeInstrument:
    def test_none(self):
        assert decode_instrument(None) is None

    def test_empty_dict(self):
        assert decode_instrument({}) is None


class TestEncodeDictable:
    def test_none(self):
        assert encode_dictable(None) is None

    def test_object(self):
        from unittest.mock import MagicMock
        obj = MagicMock()
        obj.to_dict.return_value = {'key': 'value'}
        assert encode_dictable(obj) == {'key': 'value'}


class TestEncodeNamedDictable:
    def test_none(self):
        assert encode_named_dictable(None) is None

    def test_object(self):
        from unittest.mock import MagicMock
        obj = MagicMock()
        obj.to_dict.return_value = {'key': 'value'}
        obj.__class__.__name__ = 'MyType'
        result = encode_named_dictable(obj)
        assert result['type'] == 'MyType'
        assert result['key'] == 'value'


class TestEncodePandasSeries:
    def test_non_date_index(self):
        s = pd.Series({'a': 1.0, 'b': 2.0})
        result = encode_pandas_series(s)
        assert result == {'a': 1.0, 'b': 2.0}

    def test_date_index(self):
        s = pd.Series({dt.date(2020, 7, 28): 1.0, dt.date(2020, 7, 29): 2.0})
        result = encode_pandas_series(s)
        assert '2020-07-28' in result


class TestDecodePandasSeries:
    def test_basic(self):
        result = decode_pandas_series({'2020-07-28': 1.0})
        assert isinstance(result, pd.Series)
        assert len(result) == 1


class TestValueDecoder:
    def test_none_value(self):
        decoder = _value_decoder({})
        assert decoder(None) is None
        assert decoder('null') is None

    def test_list_value(self):
        decoder = _value_decoder({})
        result = decoder([1.0, 2.0])
        assert result == (1.0, 2.0)

    def test_string_with_mapper(self):
        decoder = _value_decoder({}, str_mapper=str.upper)
        assert decoder('hello') == 'HELLO'

    def test_string_without_mapper(self):
        decoder = _value_decoder({})
        assert decoder('hello') == 'hello'

    def test_float(self):
        decoder = _value_decoder({})
        assert decoder(1.5) == 1.5

    def test_int(self):
        decoder = _value_decoder({})
        assert decoder(42) == 42

    def test_date(self):
        decoder = _value_decoder({})
        d = dt.date(2020, 7, 28)
        assert decoder(d) == d

    def test_non_dict_raises(self):
        decoder = _value_decoder({})
        with pytest.raises(TypeError, match='Cannot decode'):
            decoder(object())

    def test_dict_with_explicit_cls(self):
        from unittest.mock import MagicMock
        mock_cls = MagicMock()
        mock_cls.from_dict.return_value = 'decoded'
        decoder = _value_decoder({}, explicit_cls=mock_cls)
        assert decoder({'key': 'value'}) == 'decoded'

    def test_dict_missing_class_type_raises(self):
        decoder = _value_decoder({})
        with pytest.raises(ValueError, match='no "class_type"'):
            decoder({'key': 'value'})

    def test_dict_unknown_class_type_raises(self):
        decoder = _value_decoder({})
        with pytest.raises(ValueError, match='No class mapping'):
            decoder({'class_type': 'Unknown'})

    def test_dict_deserialize_failure(self):
        from unittest.mock import MagicMock
        mock_cls = MagicMock()
        mock_cls.from_dict.side_effect = Exception('bad data')
        decoder = _value_decoder({'MyType': mock_cls})
        with pytest.raises(ValueError, match='Failed to de-serialise'):
            decoder({'class_type': 'MyType'})


class TestGetDcType:
    def test_missing_field_allow_missing(self):
        @dataclass
        class Simple:
            name: str = 'test'
        assert _get_dc_type(Simple, 'class_type', allow_missing=True) is None

    def test_missing_field_not_allowed(self):
        @dataclass
        class Simple:
            name: str = 'test'
        with pytest.raises(ValueError, match='has no "class_type"'):
            _get_dc_type(Simple, 'class_type', allow_missing=False)

    def test_missing_default(self):
        @dataclass
        class HasField:
            class_type: Optional[str] = None
        with pytest.raises(ValueError, match='No default value'):
            _get_dc_type(HasField, 'class_type', allow_missing=False)

    def test_valid_field(self):
        @dataclass
        class HasField:
            class_type: str = 'MyType'
        assert _get_dc_type(HasField, 'class_type', allow_missing=False) == 'MyType'


class TestDcDecode:
    def test_basic(self):
        @dataclass
        class TypeA:
            class_type: str = 'A'
            value: int = 0

            @classmethod
            def from_dict(cls, data):
                return cls(value=data.get('value', 0))

        decoder = dc_decode(TypeA)
        result = decoder({'class_type': 'A', 'value': 42})
        assert isinstance(result, TypeA)
        assert result.value == 42
