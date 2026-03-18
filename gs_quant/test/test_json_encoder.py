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
import json
from enum import Enum
from unittest.mock import MagicMock

import pandas as pd

from gs_quant.json_encoder import encode_default, JSONEncoder


class TestEncodeDefault:
    def test_datetime(self):
        val = dt.datetime(2020, 1, 15, 10, 30, 0)
        result = encode_default(val)
        assert isinstance(result, str)
        assert '2020-01-15' in result

    def test_date(self):
        val = dt.date(2020, 1, 15)
        result = encode_default(val)
        assert result == '2020-01-15'

    def test_time(self):
        val = dt.time(10, 30, 0)
        result = encode_default(val)
        assert result == '10:30:00.000'

    def test_enum(self):
        class Color(Enum):
            RED = 'red'

        result = encode_default(Color.RED)
        assert result == 'red'

    def test_base_object(self):
        from gs_quant.base import Base, Market
        # Base/Market objects have to_dict called on them by encode_default
        # Create a mock that passes isinstance check and has to_dict
        mock_obj = MagicMock(spec=Base)
        mock_obj.to_dict = MagicMock(return_value={'key': 'value'})
        result = encode_default(mock_obj)
        assert result == {'key': 'value'}

    def test_dataframe(self):
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        result = encode_default(df)
        assert isinstance(result, str)  # DataFrame.to_json() returns a string

    def test_unknown_type_returns_none(self):
        result = encode_default(object())
        assert result is None


class TestJSONEncoder:
    def test_encode_date(self):
        data = {'date': dt.date(2020, 1, 15)}
        result = json.dumps(data, cls=JSONEncoder)
        assert '2020-01-15' in result

    def test_encode_unknown_raises(self):
        import pytest
        with pytest.raises(TypeError):
            json.dumps({'obj': object()}, cls=JSONEncoder)
