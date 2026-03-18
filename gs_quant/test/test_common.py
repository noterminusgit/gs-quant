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

import pytest
from unittest.mock import MagicMock

from gs_quant.common import (
    PayReceive,
    RiskMeasure,
    RiskMeasureType,
    ParameterisedRiskMeasure,
    PositionType,
    DateLimit,
    CurrencyParameter,
    DoubleParameter,
)
from gs_quant.base import EnumBase, RiskMeasureParameter
from gs_quant.target.common import PayReceive as _PayReceive


def _make_param():
    """Create a CurrencyParameter as a concrete RiskMeasureParameter with data."""
    return CurrencyParameter(value='USD')


def _make_param_different():
    """Create a DoubleParameter for type comparison tests."""
    return DoubleParameter(value=1.0)


class TestPayReceive:
    def test_basic_values(self):
        assert PayReceive.Pay.value == 'Pay'
        assert PayReceive.Receive.value == 'Rec'
        assert PayReceive.Straddle.value == 'Straddle'

    def test_missing_with_target_pay_receive(self):
        result = PayReceive(_PayReceive.Receive)
        assert result == PayReceive.Receive

    def test_missing_with_receive_string(self):
        result = PayReceive('Receive')
        assert result == PayReceive.Receive

    def test_missing_with_receiver_string(self):
        result = PayReceive('Receiver')
        assert result == PayReceive.Receive


class TestRiskMeasure:
    def test_lt_different_names(self):
        rm1 = RiskMeasure(name='AAA', measure_type=RiskMeasureType.Dollar_Price)
        rm2 = RiskMeasure(name='BBB', measure_type=RiskMeasureType.Dollar_Price)
        assert rm1 < rm2
        assert not rm2 < rm1

    def test_lt_same_name_self_has_params_other_none(self):
        param = _make_param()
        rm1 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        rm1.parameters = param
        rm2 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        rm2.parameters = None
        # self has params, other doesn't -> return False
        assert not (rm1 < rm2)

    def test_lt_same_name_self_none_other_has_params(self):
        param = _make_param()
        rm1 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        rm1.parameters = None
        rm2 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        rm2.parameters = param
        # self.parameters is None, other has params -> return True
        assert rm1 < rm2

    def test_lt_same_name_both_params_same_type(self):
        param1 = _make_param()
        param2 = _make_param()
        rm1 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        rm1.parameters = param1
        rm2 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        rm2.parameters = param2
        # Same type -> uses repr comparison
        result = rm1 < rm2
        assert isinstance(result, bool)

    def test_lt_same_name_both_params_different_type(self):
        param1 = _make_param()
        param2 = _make_param_different()
        rm1 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        rm1.parameters = param1
        rm2 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        rm2.parameters = param2
        # Different types -> compare parameter_type
        result = rm1 < rm2
        assert isinstance(result, bool)

    def test_lt_same_name_both_none_params(self):
        rm1 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        rm2 = RiskMeasure(name='Test', measure_type=RiskMeasureType.Dollar_Price)
        assert not (rm1 < rm2)


class TestParameterisedRiskMeasure:
    def test_with_valid_parameter(self):
        param = _make_param()
        prm = ParameterisedRiskMeasure(
            measure_type=RiskMeasureType.Dollar_Price,
            name='Test',
            parameters=param,
        )
        assert prm.parameters is param

    def test_with_invalid_parameter_raises(self):
        with pytest.raises(TypeError, match='Unsupported parameter'):
            ParameterisedRiskMeasure(
                measure_type=RiskMeasureType.Dollar_Price,
                name='Test',
                parameters='not-a-parameter',
            )

    def test_parameter_is_empty(self):
        prm = ParameterisedRiskMeasure(
            measure_type=RiskMeasureType.Dollar_Price,
            name='Test',
        )
        assert prm.parameter_is_empty()

    def test_repr_without_params(self):
        prm = ParameterisedRiskMeasure(
            measure_type=RiskMeasureType.Dollar_Price,
            name='DollarPrice',
        )
        assert repr(prm) == 'DollarPrice'

    def test_repr_with_params(self):
        param = _make_param()
        prm = ParameterisedRiskMeasure(
            measure_type=RiskMeasureType.Dollar_Price,
            name='Test',
            parameters=param,
        )
        result = repr(prm)
        assert 'Test(' in result
        assert 'value:USD' in result


class TestPositionType:
    def test_values(self):
        assert PositionType.OPEN.value == 'open'
        assert PositionType.CLOSE.value == 'close'
        assert PositionType.ANY.value == 'any'


class TestDateLimit:
    def test_low_limit(self):
        import datetime as dt
        assert DateLimit.LOW_LIMIT.value == dt.date(1952, 1, 1)
