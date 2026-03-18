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

from unittest.mock import patch

from gs_quant.json_convertors_common import (
    gsq_rm_for_name,
    encode_risk_measure,
    encode_risk_measure_tuple,
    decode_risk_measure,
    decode_risk_measure_tuple,
    _decode_param,
    _decode_gsq_risk_measure,
)
from gs_quant.common import RiskMeasure, RiskMeasureType, ParameterisedRiskMeasure


class TestGsqRmForName:
    def test_none_name(self):
        assert gsq_rm_for_name(None) is None

    def test_invalid_name(self):
        assert gsq_rm_for_name('NonExistentMeasure') is None

    def test_valid_name(self):
        from gs_quant import risk
        # DollarPrice should be in the risk module
        result = gsq_rm_for_name('DollarPrice')
        assert result is not None


class TestEncodeRiskMeasure:
    def test_without_parameters(self):
        rm = RiskMeasure(measure_type=RiskMeasureType.Dollar_Price, name='DollarPrice')
        result = encode_risk_measure(rm)
        assert isinstance(result, dict)
        assert 'name' in result

    def test_with_parameters(self):
        from gs_quant.common import CurrencyParameter
        param = CurrencyParameter(value='USD')
        rm = RiskMeasure(measure_type=RiskMeasureType.Dollar_Price, name='DollarPrice')
        rm.parameters = param
        result = encode_risk_measure(rm)
        assert 'parameters' in result


class TestEncodeRiskMeasureTuple:
    def test_basic(self):
        rm = RiskMeasure(measure_type=RiskMeasureType.Dollar_Price, name='DollarPrice')
        result = encode_risk_measure_tuple((rm,))
        assert len(result) == 1


class TestDecodeParam:
    def test_no_parameters(self):
        assert _decode_param({}) is None

    def test_parameters_not_dict(self):
        assert _decode_param({'parameters': 'not-a-dict'}) is None

    def test_parameters_no_type(self):
        assert _decode_param({'parameters': {'key': 'value'}}) is None


class TestDecodeGsqRiskMeasure:
    def test_no_name(self):
        assert _decode_gsq_risk_measure({}) is None

    def test_invalid_name(self):
        assert _decode_gsq_risk_measure({'name': 'NonExistent'}) is None

    def test_mismatched_asset_class(self):
        result = _decode_gsq_risk_measure({
            'name': 'DollarPrice',
            'assetClass': 'TOTALLY_WRONG',
            'measureType': 'Dollar_Price'
        })
        # Should return None if asset class doesn't match
        # DollarPrice has no asset_class, so 'TOTALLY_WRONG' != None
        assert result is None

    def test_valid_measure(self):
        from gs_quant import risk
        rm = risk.DollarPrice
        result = _decode_gsq_risk_measure({
            'name': 'DollarPrice',
            'measureType': str(rm.measure_type),
        })
        assert result is not None


class TestDecodeRiskMeasure:
    def test_known_measure(self):
        from gs_quant import risk
        rm = risk.DollarPrice
        result = decode_risk_measure({
            'name': 'DollarPrice',
            'measureType': str(rm.measure_type),
        })
        assert result is not None

    def test_unknown_with_parameters(self):
        result = decode_risk_measure({
            'name': 'UnknownMeasure',
            'measureType': 'Dollar Price',
            'parameters': {'parameterType': 'Currency'},
        })
        assert isinstance(result, ParameterisedRiskMeasure)

    def test_unknown_without_parameters(self):
        result = decode_risk_measure({
            'name': 'UnknownMeasure',
            'measureType': 'Dollar Price',
        })
        assert isinstance(result, RiskMeasure)


class TestDecodeRiskMeasureTuple:
    def test_tuple(self):
        result = decode_risk_measure_tuple(({'name': 'UnknownMeasure', 'measureType': 'Dollar Price'},))
        assert isinstance(result, tuple)
        assert len(result) == 1

    def test_list(self):
        result = decode_risk_measure_tuple([{'name': 'UnknownMeasure', 'measureType': 'Dollar Price'}])
        assert isinstance(result, tuple)

    def test_non_iterable(self):
        assert decode_risk_measure_tuple('not-a-tuple') is None
