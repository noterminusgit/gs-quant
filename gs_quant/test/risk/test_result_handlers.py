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
import math
from unittest.mock import MagicMock

import pytest

from gs_quant.base import InstrumentBase, RiskKey
from gs_quant.common import AssetClass, RiskMeasure, RiskMeasureType
from gs_quant.risk.core import (
    DataFrameWithInfo,
    DictWithInfo,
    ErrorValue,
    FloatWithInfo,
    MQVSValidatorDefnsWithInfo,
    SeriesWithInfo,
    StringWithInfo,
    UnsupportedValue,
)
from gs_quant.risk.measures import PnlExplain
from gs_quant.risk.result_handlers import (
    canonical_projection_table_handler,
    cashflows_handler,
    dict_risk_handler,
    error_handler,
    fixing_table_handler,
    map_coordinate_to_column,
    market_handler,
    mdapi_second_order_table_handler,
    mdapi_table_handler,
    message_handler,
    mmapi_pca_hedge_table_handler,
    mmapi_pca_table_handler,
    mmapi_table_handler,
    mqvs_validators_handler,
    number_and_unit_handler,
    required_assets_handler,
    risk_by_class_handler,
    risk_float_handler,
    risk_handler,
    risk_vector_handler,
    simple_valtable_handler,
    unsupported_handler,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_risk_key(name='DollarPrice', measure_type=RiskMeasureType.Dollar_Price,
                   asset_class=None, risk_measure=None):
    mock_market = MagicMock()
    mock_market.location = 'NYC'
    if risk_measure is None:
        risk_measure = RiskMeasure(name=name, measure_type=measure_type, asset_class=asset_class)
    return RiskKey('GS', dt.date(2020, 1, 1), mock_market, MagicMock(), MagicMock(), risk_measure)


def _mock_instrument():
    return MagicMock(spec=InstrumentBase)


# ---------------------------------------------------------------------------
# error_handler
# ---------------------------------------------------------------------------

class TestErrorHandler:
    def test_with_error_string(self):
        result = error_handler({'errorString': 'calc failed'}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, ErrorValue)
        assert 'calc failed' in result.error

    def test_without_error_string(self):
        result = error_handler({}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, ErrorValue)
        assert 'Unknown error' in result.error

    def test_with_request_id(self):
        result = error_handler({'errorString': 'err'}, _make_risk_key(), _mock_instrument(), request_id='req-123')
        assert isinstance(result, ErrorValue)
        assert 'req-123' in result.error
        assert result.request_id == 'req-123'

    def test_without_request_id(self):
        result = error_handler({'errorString': 'err'}, _make_risk_key(), _mock_instrument())
        assert 'request Id' not in result.error


# ---------------------------------------------------------------------------
# message_handler
# ---------------------------------------------------------------------------

class TestMessageHandler:
    def test_with_message(self):
        result = message_handler({'message': 'Success'}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, StringWithInfo)
        assert str(result) == 'Success'

    def test_without_message(self):
        result = message_handler({}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, ErrorValue)
        assert 'No result returned' in result.error

    def test_with_request_id(self):
        result = message_handler({'message': 'ok'}, _make_risk_key(), _mock_instrument(), request_id='r1')
        assert result.request_id == 'r1'


# ---------------------------------------------------------------------------
# number_and_unit_handler
# ---------------------------------------------------------------------------

class TestNumberAndUnitHandler:
    def test_with_value_and_unit(self):
        result = number_and_unit_handler({'value': 42.0, 'unit': {'name': 'USD'}},
                                         _make_risk_key(), _mock_instrument())
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 42.0
        assert result.unit == {'name': 'USD'}

    def test_missing_value(self):
        result = number_and_unit_handler({}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, FloatWithInfo)
        assert math.isnan(float(result))

    def test_with_request_id(self):
        result = number_and_unit_handler({'value': 1.0}, _make_risk_key(), _mock_instrument(), request_id='r2')
        assert result.request_id == 'r2'


# ---------------------------------------------------------------------------
# risk_handler
# ---------------------------------------------------------------------------

class TestRiskHandler:
    def test_scalar_no_children(self):
        result = risk_handler({'val': 100.0, 'unit': {'name': 'USD'}}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 100.0
        assert result.unit == {'name': 'USD'}

    def test_scalar_no_val_no_children(self):
        result = risk_handler({}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, FloatWithInfo)
        assert math.isnan(float(result))

    def test_with_children_and_parent_val(self):
        result = risk_handler(
            {'val': 50.0, 'children': {'leg1': 30.0, 'leg2': 20.0}},
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 3  # parent + 2 legs
        assert 'path' in result.columns
        assert 'value' in result.columns

    def test_with_children_no_parent_val(self):
        result = risk_handler(
            {'children': {'leg1': 30.0, 'leg2': 20.0}},
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 2  # only 2 legs, no parent


# ---------------------------------------------------------------------------
# risk_by_class_handler
# ---------------------------------------------------------------------------

class TestRiskByClassHandler:
    def test_external_risk_by_class_single_type(self):
        """When name is in external_risk_by_class_val, len(types)<=2, and all types are same."""
        rk = _make_risk_key(name='IRDeltaParallel')
        result = risk_by_class_handler(
            {
                'classes': [{'type': 'IR', 'asset': 'USD'}, {'type': 'IR', 'asset': 'EUR'}],
                'values': [10.0, 20.0],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 30.0

    def test_external_risk_by_class_no_values_key(self):
        """When values key is missing, defaults to (nan,)."""
        rk = _make_risk_key(name='IRBasisParallel')
        result = risk_by_class_handler(
            {
                'classes': [{'type': 'IR', 'asset': 'USD'}],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, FloatWithInfo)
        assert math.isnan(float(result))

    def test_more_than_two_types_goes_to_else(self):
        """When len(types) > 2, goes to else branch."""
        rk = _make_risk_key(name='IRDeltaParallel')
        result = risk_by_class_handler(
            {
                'classes': [
                    {'type': 'IR', 'asset': 'USD'},
                    {'type': 'FX', 'asset': 'EUR'},
                    {'type': 'EQ', 'asset': 'SPX'},
                ],
                'values': [10.0, 20.0, 30.0],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)

    def test_non_external_measure_goes_to_else(self):
        """When name is not in external_risk_by_class_val."""
        rk = _make_risk_key(name='SomethingElse')
        result = risk_by_class_handler(
            {
                'classes': [{'type': 'IR', 'asset': 'USD'}],
                'values': [10.0],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)

    def test_spike_jump_skipped_with_crosses(self):
        """SPIKE/JUMP types are skipped and their values added to CROSSES."""
        rk = _make_risk_key(name='SomeMeasure')
        result = risk_by_class_handler(
            {
                'classes': [
                    {'type': 'IR', 'asset': 'USD'},
                    {'type': 'FX_SPIKE', 'asset': 'EUR'},
                    {'type': 'CROSSES', 'asset': 'XXX', 'value': 0},
                    {'type': 'EQ_JUMP', 'asset': 'SPX'},
                ],
                'values': [10.0, 5.0, 100.0, 3.0],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        # SPIKE and JUMP rows should be removed; only IR and CROSSES remain
        assert len(result) == 2

    def test_spike_without_crosses(self):
        """SPIKE type skipped but no CROSSES row to absorb value."""
        rk = _make_risk_key(name='SomeMeasure')
        result = risk_by_class_handler(
            {
                'classes': [
                    {'type': 'IR', 'asset': 'USD'},
                    {'type': 'FX_SPIKE', 'asset': 'EUR'},
                ],
                'values': [10.0, 5.0],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1  # only IR

    def test_pnl_explain_measure(self):
        """PnlExplain uses __dataframe_handler_unsorted."""
        mock_market = MagicMock()
        pnl_measure = PnlExplain(to_market=mock_market)
        rk = _make_risk_key(risk_measure=pnl_measure)
        result = risk_by_class_handler(
            {
                'classes': [
                    {'type': 'IR', 'asset': 'USD'},
                    {'type': 'FX', 'asset': 'EUR'},
                    {'type': 'EQ', 'asset': 'SPX'},
                ],
                'values': [10.0, 20.0, 30.0],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)

    def test_two_different_types_goes_to_else(self):
        """len(types)<=2 but types are not all the same => goes to else."""
        rk = _make_risk_key(name='IRDeltaParallel')
        result = risk_by_class_handler(
            {
                'classes': [{'type': 'IR', 'asset': 'USD'}, {'type': 'FX', 'asset': 'EUR'}],
                'values': [10.0, 20.0],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)


# ---------------------------------------------------------------------------
# risk_vector_handler
# ---------------------------------------------------------------------------

class TestRiskVectorHandler:
    def test_single_equity_asset(self):
        rk = _make_risk_key(name='EqDelta')
        result = risk_vector_handler(
            {
                'asset': [42.0],
                'points': [{'type': 'EQ', 'asset': 'SPX', 'class_': 'Eq', 'point': '1Y', 'quoteStyle': 'abs'}],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 42.0

    def test_multi_point(self):
        rk = _make_risk_key(name='IRDelta')
        result = risk_vector_handler(
            {
                'asset': [10.0, 20.0],
                'points': [
                    {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'},
                    {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '2Y', 'quoteStyle': 'abs'},
                ],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 2

    def test_single_non_equity(self):
        """Single asset but name does not start with 'Eq' -> goes to dataframe path."""
        rk = _make_risk_key(name='IRDelta')
        result = risk_vector_handler(
            {
                'asset': [10.0],
                'points': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# cashflows_handler
# ---------------------------------------------------------------------------

class TestCashflowsHandler:
    def test_normal(self):
        result = cashflows_handler(
            {
                'cashflows': [
                    {
                        'currency': 'USD',
                        'payDate': '2020-06-15',
                        'setDate': '2020-06-13',
                        'accStart': '2020-01-15',
                        'accEnd': '2020-06-15',
                        'payAmount': 1000.0,
                        'notional': 100000.0,
                        'paymentType': 'Fixed',
                        'index': 'LIBOR',
                        'indexTerm': '3m',
                        'dayCountFraction': 'ACT/360',
                        'spread': 0.01,
                        'rate': 0.025,
                        'discountFactor': 0.99,
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1
        assert 'payment_date' in result.columns
        assert result['payment_date'].iloc[0] == dt.date(2020, 6, 15)

    def test_empty_cashflows(self):
        result = cashflows_handler({'cashflows': []}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# fixing_table_handler
# ---------------------------------------------------------------------------

class TestFixingTableHandler:
    def test_normal(self):
        result = fixing_table_handler(
            {
                'fixingTableRows': [
                    {'fixingDate': '2020-01-15', 'fixing': 1.5},
                    {'fixingDate': '2020-02-15', 'fixing': 1.6},
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, SeriesWithInfo)
        assert len(result) == 2
        assert result.index[0] == dt.date(2020, 1, 15)


# ---------------------------------------------------------------------------
# required_assets_handler
# ---------------------------------------------------------------------------

class TestRequiredAssetsHandler:
    def test_normal(self):
        result = required_assets_handler(
            {'requiredAssets': [{'type': 'IR', 'asset': 'USD'}]},
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1

    def test_empty(self):
        result = required_assets_handler(
            {'requiredAssets': []},
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# dict_risk_handler
# ---------------------------------------------------------------------------

class TestDictRiskHandler:
    def test_normal(self):
        data = {'key1': 'value1', 'key2': 42}
        result = dict_risk_handler(data, _make_risk_key(), _mock_instrument())
        assert isinstance(result, DictWithInfo)
        assert result['key1'] == 'value1'


# ---------------------------------------------------------------------------
# simple_valtable_handler
# ---------------------------------------------------------------------------

class TestSimpleValtableHandler:
    def test_normal(self):
        result = simple_valtable_handler(
            {
                'rows': [
                    {
                        'label': 'Price',
                        'value': {'$type': 'NumberAndUnit', 'value': 100.0, 'unit': {'name': 'USD'}},
                    },
                    {
                        'label': 'Delta',
                        'value': {'$type': 'NumberAndUnit', 'value': 0.5, 'unit': {'name': 'USD'}},
                    },
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 2
        assert list(result.columns) == ['label', 'value']


# ---------------------------------------------------------------------------
# canonical_projection_table_handler
# ---------------------------------------------------------------------------

class TestCanonicalProjectionTableHandler:
    def test_normal(self):
        result = canonical_projection_table_handler(
            {
                'rows': [
                    {
                        'assetClass': 'Rates',
                        'asset': 'USD',
                        'assetFamily': 'Swap',
                        'assetSubFamily': 'Plain',
                        'product': 'IRS',
                        'productFamily': 'Swap',
                        'productSubFamily': 'Vanilla',
                        'side': 'Pay',
                        'size': 1000000,
                        'sizeUnit': 'Notional',
                        'quoteLevel': 0.025,
                        'quoteUnit': 'bp',
                        'startDate': '2020-01-15',
                        'endDate': '2025-01-15',
                        'expiryDate': '2020-01-14',
                        'strike': 0.025,
                        'strikeUnit': 'bp',
                        'optionType': 'Call',
                        'optionStyle': 'European',
                        'tenor': '5Y',
                        'tenorUnit': 'Years',
                        'premiumCcy': 'USD',
                        'currency': 'USD',
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1
        assert result['start_date'].iloc[0] == dt.date(2020, 1, 15)

    def test_empty(self):
        result = canonical_projection_table_handler(
            {'rows': []}, _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# mdapi_table_handler
# ---------------------------------------------------------------------------

class TestMdapiTableHandler:
    def test_normal_with_list_point(self):
        result = mdapi_table_handler(
            {
                'rows': [
                    {
                        'coordinate': {
                            'type': 'IR',
                            'asset': 'USD',
                            'assetClass': 'Swap',
                            'point': ['1Y', '2Y'],
                            'quotingStyle': 'abs',
                        },
                        'value': 0.025,
                        'permissions': 'read',
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1

    def test_string_point(self):
        result = mdapi_table_handler(
            {
                'rows': [
                    {
                        'coordinate': {
                            'type': 'IR',
                            'asset': 'USD',
                            'assetClass': 'Swap',
                            'point': '1Y',
                            'quotingStyle': 'abs',
                        },
                        'value': 0.03,
                        'permissions': 'write',
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1

    def test_missing_value(self):
        result = mdapi_table_handler(
            {
                'rows': [
                    {
                        'coordinate': {
                            'type': 'IR',
                            'asset': 'USD',
                            'assetClass': 'Swap',
                            'point': '1Y',
                            'quotingStyle': 'abs',
                        },
                        'permissions': 'read',
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)


# ---------------------------------------------------------------------------
# mdapi_second_order_table_handler
# ---------------------------------------------------------------------------

class TestMdapiSecondOrderTableHandler:
    def test_single_row_rates_parallel_gamma(self):
        """Single row with Rates ParallelGamma -> delegates to risk_float_handler."""
        rk = _make_risk_key(
            name='ParallelGamma',
            measure_type=RiskMeasureType.ParallelGamma,
            asset_class=AssetClass.Rates,
        )
        result = mdapi_second_order_table_handler(
            {
                'values': [42.0],
                'innerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}],
                'outerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '2Y', 'quoteStyle': 'abs'}],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 42.0

    def test_single_row_rates_parallel_gamma_local_ccy(self):
        """Single row with Rates ParallelGammaLocalCcy -> delegates to risk_float_handler."""
        rk = _make_risk_key(
            name='ParallelGammaLocalCcy',
            measure_type=RiskMeasureType.ParallelGammaLocalCcy,
            asset_class=AssetClass.Rates,
        )
        result = mdapi_second_order_table_handler(
            {
                'values': [99.0],
                'innerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}],
                'outerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '2Y', 'quoteStyle': 'abs'}],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, FloatWithInfo)
        assert float(result) == 99.0

    def test_multi_row(self):
        """Multiple rows -> returns DataFrameWithInfo."""
        rk = _make_risk_key(
            name='ParallelGamma',
            measure_type=RiskMeasureType.ParallelGamma,
            asset_class=AssetClass.Rates,
        )
        result = mdapi_second_order_table_handler(
            {
                'values': [1.0, 2.0],
                'innerPoints': [
                    {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'},
                    {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '2Y', 'quoteStyle': 'abs'},
                ],
                'outerPoints': [
                    {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'},
                    {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '2Y', 'quoteStyle': 'abs'},
                ],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 2

    def test_non_rates_single_row(self):
        """Single row but not Rates asset class -> goes to DataFrame path."""
        rk = _make_risk_key(
            name='ParallelGamma',
            measure_type=RiskMeasureType.ParallelGamma,
            asset_class=AssetClass.Equity,
        )
        result = mdapi_second_order_table_handler(
            {
                'values': [42.0],
                'innerPoints': [{'type': 'EQ', 'asset': 'SPX', 'class_': 'Eq', 'point': '1Y', 'quoteStyle': 'abs'}],
                'outerPoints': [{'type': 'EQ', 'asset': 'SPX', 'class_': 'Eq', 'point': '1Y', 'quoteStyle': 'abs'}],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)

    def test_mismatched_inner_outer_raises(self):
        """When innerPoints and outerPoints have different lengths, raise."""
        rk = _make_risk_key()
        with pytest.raises(Exception, match="inner and outer points of different size"):
            mdapi_second_order_table_handler(
                {
                    'values': [1.0],
                    'innerPoints': [
                        {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'},
                    ],
                    'outerPoints': [
                        {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'},
                        {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '2Y', 'quoteStyle': 'abs'},
                    ],
                },
                rk, _mock_instrument()
            )

    def test_point_as_list(self):
        """When point is a list, it gets joined by semicolons in map_coordinate_to_column."""
        rk = _make_risk_key()
        result = mdapi_second_order_table_handler(
            {
                'values': [1.0],
                'innerPoints': [
                    {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': ['1Y', '2Y'], 'quoteStyle': 'abs'},
                ],
                'outerPoints': [
                    {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': ['3Y'], 'quoteStyle': 'abs'},
                ],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)


# ---------------------------------------------------------------------------
# mmapi_table_handler
# ---------------------------------------------------------------------------

class TestMmapiTableHandler:
    def test_normal(self):
        result = mmapi_table_handler(
            {
                'rows': [
                    {
                        'modelCoordinate': {
                            'type': 'IR',
                            'asset': 'USD',
                            'point': ['1Y', '2Y'],
                            'tags': ['tag1', 'tag2'],
                            'quotingStyle': 'abs',
                        },
                        'value': {
                            'value': [
                                {'date': '2020-01-15', 'value': 0.025},
                                {'date': '2020-02-15', 'value': 0.026},
                            ]
                        },
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1

    def test_string_point_and_tags(self):
        result = mmapi_table_handler(
            {
                'rows': [
                    {
                        'modelCoordinate': {
                            'type': 'IR',
                            'asset': 'USD',
                            'point': '1Y',
                            'tags': 'tag1',
                            'quotingStyle': 'abs',
                        },
                        'value': {
                            'value': [
                                {'date': '2020-01-15', 'value': 0.025},
                            ]
                        },
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)


# ---------------------------------------------------------------------------
# mmapi_pca_table_handler
# ---------------------------------------------------------------------------

class TestMmapiPcaTableHandler:
    def test_normal(self):
        result = mmapi_pca_table_handler(
            {
                'rows': [
                    {
                        'coordinate': {
                            'type': 'IR',
                            'asset': 'USD',
                            'assetClass': 'Swap',
                            'point': ['1Y'],
                            'quotingStyle': 'abs',
                        },
                        'value': 10.0,
                        'layer1': 'L1',
                        'layer2': 'L2',
                        'layer3': 'L3',
                        'layer4': 'L4',
                        'level': 1,
                        'sensitivity': 0.5,
                        'irDelta': 0.01,
                        'endDate': '2025-01-15',
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1

    def test_string_point(self):
        result = mmapi_pca_table_handler(
            {
                'rows': [
                    {
                        'coordinate': {
                            'type': 'IR',
                            'asset': 'USD',
                            'assetClass': 'Swap',
                            'point': '1Y',
                            'quotingStyle': 'abs',
                        },
                        'value': 10.0,
                        'layer1': 'L1',
                        'layer2': 'L2',
                        'layer3': 'L3',
                        'layer4': 'L4',
                        'level': 1,
                        'sensitivity': 0.5,
                        'irDelta': 0.01,
                        'endDate': '2025-01-15',
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)


# ---------------------------------------------------------------------------
# mmapi_pca_hedge_table_handler
# ---------------------------------------------------------------------------

class TestMmapiPcaHedgeTableHandler:
    def test_normal(self):
        result = mmapi_pca_hedge_table_handler(
            {
                'rows': [
                    {
                        'coordinate': {
                            'type': 'IR',
                            'asset': 'USD',
                            'assetClass': 'Swap',
                            'point': ['1Y', '2Y'],
                            'quotingStyle': 'abs',
                        },
                        'size': 1000000,
                        'fixedRate': 0.025,
                        'irDelta': 0.01,
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 1

    def test_string_point(self):
        result = mmapi_pca_hedge_table_handler(
            {
                'rows': [
                    {
                        'coordinate': {
                            'type': 'IR',
                            'asset': 'USD',
                            'assetClass': 'Swap',
                            'point': '1Y',
                            'quotingStyle': 'abs',
                        },
                        'size': 500000,
                        'fixedRate': 0.03,
                        'irDelta': 0.02,
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)


# ---------------------------------------------------------------------------
# mqvs_validators_handler
# ---------------------------------------------------------------------------

class TestMqvsValidatorsHandler:
    def test_normal(self):
        result = mqvs_validators_handler(
            {
                'validators': [
                    {
                        'validatorType': 'Range',
                        'targets': [{'env': 'prod'}],
                        'args': {'min': '0', 'max': '100'},
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, MQVSValidatorDefnsWithInfo)
        assert len(result.validators) == 1
        assert result.validators[0].validatorType == 'Range'


# ---------------------------------------------------------------------------
# market_handler
# ---------------------------------------------------------------------------

class TestMarketHandler:
    def test_normal(self):
        result = market_handler({'marketRef': 'ref-123'}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, StringWithInfo)
        assert str(result) == 'ref-123'

    def test_no_market_ref(self):
        result = market_handler({}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, StringWithInfo)
        assert str(result) == 'None'


# ---------------------------------------------------------------------------
# unsupported_handler
# ---------------------------------------------------------------------------

class TestUnsupportedHandler:
    def test_normal(self):
        result = unsupported_handler({}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, UnsupportedValue)

    def test_with_request_id(self):
        result = unsupported_handler({}, _make_risk_key(), _mock_instrument(), request_id='req-99')
        assert isinstance(result, UnsupportedValue)
        assert result.request_id == 'req-99'


# ---------------------------------------------------------------------------
# risk_float_handler
# ---------------------------------------------------------------------------

class TestRiskFloatHandler:
    def test_normal(self):
        result = risk_float_handler({'values': [3.14]}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, FloatWithInfo)
        assert float(result) == pytest.approx(3.14)

    def test_with_request_id(self):
        result = risk_float_handler({'values': [1.0]}, _make_risk_key(), _mock_instrument(), request_id='r-x')
        assert result.request_id == 'r-x'


# ---------------------------------------------------------------------------
# map_coordinate_to_column
# ---------------------------------------------------------------------------

class TestMapCoordinateToColumn:
    def test_with_list_point(self):
        coord = {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': ['1Y', '2Y'], 'quoteStyle': 'abs'}
        result = map_coordinate_to_column(coord, 'inner')
        assert result['inner_type'] == 'IR'
        assert result['inner_asset'] == 'USD'
        assert result['inner_class_'] == 'Swap'
        assert result['inner_quoteStyle'] == 'abs'
        # 'point' key in updated_struct is 'inner_point', so updated_struct.get('point', '') -> ''
        # The function then sets updated_struct['point'] = '' (the raw_point lookup misses)
        assert result['inner_point'] == ['1Y', '2Y']
        assert result['point'] == ''

    def test_with_string_point(self):
        coord = {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}
        result = map_coordinate_to_column(coord, 'outer')
        # Same as above: the point is under 'outer_point', not 'point'
        assert result['outer_point'] == '1Y'
        assert result['point'] == ''

    def test_no_point_key(self):
        coord = {'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'quoteStyle': 'abs'}
        result = map_coordinate_to_column(coord, 'tag')
        assert result['point'] == ''

    def test_extra_keys_ignored(self):
        coord = {'type': 'IR', 'asset': 'USD', 'extraKey': 'should_be_ignored'}
        result = map_coordinate_to_column(coord, 'x')
        assert 'x_extraKey' not in result
        assert 'x_type' in result
        assert 'x_asset' in result


# ---------------------------------------------------------------------------
# __is_single_row_2nd_order_risk (tested indirectly via mdapi_second_order_table_handler)
# ---------------------------------------------------------------------------

class TestIsSingleRow2ndOrderRisk:
    def test_none_risk_key(self):
        """When risk_key is None, should not be treated as single row 2nd order."""
        # We cannot pass None as risk_key directly because the handler accesses
        # risk_key.risk_measure. Instead, test the False branches:
        # non-RiskMeasure type
        rk = _make_risk_key()
        rk_with_non_rm = RiskKey('GS', dt.date(2020, 1, 1), MagicMock(), MagicMock(), MagicMock(), 'not_a_risk_measure')
        # single row but risk_measure is not RiskMeasure instance -> goes to dataframe path
        result = mdapi_second_order_table_handler(
            {
                'values': [42.0],
                'innerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}],
                'outerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}],
            },
            rk_with_non_rm, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)

    def test_non_parallel_gamma_measure(self):
        """RiskMeasure with non-ParallelGamma measure_type -> not single row."""
        rk = _make_risk_key(
            name='DollarPrice',
            measure_type=RiskMeasureType.Dollar_Price,
            asset_class=AssetClass.Rates,
        )
        result = mdapi_second_order_table_handler(
            {
                'values': [42.0],
                'innerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}],
                'outerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)

    def test_no_asset_class(self):
        """ParallelGamma but asset_class is None -> not single row."""
        rk = _make_risk_key(
            name='ParallelGamma',
            measure_type=RiskMeasureType.ParallelGamma,
            asset_class=None,
        )
        result = mdapi_second_order_table_handler(
            {
                'values': [42.0],
                'innerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}],
                'outerPoints': [{'type': 'IR', 'asset': 'USD', 'class_': 'Swap', 'point': '1Y', 'quoteStyle': 'abs'}],
            },
            rk, _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)


# ---------------------------------------------------------------------------
# __dataframe_handler edge cases (tested indirectly)
# ---------------------------------------------------------------------------

class TestDataframeHandlerEdgeCases:
    def test_empty_result_required_assets(self):
        """Empty iterator -> returns empty DataFrameWithInfo."""
        result = required_assets_handler({'requiredAssets': []}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 0

    def test_with_request_id(self):
        result = required_assets_handler(
            {'requiredAssets': [{'type': 'IR', 'asset': 'USD'}]},
            _make_risk_key(), _mock_instrument(), request_id='rq-1'
        )
        assert result.request_id == 'rq-1'


# ---------------------------------------------------------------------------
# __dataframe_handler_unsorted edge cases (tested indirectly)
# ---------------------------------------------------------------------------

class TestDataframeHandlerUnsortedEdgeCases:
    def test_empty_cashflows(self):
        """Empty iterator -> returns empty DataFrameWithInfo."""
        result = cashflows_handler({'cashflows': []}, _make_risk_key(), _mock_instrument())
        assert isinstance(result, DataFrameWithInfo)
        assert len(result) == 0

    def test_none_date_value(self):
        """When a date field is not a string (e.g., None), lambda should pass it through."""
        result = cashflows_handler(
            {
                'cashflows': [
                    {
                        'currency': 'USD',
                        'payDate': None,
                        'setDate': '2020-06-13',
                        'accStart': '2020-01-15',
                        'accEnd': None,
                        'payAmount': 1000.0,
                        'notional': 100000.0,
                        'paymentType': 'Fixed',
                        'index': 'LIBOR',
                        'indexTerm': '3m',
                        'dayCountFraction': 'ACT/360',
                        'spread': 0.01,
                        'rate': 0.025,
                        'discountFactor': 0.99,
                    }
                ]
            },
            _make_risk_key(), _mock_instrument()
        )
        assert isinstance(result, DataFrameWithInfo)
        assert result['payment_date'].iloc[0] is None
        assert result['accrual_end_date'].iloc[0] is None

    def test_canonical_with_request_id(self):
        result = canonical_projection_table_handler(
            {
                'rows': [
                    {
                        'assetClass': 'Rates',
                        'asset': 'USD',
                        'assetFamily': 'Swap',
                        'assetSubFamily': 'Plain',
                        'product': 'IRS',
                        'productFamily': 'Swap',
                        'productSubFamily': 'Vanilla',
                        'side': 'Pay',
                        'size': 1000000,
                        'sizeUnit': 'Notional',
                        'quoteLevel': 0.025,
                        'quoteUnit': 'bp',
                        'startDate': '2020-01-15',
                        'endDate': '2025-01-15',
                        'expiryDate': '2020-01-14',
                        'strike': 0.025,
                        'strikeUnit': 'bp',
                        'optionType': 'Call',
                        'optionStyle': 'European',
                        'tenor': '5Y',
                        'tenorUnit': 'Years',
                        'premiumCcy': 'USD',
                        'currency': 'USD',
                    }
                ]
            },
            _make_risk_key(), _mock_instrument(), request_id='req-cp'
        )
        assert result.request_id == 'req-cp'
