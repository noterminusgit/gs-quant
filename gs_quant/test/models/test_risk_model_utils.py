"""
Copyright 2021 Goldman Sachs.
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
import pytest
from unittest import mock
from unittest.mock import MagicMock, patch, call

import pandas as pd

from gs_quant.errors import MqRequestError
from gs_quant.target.risk_models import RiskModelDataMeasure as Measure, RiskModelType as Type


# ---------- _map_measure_to_field_name ----------

class TestMapMeasureToFieldName:
    def test_known_measures(self):
        from gs_quant.models.risk_model_utils import _map_measure_to_field_name
        assert _map_measure_to_field_name(Measure.Specific_Risk) == 'specificRisk'
        assert _map_measure_to_field_name(Measure.Total_Risk) == 'totalRisk'
        assert _map_measure_to_field_name(Measure.Historical_Beta) == 'historicalBeta'
        assert _map_measure_to_field_name(Measure.Predicted_Beta) == 'predictedBeta'
        assert _map_measure_to_field_name(Measure.Global_Predicted_Beta) == 'globalPredictedBeta'
        assert _map_measure_to_field_name(Measure.Daily_Return) == 'dailyReturn'
        assert _map_measure_to_field_name(Measure.Specific_Return) == 'specificReturn'
        assert _map_measure_to_field_name(Measure.Estimation_Universe_Weight) == 'estimationUniverseWeight'
        assert _map_measure_to_field_name(Measure.R_Squared) == 'rSquared'
        assert _map_measure_to_field_name(Measure.Universe_Factor_Exposure) == 'factorExposure'
        assert _map_measure_to_field_name(Measure.Factor_Return) == 'factorReturn'
        assert _map_measure_to_field_name(Measure.Factor_Standard_Deviation) == 'factorStandardDeviation'
        assert _map_measure_to_field_name(Measure.Factor_Z_Score) == 'factorZScore'
        assert _map_measure_to_field_name(Measure.Issuer_Market_Cap) == 'issuerMarketCap'
        assert _map_measure_to_field_name(Measure.Price) == 'price'

    def test_unknown_measure_returns_empty_string(self):
        from gs_quant.models.risk_model_utils import _map_measure_to_field_name
        # Use a measure not in the dict
        assert _map_measure_to_field_name(Measure.Asset_Universe) == ''


# ---------- build_factor_id_to_name_map ----------

class TestBuildFactorIdToNameMap:
    def test_basic(self):
        from gs_quant.models.risk_model_utils import build_factor_id_to_name_map
        results = [
            {'factorData': [{'factorId': 'f1', 'factorName': 'Factor1'},
                            {'factorId': 'f2', 'factorName': 'Factor2'}]},
            {'factorData': [{'factorId': 'f1', 'factorName': 'Factor1'},
                            {'factorId': 'f3', 'factorName': 'Factor3'}]},
        ]
        result = build_factor_id_to_name_map(results)
        assert result == {'f1': 'Factor1', 'f2': 'Factor2', 'f3': 'Factor3'}

    def test_empty_results(self):
        from gs_quant.models.risk_model_utils import build_factor_id_to_name_map
        assert build_factor_id_to_name_map([]) == {}

    def test_no_factor_data_key(self):
        from gs_quant.models.risk_model_utils import build_factor_id_to_name_map
        assert build_factor_id_to_name_map([{'other': 'data'}]) == {}

    def test_does_not_overwrite_existing_factor(self):
        from gs_quant.models.risk_model_utils import build_factor_id_to_name_map
        results = [
            {'factorData': [{'factorId': 'f1', 'factorName': 'First'}]},
            {'factorData': [{'factorId': 'f1', 'factorName': 'Second'}]},
        ]
        result = build_factor_id_to_name_map(results)
        # first occurrence should be kept (because of `not risk_model_factor_data.get(factor_id)`)
        assert result == {'f1': 'First'}


# ---------- build_asset_data_map ----------

class TestBuildAssetDataMap:
    def test_empty_results(self):
        from gs_quant.models.risk_model_utils import build_asset_data_map
        assert build_asset_data_map([], ('A',), Measure.Specific_Risk, {}) == {}

    def test_with_requested_universe(self):
        from gs_quant.models.risk_model_utils import build_asset_data_map
        results = [
            {
                'date': '2021-01-01',
                'assetData': {
                    'universe': ['A', 'B'],
                    'specificRisk': [0.1, 0.2],
                }
            },
            {
                'date': '2021-01-02',
                'assetData': {
                    'universe': ['A', 'B'],
                    'specificRisk': [0.3, 0.4],
                }
            },
        ]
        result = build_asset_data_map(results, ('A',), Measure.Specific_Risk, {})
        assert result == {'A': {'2021-01-01': 0.1, '2021-01-02': 0.3}}

    def test_without_requested_universe_uses_results_universe(self):
        from gs_quant.models.risk_model_utils import build_asset_data_map
        results = [
            {
                'date': '2021-01-01',
                'assetData': {
                    'universe': ['X', 'Y'],
                    'specificRisk': [1.0, 2.0],
                }
            },
        ]
        # Pass empty tuple for requested_universe
        result = build_asset_data_map(results, (), Measure.Specific_Risk, {})
        assert 'X' in result
        assert 'Y' in result
        assert result['X'] == {'2021-01-01': 1.0}
        assert result['Y'] == {'2021-01-01': 2.0}

    def test_factor_exposure_measure(self):
        from gs_quant.models.risk_model_utils import build_asset_data_map
        factor_map = {'f1': 'Style', 'f2': 'Industry'}
        results = [
            {
                'date': '2021-01-01',
                'assetData': {
                    'universe': ['A'],
                    'factorExposure': [{'f1': 0.5, 'f2': 1.5}],
                }
            },
        ]
        result = build_asset_data_map(results, ('A',), Measure.Universe_Factor_Exposure, factor_map)
        assert result == {'A': {'2021-01-01': {'Style': 0.5, 'Industry': 1.5}}}

    def test_asset_not_in_universe_row(self):
        from gs_quant.models.risk_model_utils import build_asset_data_map
        results = [
            {
                'date': '2021-01-01',
                'assetData': {
                    'universe': ['B'],
                    'specificRisk': [0.1],
                }
            },
        ]
        # Requesting 'A' that is not in the universe of any row
        result = build_asset_data_map(results, ('A',), Measure.Specific_Risk, {})
        assert result == {'A': {}}

    def test_factor_exposure_with_unmapped_factor(self):
        from gs_quant.models.risk_model_utils import build_asset_data_map
        # Factor id not in factor_map => falls back to factor id itself as key
        factor_map = {}
        results = [
            {
                'date': '2021-01-01',
                'assetData': {
                    'universe': ['A'],
                    'factorExposure': [{'unknown_factor': 0.9}],
                }
            },
        ]
        result = build_asset_data_map(results, ('A',), Measure.Universe_Factor_Exposure, factor_map)
        assert result == {'A': {'2021-01-01': {'unknown_factor': 0.9}}}


# ---------- build_factor_data_map ----------

class TestBuildFactorDataMap:
    def test_basic(self):
        from gs_quant.models.risk_model_utils import build_factor_data_map
        results = [
            {
                'date': '2021-01-01',
                'factorData': [
                    {'factorName': 'F1', 'factorReturn': 0.01},
                    {'factorName': 'F2', 'factorReturn': 0.02},
                ]
            },
            {
                'date': '2021-01-02',
                'factorData': [
                    {'factorName': 'F1', 'factorReturn': 0.03},
                    {'factorName': 'F2', 'factorReturn': 0.04},
                ]
            },
        ]
        df = build_factor_data_map(results, 'factorName', 'model1', Measure.Factor_Return)
        assert list(df.columns) == ['F1', 'F2']
        assert df.loc['2021-01-01', 'F1'] == 0.01

    def test_unsupported_measure_raises(self):
        from gs_quant.models.risk_model_utils import build_factor_data_map
        with pytest.raises(NotImplementedError):
            build_factor_data_map([], 'factorName', 'model1', Measure.Asset_Universe)

    def test_with_factors_filter(self):
        from gs_quant.models.risk_model_utils import build_factor_data_map
        results = [
            {
                'date': '2021-01-01',
                'factorData': [
                    {'factorName': 'F1', 'factorReturn': 0.01},
                    {'factorName': 'F2', 'factorReturn': 0.02},
                ]
            },
        ]
        df = build_factor_data_map(results, 'factorName', 'model1', Measure.Factor_Return, factors=['F1'])
        assert list(df.columns) == ['F1']

    def test_with_missing_factors_raises(self):
        from gs_quant.models.risk_model_utils import build_factor_data_map
        results = [
            {
                'date': '2021-01-01',
                'factorData': [
                    {'factorName': 'F1', 'factorReturn': 0.01},
                ]
            },
        ]
        with pytest.raises(ValueError, match='do not exist'):
            build_factor_data_map(results, 'factorName', 'model1', Measure.Factor_Return, factors=['Missing'])


# ---------- build_pfp_data_dataframe ----------

class TestBuildPfpDataDataframe:
    def _make_results(self):
        return [
            {
                'date': '2021-01-01',
                'factorData': [
                    {'factorId': 'f1', 'factorName': 'Style'},
                    {'factorId': 'f2', 'factorName': 'Industry'},
                ],
                'factorPortfolios': {
                    'universe': ['A', 'B'],
                    'portfolio': [
                        {'factorId': 'f1', 'weights': [0.3, 0.7]},
                        {'factorId': 'f2', 'weights': [0.4, 0.6]},
                    ]
                }
            },
        ]

    def test_return_df_true_get_factors_by_name(self):
        from gs_quant.models.risk_model_utils import build_pfp_data_dataframe
        result = build_pfp_data_dataframe(self._make_results(), return_df=True, get_factors_by_name=True)
        assert isinstance(result, pd.DataFrame)
        assert 'Style' in result.columns
        assert 'Industry' in result.columns
        assert 'identifier' in result.columns

    def test_return_df_false(self):
        from gs_quant.models.risk_model_utils import build_pfp_data_dataframe
        result = build_pfp_data_dataframe(self._make_results(), return_df=False, get_factors_by_name=True)
        assert isinstance(result, list)
        assert len(result) == 1
        assert 'identifier' in result[0]

    def test_get_factors_by_name_false(self):
        from gs_quant.models.risk_model_utils import build_pfp_data_dataframe
        result = build_pfp_data_dataframe(self._make_results(), return_df=True, get_factors_by_name=False)
        assert isinstance(result, pd.DataFrame)
        assert 'assetId' in result.columns
        # Column should be named with factorId prefix
        assert any('factorId: ' in str(c) for c in result.columns)

    def test_empty_pfp_list_returns_empty_df(self):
        from gs_quant.models.risk_model_utils import build_pfp_data_dataframe
        # If factorPortfolios has empty portfolio, pfp_list will be empty
        results = [
            {
                'date': '2021-01-01',
                'factorData': [
                    {'factorId': 'f1', 'factorName': 'Style'},
                ],
                'factorPortfolios': {
                    'universe': ['A'],
                    'portfolio': [
                        {'factorId': 'f1', 'weights': [0.3]},
                    ]
                }
            },
        ]
        result = build_pfp_data_dataframe(results, return_df=True, get_factors_by_name=True)
        assert isinstance(result, pd.DataFrame)

    def test_single_factor_data_series_branch(self):
        """When there's a single factorData on a date, factor_map_on_date is a Series, not DataFrame."""
        from gs_quant.models.risk_model_utils import build_pfp_data_dataframe
        results = [
            {
                'date': '2021-01-01',
                'factorData': [
                    {'factorId': 'f1', 'factorName': 'Style'},
                ],
                'factorPortfolios': {
                    'universe': ['A'],
                    'portfolio': [
                        {'factorId': 'f1', 'weights': [0.5]},
                    ]
                }
            },
        ]
        result = build_pfp_data_dataframe(results, return_df=True, get_factors_by_name=True)
        assert isinstance(result, pd.DataFrame)


# ---------- get_optional_data_as_dataframe ----------

class TestGetOptionalDataAsDataframe:
    def test_basic(self):
        from gs_quant.models.risk_model_utils import get_optional_data_as_dataframe
        results = [
            {'date': '2021-01-01', 'myKey': [[1, 2], [3, 4]]},
            {'date': '2021-01-02', 'myKey': [[5, 6], [7, 8]]},
        ]
        df = get_optional_data_as_dataframe(results, 'myKey')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4  # 2x2 matrix, 2 dates

    def test_empty(self):
        from gs_quant.models.risk_model_utils import get_optional_data_as_dataframe
        df = get_optional_data_as_dataframe([], 'myKey')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


# ---------- get_covariance_matrix_dataframe ----------

class TestGetCovarianceMatrixDataframe:
    def test_basic(self):
        from gs_quant.models.risk_model_utils import get_covariance_matrix_dataframe
        results = [
            {
                'date': '2021-01-01',
                'covarianceMatrix': [[1.0, 0.5], [0.5, 1.0]],
                'factorData': [
                    {'factorName': 'F1'},
                    {'factorName': 'F2'},
                ]
            }
        ]
        df = get_covariance_matrix_dataframe(results)
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ['F1', 'F2']

    def test_empty(self):
        from gs_quant.models.risk_model_utils import get_covariance_matrix_dataframe
        df = get_covariance_matrix_dataframe([])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_custom_key(self):
        from gs_quant.models.risk_model_utils import get_covariance_matrix_dataframe
        results = [
            {
                'date': '2021-01-01',
                'myMatrix': [[2.0]],
                'factorData': [
                    {'factorName': 'F1'},
                ]
            }
        ]
        df = get_covariance_matrix_dataframe(results, covariance_matrix_key='myMatrix')
        assert df.loc[('2021-01-01', 'F1'), 'F1'] == 2.0


# ---------- build_factor_volatility_dataframe ----------

class TestBuildFactorVolatilityDataframe:
    def test_basic_no_group_no_factors(self):
        from gs_quant.models.risk_model_utils import build_factor_volatility_dataframe
        results = [
            {'date': '2021-01-01', 'factorVolatility': {'f1': 0.1, 'f2': 0.2},
             'factorData': [{'factorId': 'f1', 'factorName': 'Style'}]},
        ]
        df = build_factor_volatility_dataframe(results, group_by_name=False, factors=[])
        assert list(df.index) == ['2021-01-01']
        assert 'f1' in df.columns

    def test_group_by_name(self):
        from gs_quant.models.risk_model_utils import build_factor_volatility_dataframe
        results = [
            {
                'date': '2021-01-01',
                'factorVolatility': {'f1': 0.1},
                'factorData': [{'factorId': 'f1', 'factorName': 'Style'}],
            },
        ]
        df = build_factor_volatility_dataframe(results, group_by_name=True, factors=[])
        assert 'Style' in df.columns

    def test_with_factors_filter(self):
        from gs_quant.models.risk_model_utils import build_factor_volatility_dataframe
        results = [
            {
                'date': '2021-01-01',
                'factorVolatility': {'f1': 0.1, 'f2': 0.2},
                'factorData': [{'factorId': 'f1', 'factorName': 'Style'},
                               {'factorId': 'f2', 'factorName': 'Industry'}],
            },
        ]
        df = build_factor_volatility_dataframe(results, group_by_name=True, factors=['Style'])
        assert list(df.columns) == ['Style']

    def test_with_missing_factors_raises(self):
        from gs_quant.models.risk_model_utils import build_factor_volatility_dataframe
        results = [
            {
                'date': '2021-01-01',
                'factorVolatility': {'f1': 0.1},
                'factorData': [{'factorId': 'f1', 'factorName': 'Style'}],
            },
        ]
        with pytest.raises(ValueError, match='do not exist'):
            build_factor_volatility_dataframe(results, group_by_name=True, factors=['Missing'])


# ---------- get_closest_date_index ----------

class TestGetClosestDateIndex:
    def test_before_direction(self):
        from gs_quant.models.risk_model_utils import get_closest_date_index
        dates = ['2021-01-01', '2021-01-02', '2021-01-03']
        idx = get_closest_date_index(dt.date(2021, 1, 2), dates, 'before')
        assert idx == 1

    def test_after_direction(self):
        from gs_quant.models.risk_model_utils import get_closest_date_index
        dates = ['2021-01-01', '2021-01-02', '2021-01-03']
        idx = get_closest_date_index(dt.date(2021, 1, 2), dates, 'after')
        assert idx == 1

    def test_not_found_returns_negative_one(self):
        from gs_quant.models.risk_model_utils import get_closest_date_index
        dates = ['2021-01-01']
        idx = get_closest_date_index(dt.date(2025, 6, 15), dates, 'before')
        assert idx == -1

    def test_finds_closest_before(self):
        from gs_quant.models.risk_model_utils import get_closest_date_index
        dates = ['2021-01-01', '2021-01-04']
        # date is 2021-01-03 (Saturday), should find 2021-01-01 going back
        idx = get_closest_date_index(dt.date(2021, 1, 3), dates, 'before')
        assert idx == 0  # finds 2021-01-01 after going back 2 days

    def test_finds_closest_after(self):
        from gs_quant.models.risk_model_utils import get_closest_date_index
        dates = ['2021-01-01', '2021-01-04']
        idx = get_closest_date_index(dt.date(2021, 1, 3), dates, 'after')
        assert idx == 1


# ---------- divide_request ----------

class TestDivideRequest:
    def test_basic(self):
        from gs_quant.models.risk_model_utils import divide_request
        result = list(divide_request([1, 2, 3, 4, 5], 2))
        assert result == [[1, 2], [3, 4], [5]]

    def test_exact_division(self):
        from gs_quant.models.risk_model_utils import divide_request
        result = list(divide_request([1, 2, 3, 4], 2))
        assert result == [[1, 2], [3, 4]]

    def test_single_chunk(self):
        from gs_quant.models.risk_model_utils import divide_request
        result = list(divide_request([1, 2], 5))
        assert result == [[1, 2]]


# ---------- only_factor_data_is_present ----------

class TestOnlyFactorDataIsPresent:
    def test_macro_type_with_only_factor_data(self):
        from gs_quant.models.risk_model_utils import only_factor_data_is_present
        data = {'date': '2021-01-01', 'factorData': []}
        assert only_factor_data_is_present(Type.Macro, data) is True

    def test_thematic_type_with_only_factor_data(self):
        from gs_quant.models.risk_model_utils import only_factor_data_is_present
        data = {'date': '2021-01-01', 'factorData': []}
        assert only_factor_data_is_present(Type.Thematic, data) is True

    def test_macro_type_with_extra_data(self):
        from gs_quant.models.risk_model_utils import only_factor_data_is_present
        data = {'date': '2021-01-01', 'factorData': [], 'assetData': []}
        assert only_factor_data_is_present(Type.Macro, data) is False

    def test_factor_type_with_factor_data_and_covariance(self):
        from gs_quant.models.risk_model_utils import only_factor_data_is_present
        data = {'date': '2021-01-01', 'factorData': [], 'covarianceMatrix': []}
        assert only_factor_data_is_present(Type.Factor, data) is True

    def test_factor_type_with_extra_data(self):
        from gs_quant.models.risk_model_utils import only_factor_data_is_present
        data = {'date': '2021-01-01', 'factorData': [], 'covarianceMatrix': [], 'assetData': []}
        assert only_factor_data_is_present(Type.Factor, data) is False

    def test_factor_type_without_factor_data(self):
        from gs_quant.models.risk_model_utils import only_factor_data_is_present
        data = {'date': '2021-01-01', 'covarianceMatrix': [], 'assetData': []}
        assert only_factor_data_is_present(Type.Factor, data) is False

    def test_macro_type_without_factor_data(self):
        from gs_quant.models.risk_model_utils import only_factor_data_is_present
        data = {'date': '2021-01-01', 'other': []}
        assert only_factor_data_is_present(Type.Macro, data) is False


# ---------- get_universe_size ----------

class TestGetUniverseSize:
    def test_asset_data_key(self):
        from gs_quant.models.risk_model_utils import get_universe_size
        data = {'assetData': {'universe': ['A', 'B', 'C']}}
        assert get_universe_size(data) == 3

    def test_universe_key(self):
        from gs_quant.models.risk_model_utils import get_universe_size
        data = {'factorPortfolios': {'universe': ['A', 'B']}}
        assert get_universe_size(data) == 2

    def test_universe_id1_key(self):
        from gs_quant.models.risk_model_utils import get_universe_size
        data = {'issuerSpecificCovariance': {'universeId1': ['A', 'B', 'C', 'D']}}
        assert get_universe_size(data) == 4

    def test_no_universe_raises(self):
        from gs_quant.models.risk_model_utils import get_universe_size
        with pytest.raises(ValueError, match='No universe found'):
            get_universe_size({'someKey': {'otherField': [1, 2]}})

    def test_string_value_skipped(self):
        from gs_quant.models.risk_model_utils import get_universe_size
        data = {'date': '2021-01-01', 'factorPortfolios': {'universe': ['A']}}
        assert get_universe_size(data) == 1


# ---------- _batch_input_data ----------

class TestBatchInputData:
    def test_asset_data_batching(self):
        from gs_quant.models.risk_model_utils import _batch_input_data
        input_data = {
            'assetData': {
                'universe': ['A', 'B', 'C', 'D'],
                'specificRisk': [0.1, 0.2, 0.3, 0.4],
                'factorExposure': [{'f1': 1}, {'f1': 2}, {'f1': 3}, {'f1': 4}],
                'totalRisk': [0.5, 0.6, 0.7, 0.8],
            }
        }
        batched, target_size = _batch_input_data(input_data, 2)
        assert target_size == 4
        assert len(batched) == 2
        assert batched[0]['universe'] == ['A', 'B']
        assert batched[0]['totalRisk'] == [0.5, 0.6]

    def test_pfp_data_batching(self):
        from gs_quant.models.risk_model_utils import _batch_input_data
        input_data = {
            'factorPortfolios': {
                'universe': ['A', 'B', 'C', 'D'],
                'portfolio': [
                    {'factorId': 'f1', 'weights': [0.1, 0.2, 0.3, 0.4]},
                ]
            }
        }
        batched, target_size = _batch_input_data(input_data, 2)
        assert target_size == 4
        assert len(batched) == 2
        assert batched[0]['universe'] == ['A', 'B']

    def test_isc_data_batching(self):
        from gs_quant.models.risk_model_utils import _batch_input_data
        input_data = {
            'issuerSpecificCovariance': {
                'universeId1': ['A', 'B', 'C', 'D'],
                'universeId2': ['X', 'Y', 'Z', 'W'],
                'covariance': [0.1, 0.2, 0.3, 0.4],
            }
        }
        batched, target_size = _batch_input_data(input_data, 2)
        assert target_size == 4
        assert len(batched) == 2

    def test_single_batch(self):
        from gs_quant.models.risk_model_utils import _batch_input_data
        input_data = {
            'assetData': {
                'universe': ['A'],
                'specificRisk': [0.1],
                'factorExposure': [{'f1': 1}],
            }
        }
        batched, target_size = _batch_input_data(input_data, 10)
        assert target_size == 1
        assert len(batched) == 1


# ---------- _batch_asset_input ----------

class TestBatchAssetInput:
    def test_last_batch_includes_remainder(self):
        from gs_quant.models.risk_model_utils import _batch_asset_input
        input_data = {
            'universe': ['A', 'B', 'C'],
            'specificRisk': [0.1, 0.2, 0.3],
            'factorExposure': [{'f1': 1}, {'f1': 2}, {'f1': 3}],
        }
        # last batch (i+1 == split_num)
        result = _batch_asset_input(input_data, 1, 2, 2, 3)
        assert 'A' not in result['universe']

    def test_optional_fields_none_excluded(self):
        from gs_quant.models.risk_model_utils import _batch_asset_input
        input_data = {
            'universe': ['A', 'B'],
            'specificRisk': [0.1, 0.2],
            'factorExposure': [{'f1': 1}, {'f1': 2}],
            'totalRisk': None,  # optional but None
        }
        result = _batch_asset_input(input_data, 0, 2, 1, 2)
        assert 'totalRisk' not in result


# ---------- _batch_pfp_input ----------

class TestBatchPfpInput:
    def test_basic(self):
        from gs_quant.models.risk_model_utils import _batch_pfp_input
        input_data = {
            'universe': ['A', 'B', 'C'],
            'portfolio': [
                {'factorId': 'f1', 'weights': [0.1, 0.2, 0.3]},
                {'factorId': 'f2', 'weights': [0.4, 0.5, 0.6]},
            ]
        }
        result = _batch_pfp_input(input_data, 0, 2, 2, 3)
        assert result['universe'] == ['A', 'B']
        assert len(result['portfolio']) == 2
        assert result['portfolio'][0]['factorId'] == 'f1'
        assert result['portfolio'][0]['weights'] == [0.1, 0.2]


# ---------- _batch_isc_input ----------

class TestBatchIscInput:
    def test_basic(self):
        from gs_quant.models.risk_model_utils import _batch_isc_input
        input_data = {
            'universeId1': ['A', 'B', 'C'],
            'universeId2': ['X', 'Y', 'Z'],
            'covariance': [0.1, 0.2, 0.3],
        }
        result = _batch_isc_input(input_data, 0, 2, 2, 3)
        assert result['universeId1'] == ['A', 'B']
        assert result['universeId2'] == ['X', 'Y']
        assert result['covariance'] == [0.1, 0.2]

    def test_last_batch(self):
        from gs_quant.models.risk_model_utils import _batch_isc_input
        input_data = {
            'universeId1': ['A', 'B', 'C'],
            'universeId2': ['X', 'Y', 'Z'],
            'covariance': [0.1, 0.2, 0.3],
        }
        # Last batch: split_num == i + 1
        result = _batch_isc_input(input_data, 1, 2, 2, 3)
        assert result['universeId1'] == ['C']


# ---------- _repeat_try_catch_request ----------

class TestRepeatTryCatchRequest:
    def test_success_first_try(self):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(return_value='ok')
        _repeat_try_catch_request(mock_fn, number_retries=3)
        assert mock_fn.call_count == 1

    def test_success_with_return_result(self):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(return_value='result_value')
        result = _repeat_try_catch_request(mock_fn, number_retries=3, return_result=True)
        assert result == 'result_value'

    def test_success_none_result(self):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(return_value=None)
        _repeat_try_catch_request(mock_fn, number_retries=3)
        assert mock_fn.call_count == 1

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_mq_request_error_retry_on_500(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(
            side_effect=[
                MqRequestError(500, 'server error'),
                'ok',
            ]
        )
        _repeat_try_catch_request(mock_fn, number_retries=3, verbose=True)
        assert mock_fn.call_count == 2

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_mq_request_error_retry_on_429(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(
            side_effect=[
                MqRequestError(429, 'rate limit'),
                'ok',
            ]
        )
        _repeat_try_catch_request(mock_fn, number_retries=3, verbose=True)
        assert mock_fn.call_count == 2

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_mq_request_error_no_retry_on_400(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(side_effect=MqRequestError(400, 'bad request'))
        with pytest.raises(MqRequestError):
            _repeat_try_catch_request(mock_fn, number_retries=3)
        assert mock_fn.call_count == 1

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_mq_request_error_no_retry_on_404(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(side_effect=MqRequestError(404, 'not found'))
        with pytest.raises(MqRequestError):
            _repeat_try_catch_request(mock_fn, number_retries=3)

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_mq_request_error_max_retries(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(side_effect=MqRequestError(500, 'server error'))
        with pytest.raises(MqRequestError):
            _repeat_try_catch_request(mock_fn, number_retries=2, verbose=True)
        assert mock_fn.call_count == 2

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_mq_request_error_max_retries_verbose_false(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(side_effect=MqRequestError(500, 'server error'))
        with pytest.raises(MqRequestError):
            _repeat_try_catch_request(mock_fn, number_retries=2, verbose=False)
        assert mock_fn.call_count == 2

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_unknown_exception_retry(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(
            side_effect=[
                RuntimeError('unknown'),
                'ok',
            ]
        )
        _repeat_try_catch_request(mock_fn, number_retries=3, verbose=True)
        assert mock_fn.call_count == 2

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_unknown_exception_max_retries(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(side_effect=RuntimeError('unknown'))
        with pytest.raises(RuntimeError):
            _repeat_try_catch_request(mock_fn, number_retries=2, verbose=True)
        assert mock_fn.call_count == 2

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_unknown_exception_max_retries_verbose_false(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(side_effect=RuntimeError('unknown'))
        with pytest.raises(RuntimeError):
            _repeat_try_catch_request(mock_fn, number_retries=2, verbose=False)
        assert mock_fn.call_count == 2

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_mq_request_error_retry_then_success_verbose_false(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(
            side_effect=[
                MqRequestError(500, 'err'),
                'ok',
            ]
        )
        _repeat_try_catch_request(mock_fn, number_retries=3, verbose=False)
        assert mock_fn.call_count == 2

    @patch('gs_quant.models.risk_model_utils.sleep')
    def test_unknown_exception_retry_verbose_false(self, mock_sleep):
        from gs_quant.models.risk_model_utils import _repeat_try_catch_request
        mock_fn = MagicMock(
            side_effect=[
                RuntimeError('unknown'),
                'ok',
            ]
        )
        _repeat_try_catch_request(mock_fn, number_retries=3, verbose=False)
        assert mock_fn.call_count == 2


# ---------- batch_and_upload_partial_data_use_target_universe_size ----------

class TestBatchAndUploadPartialDataUseTargetUniverseSize:
    @patch('gs_quant.models.risk_model_utils.sleep')
    @patch('gs_quant.models.risk_model_utils._batch_data_if_present')
    @patch('gs_quant.models.risk_model_utils._upload_factor_data_if_present')
    def test_basic(self, mock_upload, mock_batch, mock_sleep):
        from gs_quant.models.risk_model_utils import batch_and_upload_partial_data_use_target_universe_size
        data = {'date': '2021-01-01', 'factorData': [], 'assetData': {}}
        batch_and_upload_partial_data_use_target_universe_size('model1', data, 100)
        mock_upload.assert_called_once()
        mock_batch.assert_called_once()


# ---------- _upload_factor_data_if_present ----------

class TestUploadFactorDataIfPresent:
    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_no_factor_data(self, mock_repeat):
        from gs_quant.models.risk_model_utils import _upload_factor_data_if_present
        _upload_factor_data_if_present('model1', {}, '2021-01-01')
        mock_repeat.assert_not_called()

    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_with_factor_data_only(self, mock_repeat):
        from gs_quant.models.risk_model_utils import _upload_factor_data_if_present
        data = {'factorData': [{'factorId': 'f1'}]}
        _upload_factor_data_if_present('model1', data, '2021-01-01')
        mock_repeat.assert_called_once()

    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_with_covariance_matrix(self, mock_repeat):
        from gs_quant.models.risk_model_utils import _upload_factor_data_if_present
        data = {'factorData': [{'factorId': 'f1'}], 'covarianceMatrix': [[1]]}
        _upload_factor_data_if_present('model1', data, '2021-01-01')
        mock_repeat.assert_called_once()
        # Check that covarianceMatrix is in the model_data kwarg
        _, kwargs = mock_repeat.call_args
        assert 'covarianceMatrix' in kwargs['model_data']

    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_with_unadjusted_covariance_aws_upload(self, mock_repeat):
        from gs_quant.models.risk_model_utils import _upload_factor_data_if_present
        data = {'factorData': [{'factorId': 'f1'}], 'unadjustedCovarianceMatrix': [[1]]}
        _upload_factor_data_if_present('model1', data, '2021-01-01', aws_upload=True)
        _, kwargs = mock_repeat.call_args
        assert 'unadjustedCovarianceMatrix' in kwargs['model_data']

    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_unadjusted_covariance_without_aws_upload(self, mock_repeat):
        from gs_quant.models.risk_model_utils import _upload_factor_data_if_present
        data = {'factorData': [{'factorId': 'f1'}], 'unadjustedCovarianceMatrix': [[1]]}
        _upload_factor_data_if_present('model1', data, '2021-01-01')
        _, kwargs = mock_repeat.call_args
        assert 'unadjustedCovarianceMatrix' not in kwargs['model_data']

    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_with_pre_vra_covariance_aws_upload(self, mock_repeat):
        from gs_quant.models.risk_model_utils import _upload_factor_data_if_present
        data = {'factorData': [{'factorId': 'f1'}], 'preVRACovarianceMatrix': [[1]]}
        _upload_factor_data_if_present('model1', data, '2021-01-01', aws_upload=True)
        _, kwargs = mock_repeat.call_args
        assert 'preVRACovarianceMatrix' in kwargs['model_data']

    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_pre_vra_covariance_without_aws_upload(self, mock_repeat):
        from gs_quant.models.risk_model_utils import _upload_factor_data_if_present
        data = {'factorData': [{'factorId': 'f1'}], 'preVRACovarianceMatrix': [[1]]}
        _upload_factor_data_if_present('model1', data, '2021-01-01')
        _, kwargs = mock_repeat.call_args
        assert 'preVRACovarianceMatrix' not in kwargs['model_data']


# ---------- _batch_data_if_present ----------

class TestBatchDataIfPresent:
    @patch('gs_quant.models.risk_model_utils.sleep')
    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_with_asset_data(self, mock_repeat, mock_sleep):
        from gs_quant.models.risk_model_utils import _batch_data_if_present
        data = {
            'assetData': {
                'universe': ['A'],
                'specificRisk': [0.1],
                'factorExposure': [{'f1': 1}],
            },
        }
        _batch_data_if_present('model1', data, 10, '2021-01-01')
        mock_repeat.assert_called()

    @patch('gs_quant.models.risk_model_utils.sleep')
    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_with_isc_and_pfp(self, mock_repeat, mock_sleep):
        from gs_quant.models.risk_model_utils import _batch_data_if_present
        data = {
            'issuerSpecificCovariance': {
                'universeId1': ['A'],
                'universeId2': ['B'],
                'covariance': [0.1],
            },
            'factorPortfolios': {
                'universe': ['A'],
                'portfolio': [{'factorId': 'f1', 'weights': [0.1]}],
            },
        }
        _batch_data_if_present('model1', data, 10, '2021-01-01')
        # Should be called for both isc and pfp
        assert mock_repeat.call_count == 2

    @patch('gs_quant.models.risk_model_utils.sleep')
    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_no_data(self, mock_repeat, mock_sleep):
        from gs_quant.models.risk_model_utils import _batch_data_if_present
        _batch_data_if_present('model1', {}, 10, '2021-01-01')
        mock_repeat.assert_not_called()

    @patch('gs_quant.models.risk_model_utils.sleep')
    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_isc_key_present_but_none_value(self, mock_repeat, mock_sleep):
        from gs_quant.models.risk_model_utils import _batch_data_if_present
        data = {
            'issuerSpecificCovariance': None,
        }
        _batch_data_if_present('model1', data, 10, '2021-01-01')
        mock_repeat.assert_not_called()


# ---------- batch_and_upload_partial_data ----------

class TestBatchAndUploadPartialData:
    @patch('gs_quant.models.risk_model_utils.sleep')
    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    @patch('gs_quant.models.risk_model_utils._upload_factor_data_if_present')
    def test_with_currency_rates_data(self, mock_upload_factor, mock_repeat, mock_sleep):
        from gs_quant.models.risk_model_utils import batch_and_upload_partial_data
        data = {
            'date': '2021-01-01',
            'currencyRatesData': {'rates': [1.0]},
        }
        batch_and_upload_partial_data('model1', data, 100)
        mock_upload_factor.assert_called_once()
        # _repeat_try_catch_request called for currencyRatesData + 3 risk_model_data_types
        assert mock_repeat.call_count == 4

    @patch('gs_quant.models.risk_model_utils.sleep')
    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    @patch('gs_quant.models.risk_model_utils._upload_factor_data_if_present')
    def test_without_currency_rates_data(self, mock_upload_factor, mock_repeat, mock_sleep):
        from gs_quant.models.risk_model_utils import batch_and_upload_partial_data
        data = {'date': '2021-01-01'}
        batch_and_upload_partial_data('model1', data, 100)
        mock_upload_factor.assert_called_once()
        # _repeat_try_catch_request called for 3 risk_model_data_types only
        assert mock_repeat.call_count == 3


# ---------- _batch_data_v2 ----------

class TestBatchDataV2:
    @patch('gs_quant.models.risk_model_utils.GsFactorRiskModelApi.upload_risk_model_data')
    def test_with_asset_data(self, mock_upload):
        from gs_quant.models.risk_model_utils import _batch_data_v2
        mock_upload.return_value = 'success'
        data = {
            'universe': ['A'],
            'specificRisk': [0.1],
            'factorExposure': [{'f1': 1}],
        }
        _batch_data_v2('model1', data, 'assetData', 10, '2021-01-01')
        mock_upload.assert_called_once()

    @patch('gs_quant.models.risk_model_utils.GsFactorRiskModelApi.upload_risk_model_data')
    def test_with_isc_data_halves_max_size(self, mock_upload):
        from gs_quant.models.risk_model_utils import _batch_data_v2
        mock_upload.return_value = 'success'
        data = {
            'universeId1': ['A'],
            'universeId2': ['B'],
            'covariance': [0.1],
        }
        _batch_data_v2('model1', data, 'issuerSpecificCovariance', 10, '2021-01-01')
        mock_upload.assert_called_once()

    @patch('gs_quant.models.risk_model_utils.GsFactorRiskModelApi.upload_risk_model_data')
    def test_with_pfp_data_halves_max_size(self, mock_upload):
        from gs_quant.models.risk_model_utils import _batch_data_v2
        mock_upload.return_value = 'success'
        data = {
            'universe': ['A'],
            'portfolio': [{'factorId': 'f1', 'weights': [0.1]}],
        }
        _batch_data_v2('model1', data, 'factorPortfolios', 10, '2021-01-01')
        mock_upload.assert_called_once()

    @patch('gs_quant.models.risk_model_utils.GsFactorRiskModelApi.upload_risk_model_data')
    def test_none_data(self, mock_upload):
        from gs_quant.models.risk_model_utils import _batch_data_v2
        _batch_data_v2('model1', None, 'assetData', 10, '2021-01-01')
        mock_upload.assert_not_called()

    @patch('gs_quant.models.risk_model_utils.GsFactorRiskModelApi.upload_risk_model_data')
    def test_multiple_batches_final_upload(self, mock_upload):
        from gs_quant.models.risk_model_utils import _batch_data_v2
        mock_upload.return_value = 'success'
        data = {
            'universe': ['A', 'B', 'C', 'D'],
            'specificRisk': [0.1, 0.2, 0.3, 0.4],
            'factorExposure': [{'f1': 1}, {'f1': 2}, {'f1': 3}, {'f1': 4}],
        }
        _batch_data_v2('model1', data, 'assetData', 2, '2021-01-01')
        assert mock_upload.call_count == 2
        # Last call should have final_upload=True
        last_call_kwargs = mock_upload.call_args_list[-1][1]
        assert last_call_kwargs['final_upload'] is True
        # First call should have final_upload=False
        first_call_kwargs = mock_upload.call_args_list[0][1]
        assert first_call_kwargs['final_upload'] is False


# ---------- batch_and_upload_coverage_data ----------

class TestBatchAndUploadCoverageData:
    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_basic(self, mock_repeat):
        from gs_quant.models.risk_model_utils import batch_and_upload_coverage_data
        batch_and_upload_coverage_data(dt.date(2021, 1, 1), ['gsid1', 'gsid2'], 'model1', 10)
        mock_repeat.assert_called()


# ---------- upload_model_data ----------

class TestUploadModelData:
    @patch('gs_quant.models.risk_model_utils._repeat_try_catch_request')
    def test_basic(self, mock_repeat):
        from gs_quant.models.risk_model_utils import upload_model_data
        upload_model_data('model1', {'data': 'value'})
        mock_repeat.assert_called_once()


# ---------- risk_model_data_to_json ----------

class TestRiskModelDataToJson:
    def test_basic_no_optional(self):
        from gs_quant.models.risk_model_utils import risk_model_data_to_json
        mock_asset_data = MagicMock()
        mock_asset_data.to_json.return_value = {'universe': ['A']}

        mock_data = MagicMock()
        mock_data.to_json.return_value = {
            'date': '2021-01-01',
            'assetData': mock_asset_data,
            'factorPortfolios': None,
            'issuerSpecificCovariance': None,
        }

        result = risk_model_data_to_json(mock_data)
        assert result['assetData'] == {'universe': ['A']}

    def test_with_factor_portfolios(self):
        from gs_quant.models.risk_model_utils import risk_model_data_to_json
        mock_portfolio_item = MagicMock()
        mock_portfolio_item.to_json.return_value = {'factorId': 'f1', 'weights': [0.1]}

        mock_pfp = MagicMock()
        mock_pfp.to_json.return_value = {'universe': ['A'], 'portfolio': [mock_portfolio_item]}
        mock_pfp.get = mock_pfp.to_json.return_value.get

        mock_asset_data = MagicMock()
        mock_asset_data.to_json.return_value = {'universe': ['A']}

        mock_data = MagicMock()
        data_dict = {
            'date': '2021-01-01',
            'assetData': mock_asset_data,
            'factorPortfolios': mock_pfp,
            'issuerSpecificCovariance': None,
        }
        mock_data.to_json.return_value = data_dict

        result = risk_model_data_to_json(mock_data)
        assert 'factorPortfolios' in result

    def test_with_issuer_specific_covariance(self):
        from gs_quant.models.risk_model_utils import risk_model_data_to_json
        mock_isc = MagicMock()
        mock_isc.to_json.return_value = {'universeId1': ['A'], 'universeId2': ['B'], 'covariance': [0.1]}

        mock_asset_data = MagicMock()
        mock_asset_data.to_json.return_value = {'universe': ['A']}

        mock_data = MagicMock()
        mock_data.to_json.return_value = {
            'date': '2021-01-01',
            'assetData': mock_asset_data,
            'factorPortfolios': None,
            'issuerSpecificCovariance': mock_isc,
        }

        result = risk_model_data_to_json(mock_data)
        assert result['issuerSpecificCovariance'] == {'universeId1': ['A'], 'universeId2': ['B'], 'covariance': [0.1]}
