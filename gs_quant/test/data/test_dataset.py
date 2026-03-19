"""
Copyright 2018 Goldman Sachs.
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

import copy
import datetime as dt
from unittest.mock import MagicMock, patch, PropertyMock, AsyncMock

import numpy as np
import pandas as pd
import pytest

from gs_quant.api.gs.data import GsDataApi
from gs_quant.data import Dataset
from gs_quant.data.dataset import PTPDataset, MarqueeDataIngestionLibrary, InvalidInputException
from gs_quant.data.fields import Fields
from gs_quant.errors import MqValueError
from gs_quant.session import GsSession, Environment
from gs_quant.target.data import (
    Format,
    DataQuery,
    DataSetEntity,
    DataSetParameters,
    DataSetDimensions,
    FieldColumnPair,
    DataSetFieldEntity,
    DBConfig,
    DataSetType,
)

test_types = {
    'date': 'date',
    'assetId': 'string',
    'askPrice': 'number',
    'adjustedAskPrice': 'number',
    'bidPrice': 'number',
    'adjustedBidPrice': 'number',
    'tradePrice': 'number',
    'adjustedTradePrice': 'number',
    'openPrice': 'number',
    'adjustedOpenPrice': 'number',
    'highPrice': 'number',
    'lowPrice': 'number',
    'adjustedHighPrice': 'number',
    'adjustedLowPrice': 'number',
    'updateTime': 'date-time',
}
test_data = [
    {
        'date': dt.date(2019, 1, 2),
        'assetId': 'MA4B66MW5E27U8P32SB',
        'askPrice': 2529,
        'adjustedAskPrice': 2529,
        'bidPrice': 2442.55,
        'adjustedBidPrice': 2442.55,
        'tradePrice': 2510.03,
        'adjustedTradePrice': 2510.03,
        'openPrice': 2476.96,
        'adjustedOpenPrice': 2476.96,
        'highPrice': 2519.49,
        'lowPrice': 2467.47,
        'adjustedHighPrice': 2519.49,
        'adjustedLowPrice': 2467.47,
        'updateTime': dt.datetime.strptime('2019-01-03T00:53:00Z', '%Y-%m-%dT%H:%M:%SZ'),
    },
    {
        'date': dt.date(2019, 1, 3),
        'assetId': 'MA4B66MW5E27U8P32SB',
        'askPrice': 2502.34,
        'adjustedAskPrice': 2502.34,
        'bidPrice': 2418.09,
        'adjustedBidPrice': 2418.09,
        'tradePrice': 2447.89,
        'adjustedTradePrice': 2447.89,
        'openPrice': 2491.92,
        'adjustedOpenPrice': 2491.92,
        'highPrice': 2493.14,
        'lowPrice': 2443.96,
        'adjustedHighPrice': 2493.14,
        'adjustedLowPrice': 2443.96,
        'updateTime': dt.datetime.strptime('2019-01-04T00:14:00Z', '%Y-%m-%dT%H:%M:%SZ'),
    },
    {
        'date': dt.date(2019, 1, 4),
        'assetId': 'MA4B66MW5E27U8P32SB',
        'askPrice': 2566.52,
        'adjustedAskPrice': 2566.52,
        'bidPrice': 2487.8,
        'adjustedBidPrice': 2487.8,
        'tradePrice': 2531.94,
        'adjustedTradePrice': 2531.94,
        'openPrice': 2474.33,
        'adjustedOpenPrice': 2474.33,
        'highPrice': 2538.07,
        'lowPrice': 2474.33,
        'adjustedHighPrice': 2538.07,
        'adjustedLowPrice': 2474.33,
        'updateTime': dt.datetime.strptime('2019-01-08T00:31:00Z', '%Y-%m-%dT%H:%M:%SZ'),
    },
    {
        'date': dt.date(2019, 1, 7),
        'assetId': 'MA4B66MW5E27U8P32SB',
        'askPrice': 2591.75,
        'adjustedAskPrice': 2591.75,
        'bidPrice': 2509.77,
        'adjustedBidPrice': 2509.77,
        'tradePrice': 2549.69,
        'adjustedTradePrice': 2549.69,
        'openPrice': 2535.61,
        'adjustedOpenPrice': 2535.61,
        'highPrice': 2566.16,
        'lowPrice': 2524.56,
        'adjustedHighPrice': 2566.16,
        'adjustedLowPrice': 2524.56,
        'updateTime': dt.datetime.strptime('2019-01-08T00:31:00Z', '%Y-%m-%dT%H:%M:%SZ'),
    },
    {
        'date': dt.date(2019, 1, 8),
        'assetId': 'MA4B66MW5E27U8P32SB',
        'askPrice': 2610.52,
        'adjustedAskPrice': 2610.52,
        'bidPrice': 2531.15,
        'adjustedBidPrice': 2531.15,
        'tradePrice': 2574.41,
        'adjustedTradePrice': 2574.41,
        'openPrice': 2568.11,
        'adjustedOpenPrice': 2568.11,
        'highPrice': 2579.82,
        'lowPrice': 2547.56,
        'adjustedHighPrice': 2579.82,
        'adjustedLowPrice': 2547.56,
        'updateTime': dt.datetime.strptime('2019-01-09T00:50:00Z', '%Y-%m-%dT%H:%M:%SZ'),
    },
    {
        'date': dt.date(2019, 1, 9),
        'assetId': 'MA4B66MW5E27U8P32SB',
        'askPrice': 2623.09,
        'adjustedAskPrice': 2623.09,
        'bidPrice': 2537.19,
        'adjustedBidPrice': 2537.19,
        'tradePrice': 2584.96,
        'adjustedTradePrice': 2584.96,
        'openPrice': 2580,
        'adjustedOpenPrice': 2580,
        'highPrice': 2595.32,
        'lowPrice': 2568.89,
        'adjustedHighPrice': 2595.32,
        'adjustedLowPrice': 2568.89,
        'updateTime': dt.datetime.strptime('2019-01-10T00:44:00Z', '%Y-%m-%dT%H:%M:%SZ'),
    },
]

tr_types = {
    'time': 'date-time',
    'assetId': 'string',
    'askPrice': 'number',
    'adjustedAskPrice': 'number',
    'bidPrice': 'number',
    'adjustedBidPrice': 'number',
    'tradePrice': 'number',
    'adjustedTradePrice': 'number',
    'openPrice': 'number',
    'adjustedOpenPrice': 'number',
    'highPrice': 'number',
    'adjustedLowPrice': 'number',
    'lowPrice': 'number',
    'adjustedHighPrice': 'number',
}
tr_data = [
    {
        'time': dt.datetime(2023, 5, 31, 14),
        'assetId': 'MA4B66MW5E27U8P32SB',
        'askPrice': 2529,
        'adjustedAskPrice': 2529,
        'bidPrice': 2442.55,
        'adjustedBidPrice': 2442.55,
        'tradePrice': 2510.03,
        'adjustedTradePrice': 2510.03,
        'openPrice': 2476.96,
        'adjustedOpenPrice': 2476.96,
        'highPrice': 2519.49,
        'lowPrice': 2467.47,
        'adjustedHighPrice': 2519.49,
        'adjustedLowPrice': 2467.47,
    },
    {
        'time': dt.datetime(2023, 5, 31, 15),
        'assetId': 'MA4B66MW5E27U8P32SB',
        'askPrice': 2502.34,
        'adjustedAskPrice': 2502.34,
        'bidPrice': 2418.09,
        'adjustedBidPrice': 2418.09,
        'tradePrice': 2447.89,
        'adjustedTradePrice': 2447.89,
        'openPrice': 2491.92,
        'adjustedOpenPrice': 2491.92,
        'highPrice': 2493.14,
        'lowPrice': 2443.96,
        'adjustedHighPrice': 2493.14,
        'adjustedLowPrice': 2443.96,
    },
    {'time': dt.datetime(2023, 5, 31, 16), 'assetId': 'MA4B66MW5E27U8P32SB'},
    {
        'time': dt.datetime(2023, 5, 31, 17),
        'assetId': 'MA4B66MW5E27U8P32SB',
        'askPrice': 2566.52,
        'adjustedAskPrice': 2566.52,
        'bidPrice': 2487.8,
        'adjustedBidPrice': 2487.8,
        'tradePrice': 2531.94,
        'adjustedTradePrice': 2531.94,
        'openPrice': 2474.33,
        'adjustedOpenPrice': 2474.33,
        'highPrice': 2538.07,
        'lowPrice': 2474.33,
        'adjustedHighPrice': 2538.07,
        'adjustedLowPrice': 2474.33,
    },
]

test_coverage_data = {'results': [{'gsid': 'gsid1'}]}


# =====================================================================
# Original tests (preserved)
# =====================================================================

def test_query_data(mocker):
    mock = mocker.patch("gs_quant.api.gs.data.GsDataApi.query_data", return_value=test_data)
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=test_types)
    dataset = Dataset(Dataset.TR.TREOD)
    data = dataset.get_data(dt.date(2019, 1, 2), dt.date(2019, 1, 9), assetId='MA4B66MW5E27U8P32SB')
    assert data.equals(GsDataApi.construct_dataframe_with_types(str(Dataset.TR.TREOD), test_data))

    assert mock.call_count == 1
    query = mock.call_args[0][0]
    assert type(query) is DataQuery
    assert query.empty_intervals is None


def test_query_data_intervals(mocker):
    mock = mocker.patch("gs_quant.api.gs.data.GsDataApi.query_data", return_value=tr_data)
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=tr_types)
    dataset = Dataset(Dataset.TR.TREOD)
    data = dataset.get_data(
        dt.datetime(2023, 5, 31, 13),
        dt.datetime(2023, 5, 31, 17),
        assetId='MA4B66MW5E27U8P32SB',
        intervals=4,
        empty_intervals=True,
    )
    assert data.equals(GsDataApi.construct_dataframe_with_types(str(Dataset.TR.TR), tr_data))

    assert mock.call_count == 1
    query = mock.call_args[0][0]
    assert type(query) is DataQuery
    assert query.empty_intervals is True


def test_query_data_types(mocker):
    mocker.patch("gs_quant.api.gs.data.GsDataApi.query_data", return_value=test_data)
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=test_types)
    dataset = Dataset(Dataset.TR.TREOD)
    data = dataset.get_data(dt.date(2019, 1, 2), dt.date(2019, 1, 9), assetId='MA4B66MW5E27U8P32SB')
    assert data.equals(GsDataApi.construct_dataframe_with_types(str(Dataset.TR.TREOD), test_data))


def test_last_data(mocker):
    mocker.patch("gs_quant.api.gs.data.GsDataApi.last_data", return_value=[test_data[-1]])
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=test_types)
    dataset = Dataset(Dataset.TR.TREOD)
    data = dataset.get_data_last(dt.date(2019, 1, 9), assetId='MA4B66MW5E27U8P32SB')
    assert data.equals(GsDataApi.construct_dataframe_with_types(str(Dataset.TR.TREOD), ([test_data[-1]])))


def test_get_data_series(mocker):
    field_value_maps = test_data
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=test_types)
    mocker.patch.object(GsDataApi, 'query_data', return_value=field_value_maps)
    mocker.patch.object(GsDataApi, 'symbol_dimensions', return_value=('assetId',))

    dataset = Dataset(Dataset.TR.TREOD)
    series = dataset.get_data_series(
        'tradePrice', dt.date(2019, 1, 2), dt.date(2019, 1, 9), assetId='MA4B66MW5E27U8P32SB'
    )

    df = pd.DataFrame(test_data)
    index = pd.to_datetime(df.loc[:, 'date'].values)
    expected = pd.Series(index=index, data=df.loc[:, 'tradePrice'].values)
    expected = expected.rename_axis('date')

    pd.testing.assert_series_equal(series, expected)


def test_get_coverage(mocker):
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_coverage", return_value=test_coverage_data)
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value={'gsid': 'string'})
    data = Dataset(Dataset.TR.TREOD).get_coverage()
    results = test_coverage_data["results"]
    gsid = GsDataApi.construct_dataframe_with_types(str(Dataset.TR.TREOD), results).get('gsid').get(0)
    assert data["results"][0]["gsid"] == gsid


def test_construct_dataframe_with_types(mocker):
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=test_types)
    df = GsDataApi.construct_dataframe_with_types(str(Dataset.TR.TREOD), [test_data[0]])
    assert np.issubdtype(df.index.dtype, np.datetime64)
    assert df['adjustedAskPrice'].dtype == np.int64
    assert df['adjustedBidPrice'].dtype == np.float64
    # pandas 3.0 uses StringDtype for strings, older versions use object
    assert df['assetId'].dtype == object or pd.api.types.is_string_dtype(df['assetId'])
    assert np.issubdtype(df['updateTime'].dtype, np.datetime64)


def test_construct_dataframe_var_schema(mocker):
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=test_types)
    var_schema = copy.deepcopy(test_data)
    for i in range(len(var_schema)):
        if i % 2 == 1:
            prev = var_schema[i - 1]
            curr = var_schema[i]
            curr['difference_tradePrice'] = curr['tradePrice'] - prev['tradePrice']

    df = GsDataApi.construct_dataframe_with_types(str(Dataset.TR.TREOD), var_schema, True)
    assert np.issubdtype(df['difference_tradePrice'].dtype, np.floating)


def test_dataframe_with_mixed_date_type(mocker):
    mocker.patch.object(GsDataApi, 'get_types', return_value={'updateTime': 'date-time'})

    df = GsDataApi.construct_dataframe_with_types(
        'BBG_PER_SECURITY', [{'updateTime': '2022-02-24T19:25:28Z'}, {'updateTime': '2022-11-10T17:18:23.021494Z'}]
    )

    assert df.empty is False


def test_data_series_format(mocker):
    start = dt.date(2019, 1, 2)
    end = dt.datetime(2019, 1, 9)
    df = pd.DataFrame(test_data)
    index = pd.to_datetime(df.loc[:, 'date'].values)
    expected = pd.Series(index=index, data=df.loc[:, 'tradePrice'].values)
    expected = expected.rename_axis('date')

    # mock GsSession and data response
    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )
    mock_response = {'requestId': 'qwerty', 'data': test_data}
    mocker.patch.object(GsSession.current.sync, 'post', side_effect=lambda *args, **kwargs: mock_response)
    mocker.patch.object(GsSession.current.sync, 'get', return_value={"id": "TREOD"})
    mocker.patch.object(GsDataApi, 'symbol_dimensions', return_value=('assetId',))
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=test_types)

    actual = Dataset('TREOD').get_data_series(field='tradePrice', start=start, end=end, assetId='MA4B66MW5E27U8P32SB')
    pd.testing.assert_series_equal(actual, expected)
    assert len(GsSession.current.sync.post.mock_calls) == 1
    name, args, kwargs = GsSession.current.sync.post.mock_calls[0]
    assert kwargs['payload'].format == Format.MessagePack
    assert kwargs['request_headers'] == {'Accept': 'application/msgpack'}
    assert args[0] == '/data/TREOD/query'

    GsSession.current.sync.post.reset_mock()
    actual = Dataset('TREOD').get_data_series(
        field='tradePrice', start=start, end=end, assetId='MA4B66MW5E27U8P32SB', format=Format.Json
    )
    pd.testing.assert_series_equal(actual, expected)
    assert len(GsSession.current.sync.post.mock_calls) == 1
    name, args, kwargs = GsSession.current.sync.post.mock_calls[0]
    assert kwargs['payload'].format == Format.Json
    assert 'request_headers' not in kwargs
    assert args[0] == '/data/TREOD/query'


def test_get_data_bulk(mocker):
    df2 = pd.DataFrame()
    test_df = {
        'date': {pd.Timestamp('20230302'): dt.date(2023, 3, 2)},
        'clusterRegion': {pd.Timestamp('20230302'): 'Asia Pacific'},
        'clusterClass': {pd.Timestamp('20230302'): '13'},
        'assetId': {pd.Timestamp('20230302'): 'MA4B66MW5E27U8P4ZFX'},
        'clusterDescription': {pd.Timestamp('20230302'): 'Small Trd Count, Hard to Complete'},
        'updateTime': {
            pd.Timestamp('20230302'): dt.datetime.strptime('2023-03-05T00:40:54.000Z', '%Y-%m-%dT%H:%M:%S.%fZ')
        },
    }

    df = pd.DataFrame(test_df)
    df = df.set_index('date')

    coverage = pd.DataFrame({'clusterRegion': ['Asia Pacific']})
    symbol_dimension = ('clusterRegion',)
    dataset_definition = DataSetEntity()
    dataset_definition.parameters = DataSetParameters()
    dataset_definition.parameters.history_date = dt.datetime(2017, 1, 2)
    dataset_definition.dimensions = DataSetDimensions()
    dataset_definition.dimensions.time_field = 'date'

    # mock GsSession
    mocker.patch.object(
        GsSession.__class__, 'default_value', return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
    )

    # mock fetch_data function in utilities.py
    mock = mocker.patch("gs_quant.data.utilities.Utilities.fetch_data", return_value=df)

    # mock granular functions in get_dataset_parameter in utilities.py
    mocker.patch("gs_quant.api.gs.data.GsDataApi.symbol_dimensions", return_value=symbol_dimension)
    mocker.patch("gs_quant.api.gs.data.GsDataApi.get_definition", return_value=dataset_definition)

    # mock get_dataset_coverage function in utilities.py
    mocker.patch("gs_quant.data.dataset.Dataset.get_coverage", return_value=coverage)

    def handler(data_frame):
        nonlocal df2
        df2 = data_frame.head(1)

    dataset_id = "EQTRADECLUSTERS"
    original_start = dt.datetime(2023, 3, 2, 0, 0, 0)
    final_end = dt.datetime(2023, 3, 2, 0, 0, 0)
    c = Dataset(dataset_id)
    c.get_data_bulk(
        original_start=original_start,
        final_end=final_end,
        request_batch_size=4,
        identifier="clusterRegion",
        handler=handler,
    )

    assert mock.call_count == 1
    assert df.equals(df2)


# =====================================================================
# NEW branch-coverage tests
# =====================================================================


class TestDatasetConstruction:
    """Tests for Dataset.__init__, _get_dataset_id_str, and property accessors."""

    def test_init_with_string_id(self):
        ds = Dataset('MY_DATASET')
        assert ds.id == 'MY_DATASET'

    def test_init_with_vendor_enum(self):
        ds = Dataset(Dataset.GS.WEATHER)
        assert ds.id == 'WEATHER'

    def test_init_with_tr_vendor(self):
        ds = Dataset(Dataset.TR.TREOD)
        assert ds.id == 'TREOD'

    def test_init_with_fred_vendor(self):
        ds = Dataset(Dataset.FRED.GDP)
        assert ds.id == 'GDP'

    def test_init_with_trading_economics_vendor(self):
        ds = Dataset(Dataset.TradingEconomics.MACRO_EVENTS_CALENDAR)
        assert ds.id == 'MACRO_EVENTS_CALENDAR'

    def test_init_with_provider(self):
        mock_provider = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        assert ds.provider == mock_provider

    def test_provider_defaults_to_gs_data_api(self):
        ds = Dataset('TEST')
        assert ds.provider == GsDataApi

    def test_name_property_returns_none(self):
        ds = Dataset('TEST')
        assert ds.name is None

    def test_id_property(self):
        ds = Dataset('FOO')
        assert ds.id == 'FOO'


class TestBuildDataQuery:
    """Tests for Dataset._build_data_query -- covers field mapping, schema_varies, date kwarg handling."""

    def test_fields_none(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, schema_varies = ds._build_data_query(
            start=dt.date(2020, 1, 1), end=dt.date(2020, 1, 31),
            as_of=None, since=None, fields=None, empty_intervals=None,
        )
        assert schema_varies is False
        mock_provider.build_query.assert_called_once()
        call_kwargs = mock_provider.build_query.call_args
        assert call_kwargs[1]['fields'] is None

    def test_fields_as_strings(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, schema_varies = ds._build_data_query(
            start=dt.date(2020, 1, 1), end=dt.date(2020, 1, 31),
            as_of=None, since=None, fields=['tradePrice', 'bidPrice'], empty_intervals=None,
        )
        assert schema_varies is False
        call_kwargs = mock_provider.build_query.call_args
        assert call_kwargs[1]['fields'] == ['tradePrice', 'bidPrice']

    def test_fields_as_enum(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, schema_varies = ds._build_data_query(
            start=None, end=None, as_of=None, since=None,
            fields=[Fields.TRADE_PRICE, Fields.BID_PRICE], empty_intervals=None,
        )
        assert schema_varies is False
        call_kwargs = mock_provider.build_query.call_args
        assert call_kwargs[1]['fields'] == ['tradePrice', 'bidPrice']

    def test_fields_mixed_strings_and_enums(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, schema_varies = ds._build_data_query(
            start=None, end=None, as_of=None, since=None,
            fields=['askPrice', Fields.TRADE_PRICE], empty_intervals=None,
        )
        call_kwargs = mock_provider.build_query.call_args
        assert call_kwargs[1]['fields'] == ['askPrice', 'tradePrice']

    def test_schema_varies_true_with_function_field(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, schema_varies = ds._build_data_query(
            start=None, end=None, as_of=None, since=None,
            fields=['difference(tradePrice)'], empty_intervals=None,
        )
        assert schema_varies is True

    def test_schema_varies_false_with_normal_fields(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, schema_varies = ds._build_data_query(
            start=None, end=None, as_of=None, since=None,
            fields=['tradePrice'], empty_intervals=None,
        )
        assert schema_varies is False

    def test_date_kwarg_string_valid_format(self):
        """When date is a valid YYYY-MM-DD string, it should be parsed to a date object."""
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, _ = ds._build_data_query(
            start=None, end=None, as_of=None, since=None,
            fields=None, empty_intervals=None, date='2020-01-15',
        )
        call_kwargs = mock_provider.build_query.call_args
        # date kwarg should have been converted to datetime.date
        assert call_kwargs[1]['date'] == dt.date(2020, 1, 15)
        # With start=None and end=None and no 'dates' in kwargs, dates tuple should be set
        assert 'dates' in call_kwargs[1]

    def test_date_kwarg_string_invalid_format(self):
        """When date is a string but not YYYY-MM-DD, the ValueError is caught and ignored."""
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, _ = ds._build_data_query(
            start=None, end=None, as_of=None, since=None,
            fields=None, empty_intervals=None, date='01/15/2020',
        )
        call_kwargs = mock_provider.build_query.call_args
        # date stays as-is (string)
        assert call_kwargs[1]['date'] == '01/15/2020'

    def test_date_kwarg_not_string(self):
        """When date is already a date object, it should not be parsed."""
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        d = dt.date(2020, 3, 15)
        query, _ = ds._build_data_query(
            start=None, end=None, as_of=None, since=None,
            fields=None, empty_intervals=None, date=d,
        )
        call_kwargs = mock_provider.build_query.call_args
        assert call_kwargs[1]['date'] == d
        assert 'dates' in call_kwargs[1]

    def test_date_kwarg_with_dates_already_present(self):
        """When 'dates' is already in kwargs, it should not be overwritten."""
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        explicit_dates = (dt.date(2020, 1, 1), dt.date(2020, 1, 2))
        query, _ = ds._build_data_query(
            start=None, end=None, as_of=None, since=None,
            fields=None, empty_intervals=None, date='2020-01-15', dates=explicit_dates,
        )
        call_kwargs = mock_provider.build_query.call_args
        assert call_kwargs[1]['dates'] == explicit_dates

    def test_date_kwarg_with_start_set(self):
        """When start is set, dates should not be auto-generated even if date kwarg is present."""
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, _ = ds._build_data_query(
            start=dt.date(2020, 1, 1), end=None, as_of=None, since=None,
            fields=None, empty_intervals=None, date='2020-01-15',
        )
        call_kwargs = mock_provider.build_query.call_args
        # dates should NOT be auto-set when start is not None
        assert 'dates' not in call_kwargs[1]

    def test_date_kwarg_with_end_set(self):
        """When end is set, dates should not be auto-generated."""
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, _ = ds._build_data_query(
            start=None, end=dt.date(2020, 1, 31), as_of=None, since=None,
            fields=None, empty_intervals=None, date='2020-01-15',
        )
        call_kwargs = mock_provider.build_query.call_args
        assert 'dates' not in call_kwargs[1]

    def test_no_date_kwarg(self):
        """When no date kwarg, no special date handling."""
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        ds = Dataset('TEST', provider=mock_provider)
        query, _ = ds._build_data_query(
            start=dt.date(2020, 1, 1), end=dt.date(2020, 1, 31),
            as_of=None, since=None, fields=None, empty_intervals=None,
        )
        call_kwargs = mock_provider.build_query.call_args
        assert 'date' not in call_kwargs[1]


class TestBuildDataFrame:
    """Tests for Dataset._build_data_frame -- tuple vs list data."""

    def test_data_is_list(self):
        mock_provider = MagicMock()
        expected_df = pd.DataFrame({'a': [1, 2]})
        mock_provider.construct_dataframe_with_types.return_value = expected_df
        ds = Dataset('TEST', provider=mock_provider)
        result = ds._build_data_frame([{'a': 1}, {'a': 2}], False, False)
        assert result.equals(expected_df)
        mock_provider.construct_dataframe_with_types.assert_called_once_with(
            'TEST', [{'a': 1}, {'a': 2}], False, standard_fields=False
        )

    def test_data_is_tuple_applies_groupby(self):
        mock_provider = MagicMock()
        df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'x']})
        mock_provider.construct_dataframe_with_types.return_value = df
        ds = Dataset('TEST', provider=mock_provider)
        data = ([{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'x'}], 'b')
        result = ds._build_data_frame(data, True, True)
        # Should call construct_dataframe_with_types with data[0]
        mock_provider.construct_dataframe_with_types.assert_called_once_with(
            'TEST', data[0], True, standard_fields=True
        )
        # Result should be a dataframe (groupby applied)
        assert isinstance(result, pd.DataFrame)

    def test_data_is_tuple_with_standard_fields(self):
        mock_provider = MagicMock()
        df = pd.DataFrame({'val': [10, 20], 'group': ['a', 'b']})
        mock_provider.construct_dataframe_with_types.return_value = df
        ds = Dataset('TEST', provider=mock_provider)
        data = ([{'val': 10, 'group': 'a'}, {'val': 20, 'group': 'b'}], 'group')
        result = ds._build_data_frame(data, False, True)
        assert isinstance(result, pd.DataFrame)


class TestGetData:
    """Tests for Dataset.get_data."""

    def test_get_data_basic(self):
        mock_provider = MagicMock()
        expected_df = pd.DataFrame({'price': [100]})
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.query_data.return_value = [{'price': 100}]
        mock_provider.construct_dataframe_with_types.return_value = expected_df
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_data(start=dt.date(2020, 1, 1), end=dt.date(2020, 1, 31))
        assert result.equals(expected_df)

    def test_get_data_with_asset_id_type(self):
        mock_provider = MagicMock()
        expected_df = pd.DataFrame({'price': [100]})
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.query_data.return_value = [{'price': 100}]
        mock_provider.construct_dataframe_with_types.return_value = expected_df
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_data(start=dt.date(2020, 1, 1), asset_id_type='bbid')
        mock_provider.query_data.assert_called_once()
        assert mock_provider.query_data.call_args[1]['asset_id_type'] == 'bbid'

    def test_get_data_with_standard_fields(self):
        mock_provider = MagicMock()
        expected_df = pd.DataFrame({'price': [100]})
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.query_data.return_value = [{'price': 100}]
        mock_provider.construct_dataframe_with_types.return_value = expected_df
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_data(standard_fields=True)
        assert result.equals(expected_df)


class TestGetDataAsync:
    """Tests for Dataset.get_data_async."""

    @pytest.mark.asyncio
    async def test_get_data_async_basic(self):
        mock_provider = MagicMock()
        expected_df = pd.DataFrame({'price': [100]})
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.query_data_async = AsyncMock(return_value=[{'price': 100}])
        mock_provider.construct_dataframe_with_types.return_value = expected_df
        ds = Dataset('TEST', provider=mock_provider)
        result = await ds.get_data_async(start=dt.date(2020, 1, 1), end=dt.date(2020, 1, 31))
        assert result.equals(expected_df)

    @pytest.mark.asyncio
    async def test_get_data_async_with_fields(self):
        mock_provider = MagicMock()
        expected_df = pd.DataFrame({'tradePrice': [100]})
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.query_data_async = AsyncMock(return_value=[{'tradePrice': 100}])
        mock_provider.construct_dataframe_with_types.return_value = expected_df
        ds = Dataset('TEST', provider=mock_provider)
        result = await ds.get_data_async(fields=['tradePrice'])
        assert result.equals(expected_df)


class TestBuildDataSeriesQuery:
    """Tests for Dataset._build_data_series_query."""

    def test_field_as_string(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.symbol_dimensions.return_value = ('assetId',)
        ds = Dataset('TEST', provider=mock_provider)
        field_value, query, symbol_dimension = ds._build_data_series_query(
            field='tradePrice', start=dt.date(2020, 1, 1),
            end=dt.date(2020, 1, 31), as_of=None, since=None, dates=None,
        )
        assert field_value == 'tradePrice'
        assert symbol_dimension == 'assetId'

    def test_field_as_enum(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.symbol_dimensions.return_value = ('assetId',)
        ds = Dataset('TEST', provider=mock_provider)
        field_value, query, symbol_dimension = ds._build_data_series_query(
            field=Fields.TRADE_PRICE, start=None, end=None,
            as_of=None, since=None, dates=None,
        )
        assert field_value == 'tradePrice'

    def test_symbol_dimensions_multiple_raises(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.symbol_dimensions.return_value = ('assetId', 'region')
        ds = Dataset('TEST', provider=mock_provider)
        with pytest.raises(MqValueError, match='get_data_series only valid'):
            ds._build_data_series_query(
                field='tradePrice', start=None, end=None,
                as_of=None, since=None, dates=None,
            )

    def test_symbol_dimensions_empty_raises(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.symbol_dimensions.return_value = ()
        ds = Dataset('TEST', provider=mock_provider)
        with pytest.raises(MqValueError, match='get_data_series only valid'):
            ds._build_data_series_query(
                field='tradePrice', start=None, end=None,
                as_of=None, since=None, dates=None,
            )


class TestBuildDataSeries:
    """Tests for Dataset._build_data_series -- covers GsDataApi isinstance check, empty df, function fields."""

    def test_empty_dataframe_returns_empty_series(self):
        mock_provider = MagicMock()
        mock_provider.construct_dataframe_with_types.return_value = pd.DataFrame()
        ds = Dataset('TEST', provider=mock_provider)
        result = ds._build_data_series([], 'tradePrice', 'assetId', False)
        assert isinstance(result, pd.Series)
        assert result.empty
        assert result.dtype == float

    def test_single_group_succeeds_with_gs_provider(self, mocker):
        """When provider is a GsDataApi instance and data has a single group, no error is raised."""
        mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=test_types)
        mock_provider = MagicMock(spec=GsDataApi)
        mock_provider.construct_dataframe_with_types.side_effect = GsDataApi.construct_dataframe_with_types

        ds = Dataset('TEST', provider=mock_provider)
        result = ds._build_data_series(test_data, 'tradePrice', 'assetId', False)
        assert isinstance(result, pd.Series)
        assert len(result) == 6

    def test_multiple_groups_raises_with_gs_provider(self, mocker):
        """When provider is an instance of GsDataApi and data has multiple groups, should raise."""
        mocker.patch("gs_quant.api.gs.data.GsDataApi.get_types", return_value=test_types)
        # Create data with two different assetIds
        multi_data = []
        for i in range(3):
            row = copy.deepcopy(test_data[0])
            row['date'] = dt.date(2019, 1, 2 + i)
            row['assetId'] = 'ASSET_A'
            multi_data.append(row)
        for i in range(3):
            row = copy.deepcopy(test_data[0])
            row['date'] = dt.date(2019, 1, 5 + i)
            row['assetId'] = 'ASSET_B'
            multi_data.append(row)

        # Use a mock that passes isinstance(provider, GsDataApi) check
        mock_provider = MagicMock(spec=GsDataApi)
        # Delegate construct_dataframe_with_types to actual implementation
        mock_provider.construct_dataframe_with_types.side_effect = GsDataApi.construct_dataframe_with_types
        ds = Dataset('TEST', provider=mock_provider)
        with pytest.raises(MqValueError, match='Not a series for a single'):
            ds._build_data_series(multi_data, 'tradePrice', 'assetId', False)

    def test_non_gs_provider_skips_groupby_check(self):
        mock_provider = MagicMock()
        df = pd.DataFrame({
            'tradePrice': [100, 200],
            'assetId': ['A', 'B'],
        })
        df.index = pd.DatetimeIndex([dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 2)])
        mock_provider.construct_dataframe_with_types.return_value = df
        ds = Dataset('TEST', provider=mock_provider)
        # Should not raise even though there are multiple assets
        result = ds._build_data_series([], 'tradePrice', 'assetId', False)
        assert isinstance(result, pd.Series)
        assert len(result) == 2

    def test_function_field_parentheses_replaced(self):
        mock_provider = MagicMock()
        df = pd.DataFrame({
            'difference_tradePrice': [10.0, 20.0],
        })
        df.index = pd.DatetimeIndex([dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 2)])
        mock_provider.construct_dataframe_with_types.return_value = df
        ds = Dataset('TEST', provider=mock_provider)
        result = ds._build_data_series([], 'difference(tradePrice)', 'assetId', False)
        assert isinstance(result, pd.Series)
        assert len(result) == 2

    def test_gs_provider_single_group_then_empty_check(self):
        """When provider is GsDataApi instance, single group, then df.empty check path."""
        mock_provider = MagicMock(spec=GsDataApi)
        mock_provider.construct_dataframe_with_types.return_value = pd.DataFrame(
            {'tradePrice': [100.0], 'assetId': ['A']},
            index=pd.DatetimeIndex([dt.datetime(2020, 1, 1)]),
        )
        ds = Dataset('TEST', provider=mock_provider)
        result = ds._build_data_series([], 'tradePrice', 'assetId', False)
        assert isinstance(result, pd.Series)
        assert len(result) == 1

    def test_gs_provider_empty_df_after_groupby(self):
        """When provider is GsDataApi instance and df is empty, returns empty Series."""
        mock_provider = MagicMock(spec=GsDataApi)
        empty_df = pd.DataFrame(columns=['tradePrice', 'assetId'])
        mock_provider.construct_dataframe_with_types.return_value = empty_df
        ds = Dataset('TEST', provider=mock_provider)
        result = ds._build_data_series([], 'tradePrice', 'assetId', False)
        assert isinstance(result, pd.Series)
        assert result.empty
        assert result.dtype == float

    def test_standard_fields_passed_through(self):
        mock_provider = MagicMock()
        df = pd.DataFrame({'val': [1.0]})
        df.index = pd.DatetimeIndex([dt.datetime(2020, 1, 1)])
        mock_provider.construct_dataframe_with_types.return_value = df
        ds = Dataset('TEST', provider=mock_provider)
        ds._build_data_series([], 'val', 'assetId', True)
        mock_provider.construct_dataframe_with_types.assert_called_once_with('TEST', [], standard_fields=True)


class TestGetDataSeries:
    """Tests for Dataset.get_data_series end-to-end."""

    def test_get_data_series_with_dates(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.symbol_dimensions.return_value = ('assetId',)
        mock_provider.query_data.return_value = []
        df = pd.DataFrame()
        mock_provider.construct_dataframe_with_types.return_value = df
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_data_series('tradePrice', dates=[dt.date(2020, 1, 1)])
        assert isinstance(result, pd.Series)
        assert result.empty


class TestGetDataSeriesAsync:
    """Tests for Dataset.get_data_series_async."""

    @pytest.mark.asyncio
    async def test_get_data_series_async_basic(self):
        mock_provider = MagicMock()
        mock_provider.build_query.return_value = MagicMock()
        mock_provider.symbol_dimensions.return_value = ('assetId',)
        mock_provider.query_data_async = AsyncMock(return_value=[])
        empty_df = pd.DataFrame()
        mock_provider.construct_dataframe_with_types.return_value = empty_df
        ds = Dataset('TEST', provider=mock_provider)
        result = await ds.get_data_series_async('tradePrice', start=dt.date(2020, 1, 1))
        assert isinstance(result, pd.Series)
        assert result.empty


class TestGetDataLast:
    """Tests for Dataset.get_data_last."""

    def test_get_data_last_basic(self):
        mock_provider = MagicMock()
        mock_query = MagicMock()
        mock_provider.build_query.return_value = mock_query
        mock_provider.last_data.return_value = [{'price': 100}]
        expected_df = pd.DataFrame({'price': [100]})
        mock_provider.construct_dataframe_with_types.return_value = expected_df
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_data_last(as_of=dt.date(2020, 1, 31))
        assert result.equals(expected_df)
        # Verify format is set to None
        assert mock_query.format is None
        mock_provider.build_query.assert_called_once()
        # Check format='JSON' is passed in build_query
        call_kwargs = mock_provider.build_query.call_args
        assert call_kwargs[1]['format'] == 'JSON'

    def test_get_data_last_with_kwargs(self):
        mock_provider = MagicMock()
        mock_query = MagicMock()
        mock_provider.build_query.return_value = mock_query
        mock_provider.last_data.return_value = []
        mock_provider.construct_dataframe_with_types.return_value = pd.DataFrame()
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_data_last(as_of=dt.datetime(2020, 1, 31), start=dt.date(2020, 1, 1), city='Boston')
        assert isinstance(result, pd.DataFrame)

    def test_get_data_last_with_standard_fields(self):
        mock_provider = MagicMock()
        mock_query = MagicMock()
        mock_provider.build_query.return_value = mock_query
        mock_provider.last_data.return_value = [{'price': 50}]
        mock_provider.construct_dataframe_with_types.return_value = pd.DataFrame({'price': [50]})
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_data_last(as_of=dt.date(2020, 6, 1), standard_fields=True)
        mock_provider.construct_dataframe_with_types.assert_called_once_with('TEST', [{'price': 50}], standard_fields=True)


class TestGetCoverage:
    """Tests for Dataset.get_coverage."""

    def test_get_coverage_basic(self):
        mock_provider = MagicMock()
        mock_provider.get_coverage.return_value = [{'assetId': 'A'}, {'assetId': 'B'}]
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_coverage()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_get_coverage_with_params(self):
        mock_provider = MagicMock()
        mock_provider.get_coverage.return_value = [{'assetId': 'A'}]
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_coverage(limit=10, offset=5, fields=['assetId'], include_history=True)
        mock_provider.get_coverage.assert_called_once_with(
            'TEST', limit=10, offset=5, fields=['assetId'], include_history=True
        )

    def test_get_coverage_empty(self):
        mock_provider = MagicMock()
        mock_provider.get_coverage.return_value = []
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.get_coverage()
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestGetCoverageAsync:
    """Tests for Dataset.get_coverage_async."""

    @pytest.mark.asyncio
    async def test_get_coverage_async_basic(self):
        mock_provider = MagicMock()
        mock_provider.get_coverage_async = AsyncMock(return_value=[{'assetId': 'A'}])
        ds = Dataset('TEST', provider=mock_provider)
        result = await ds.get_coverage_async()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_coverage_async_with_params(self):
        mock_provider = MagicMock()
        mock_provider.get_coverage_async = AsyncMock(return_value=[])
        ds = Dataset('TEST', provider=mock_provider)
        result = await ds.get_coverage_async(limit=5, offset=0, fields=['bbid'], include_history=True)
        mock_provider.get_coverage_async.assert_called_once_with(
            'TEST', limit=5, offset=0, fields=['bbid'], include_history=True
        )


class TestDeleteUndeleteUpload:
    """Tests for Dataset.delete, undelete, delete_data, upload_data."""

    def test_delete(self):
        mock_provider = MagicMock()
        mock_provider.delete_dataset.return_value = {'status': 'ok'}
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.delete()
        assert result == {'status': 'ok'}
        mock_provider.delete_dataset.assert_called_once_with('TEST')

    def test_undelete(self):
        mock_provider = MagicMock()
        mock_provider.undelete_dataset.return_value = {'status': 'ok'}
        ds = Dataset('TEST', provider=mock_provider)
        result = ds.undelete()
        assert result == {'status': 'ok'}
        mock_provider.undelete_dataset.assert_called_once_with('TEST')

    def test_delete_data(self):
        mock_provider = MagicMock()
        mock_provider.delete_data.return_value = {'deleted': 5}
        ds = Dataset('TEST', provider=mock_provider)
        query = {'startDate': dt.date(2020, 1, 1), 'endDate': dt.date(2020, 1, 31), 'deleteAll': True}
        result = ds.delete_data(query)
        assert result == {'deleted': 5}
        mock_provider.delete_data.assert_called_once_with('TEST', query)

    def test_upload_data_with_list(self):
        mock_provider = MagicMock()
        mock_provider.upload_data.return_value = {'uploaded': 1}
        ds = Dataset('TEST', provider=mock_provider)
        data = [{'date': '2020-01-01', 'price': 100}]
        result = ds.upload_data(data)
        assert result == {'uploaded': 1}
        mock_provider.upload_data.assert_called_once_with('TEST', data)

    def test_upload_data_with_dataframe(self):
        mock_provider = MagicMock()
        mock_provider.upload_data.return_value = {'uploaded': 2}
        ds = Dataset('TEST', provider=mock_provider)
        df = pd.DataFrame({'price': [100, 200]})
        result = ds.upload_data(df)
        assert result == {'uploaded': 2}

    def test_upload_data_with_tuple(self):
        mock_provider = MagicMock()
        mock_provider.upload_data.return_value = {'uploaded': 1}
        ds = Dataset('TEST', provider=mock_provider)
        data = ({'date': '2020-01-01', 'price': 100},)
        result = ds.upload_data(data)
        mock_provider.upload_data.assert_called_once_with('TEST', data)


class TestGetDataBulk:
    """Tests for Dataset.get_data_bulk -- covers time_field branches, write_to_csv, handler."""

    def _make_dataset_definition(self, time_field='date', history_date=None):
        definition = DataSetEntity()
        definition.parameters = DataSetParameters()
        definition.parameters.history_date = history_date or dt.datetime(2017, 1, 2)
        definition.dimensions = DataSetDimensions()
        definition.dimensions.time_field = time_field
        return definition

    def test_get_data_bulk_time_field_time(self, mocker):
        """Test the time_field == 'time' branch."""
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        definition = self._make_dataset_definition(
            time_field='time',
            history_date=dt.datetime(2017, 1, 2, tzinfo=dt.timezone.utc),
        )
        mocker.patch("gs_quant.api.gs.data.GsDataApi.symbol_dimensions", return_value=('region',))
        mocker.patch("gs_quant.api.gs.data.GsDataApi.get_definition", return_value=definition)

        coverage = pd.DataFrame({'region': ['US']})
        mocker.patch("gs_quant.data.dataset.Dataset.get_coverage", return_value=coverage)

        mock_fetch = mocker.patch("gs_quant.data.utilities.Utilities.fetch_data",
                                  return_value=pd.DataFrame({'val': [1]}))

        received = []

        def handler(df):
            received.append(df)

        ds = Dataset('TEST')
        original_start = dt.datetime(2023, 3, 2, 10, 0, 0, tzinfo=dt.timezone.utc)
        final_end = dt.datetime(2023, 3, 2, 11, 0, 0, tzinfo=dt.timezone.utc)
        ds.get_data_bulk(
            request_batch_size=4,
            original_start=original_start,
            final_end=final_end,
            identifier='region',
            datetime_delta_override=2,
            handler=handler,
        )
        assert mock_fetch.call_count >= 1

    def test_get_data_bulk_with_datetime_delta_override_date(self, mocker):
        """Test date time_field with datetime_delta_override."""
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        definition = self._make_dataset_definition(time_field='date')
        mocker.patch("gs_quant.api.gs.data.GsDataApi.symbol_dimensions", return_value=('region',))
        mocker.patch("gs_quant.api.gs.data.GsDataApi.get_definition", return_value=definition)

        coverage = pd.DataFrame({'region': ['US']})
        mocker.patch("gs_quant.data.dataset.Dataset.get_coverage", return_value=coverage)

        mock_fetch = mocker.patch("gs_quant.data.utilities.Utilities.fetch_data",
                                  return_value=pd.DataFrame({'val': [1]}))

        received = []

        def handler(df):
            received.append(df)

        ds = Dataset('TEST')
        ds.get_data_bulk(
            request_batch_size=2,
            original_start=dt.datetime(2023, 3, 2),
            final_end=dt.datetime(2023, 3, 2),
            identifier='region',
            datetime_delta_override=10,
            handler=handler,
        )
        assert mock_fetch.call_count >= 1

    def test_get_data_bulk_no_final_end(self, mocker):
        """Test that final_end defaults to datetime.now()."""
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        definition = self._make_dataset_definition(time_field='date')
        mocker.patch("gs_quant.api.gs.data.GsDataApi.symbol_dimensions", return_value=('region',))
        mocker.patch("gs_quant.api.gs.data.GsDataApi.get_definition", return_value=definition)

        coverage = pd.DataFrame({'region': ['US']})
        mocker.patch("gs_quant.data.dataset.Dataset.get_coverage", return_value=coverage)

        mock_fetch = mocker.patch("gs_quant.data.utilities.Utilities.fetch_data",
                                  return_value=pd.DataFrame({'val': [1]}))

        received = []

        def handler(df):
            received.append(df)

        ds = Dataset('TEST')
        ds.get_data_bulk(
            request_batch_size=1,
            original_start=dt.datetime(2023, 3, 2),
            final_end=None,
            identifier='region',
            handler=handler,
        )
        assert mock_fetch.call_count >= 1

    def test_get_data_bulk_authenticate_fallback(self, mocker):
        """Test the except AttributeError branch where GsSession.current has no client_id.

        The code does:
            try:
                authenticate = partial(GsSession.use, client_id=GsSession.current.client_id, ...)
            except AttributeError:
                authenticate = partial(GsSession.use)

        We trigger the AttributeError by temporarily replacing the `current` property getter.
        """
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        # Delete client_id from the current session instance to trigger AttributeError
        session = GsSession.current
        original_client_id = session.client_id
        del session.client_id
        mocker.patch('gs_quant.data.dataset.GsSession.use')

        definition = self._make_dataset_definition(time_field='date')
        mocker.patch("gs_quant.api.gs.data.GsDataApi.symbol_dimensions", return_value=('region',))
        mocker.patch("gs_quant.api.gs.data.GsDataApi.get_definition", return_value=definition)

        coverage = pd.DataFrame({'region': ['US']})
        mocker.patch("gs_quant.data.dataset.Dataset.get_coverage", return_value=coverage)

        mock_fetch = mocker.patch("gs_quant.data.utilities.Utilities.fetch_data",
                                  return_value=pd.DataFrame({'val': [1]}))

        received = []

        def handler(df):
            received.append(df)

        ds = Dataset('TEST')
        try:
            ds.get_data_bulk(
                request_batch_size=1,
                original_start=dt.datetime(2023, 3, 2),
                final_end=dt.datetime(2023, 3, 2),
                identifier='region',
                handler=handler,
            )
        finally:
            # Restore client_id to avoid polluting other tests
            session.client_id = original_client_id
        assert mock_fetch.call_count >= 1

    def test_get_data_bulk_write_to_csv(self, mocker):
        """Test the write_to_csv branch (handler=None)."""
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        definition = self._make_dataset_definition(time_field='date')
        mocker.patch("gs_quant.api.gs.data.GsDataApi.symbol_dimensions", return_value=('region',))
        mocker.patch("gs_quant.api.gs.data.GsDataApi.get_definition", return_value=definition)

        coverage = pd.DataFrame({'region': ['US']})
        mocker.patch("gs_quant.data.dataset.Dataset.get_coverage", return_value=coverage)

        mock_fetch = mocker.patch("gs_quant.data.utilities.Utilities.fetch_data",
                                  return_value=pd.DataFrame({'val': [1]}))
        # Mock write_consolidated_results to avoid file I/O and pandas to_csv compat issue
        mock_write = mocker.patch("gs_quant.data.utilities.Utilities.write_consolidated_results")
        mocker.patch("gs_quant.data.utilities.Utilities.pre_checks",
                     return_value=(dt.datetime(2023, 3, 2), '/tmp/test_dir'))

        ds = Dataset('TEST')
        ds.get_data_bulk(
            request_batch_size=1,
            original_start=dt.datetime(2023, 3, 2),
            final_end=dt.datetime(2023, 3, 2),
            identifier='region',
            handler=None,  # write_to_csv = True
        )
        assert mock_fetch.call_count >= 1
        # Verify the write_to_csv path was taken (write_consolidated_results called)
        assert mock_write.call_count >= 1

    def test_get_data_bulk_time_field_time_no_override(self, mocker):
        """Test the time_field == 'time' branch with datetime_delta_override=None (uses default timedelta)."""
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        definition = self._make_dataset_definition(
            time_field='time',
            history_date=dt.datetime(2017, 1, 2, tzinfo=dt.timezone.utc),
        )
        mocker.patch("gs_quant.api.gs.data.GsDataApi.symbol_dimensions", return_value=('region',))
        mocker.patch("gs_quant.api.gs.data.GsDataApi.get_definition", return_value=definition)

        coverage = pd.DataFrame({'region': ['US']})
        mocker.patch("gs_quant.data.dataset.Dataset.get_coverage", return_value=coverage)

        mock_fetch = mocker.patch("gs_quant.data.utilities.Utilities.fetch_data",
                                  return_value=pd.DataFrame({'val': [1]}))

        received = []

        def handler(df):
            received.append(df)

        ds = Dataset('TEST')
        original_start = dt.datetime(2023, 3, 2, 10, 0, 0, tzinfo=dt.timezone.utc)
        final_end = dt.datetime(2023, 3, 2, 11, 0, 0, tzinfo=dt.timezone.utc)
        ds.get_data_bulk(
            request_batch_size=4,
            original_start=original_start,
            final_end=final_end,
            identifier='region',
            datetime_delta_override=None,  # Uses default timedelta
            handler=handler,
        )
        assert mock_fetch.call_count >= 1

    def test_get_data_bulk_multiple_coverage_batches(self, mocker):
        """Test with symbols_per_csv < coverage length to get multiple batches."""
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        definition = self._make_dataset_definition(time_field='date')
        mocker.patch("gs_quant.api.gs.data.GsDataApi.symbol_dimensions", return_value=('region',))
        mocker.patch("gs_quant.api.gs.data.GsDataApi.get_definition", return_value=definition)

        coverage = pd.DataFrame({'region': ['US', 'EU', 'APAC']})
        mocker.patch("gs_quant.data.dataset.Dataset.get_coverage", return_value=coverage)

        mock_fetch = mocker.patch("gs_quant.data.utilities.Utilities.fetch_data",
                                  return_value=pd.DataFrame({'val': [1]}))

        received = []

        def handler(df):
            received.append(df)

        ds = Dataset('TEST')
        ds.get_data_bulk(
            request_batch_size=1,
            original_start=dt.datetime(2023, 3, 2),
            final_end=dt.datetime(2023, 3, 2),
            identifier='region',
            symbols_per_csv=2,
            handler=handler,
        )
        # Should process two batches (2 + 1)
        assert len(received) >= 2


# =====================================================================
# PTPDataset tests
# =====================================================================

class TestPTPDataset:
    """Tests for PTPDataset -- construction, sync, plot, delete."""

    def test_init_with_series(self):
        s = pd.Series([1.0, 2.0, 3.0],
                       index=pd.date_range('2021-01-01', periods=3, freq='D'),
                       name='values')
        ds = PTPDataset(s)
        assert isinstance(ds._series, pd.DataFrame)
        assert 'values' in ds._series.columns

    def test_init_with_series_attrs_name(self):
        s = pd.Series([1.0, 2.0],
                       index=pd.date_range('2021-01-01', periods=2, freq='D'))
        s.attrs['name'] = 'myfield'
        ds = PTPDataset(s)
        assert 'myfield' in ds._series.columns

    def test_init_with_series_no_name(self):
        s = pd.Series([1.0, 2.0],
                       index=pd.date_range('2021-01-01', periods=2, freq='D'))
        ds = PTPDataset(s)
        assert 'values' in ds._series.columns

    def test_init_with_dataframe(self):
        df = pd.DataFrame({'val': [1.0, 2.0]},
                           index=pd.date_range('2021-01-01', periods=2, freq='D'))
        ds = PTPDataset(df)
        assert ds._series.equals(df)

    def test_init_requires_datetime_index(self):
        df = pd.DataFrame({'val': [1.0, 2.0]}, index=[0, 1])
        with pytest.raises(MqValueError, match='DatetimeIndex'):
            PTPDataset(df)

    def test_init_rejects_non_numeric_columns(self):
        df = pd.DataFrame({'val': ['a', 'b']},
                           index=pd.date_range('2021-01-01', periods=2, freq='D'))
        with pytest.raises(MqValueError, match='only numbers'):
            PTPDataset(df)

    def test_init_with_name(self):
        s = pd.Series([1.0], index=pd.date_range('2021-01-01', periods=1, freq='D'))
        ds = PTPDataset(s, name='My Dataset')
        assert ds._name == 'My Dataset'

    def test_init_with_no_name(self):
        s = pd.Series([1.0], index=pd.date_range('2021-01-01', periods=1, freq='D'))
        ds = PTPDataset(s)
        assert ds._name is None

    def test_sync(self, mocker):
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        mock_post = mocker.patch.object(
            GsSession.current.sync, 'post',
            return_value={
                'fieldMap': {'col1': 'tradePrice', 'col2': 'date', 'col3': 'updateTime', 'col4': 'datasetId'},
                'dataset': {'id': 'DS123'},
            }
        )
        s = pd.Series([1.0, 2.0],
                       index=pd.date_range('2021-01-01', periods=2, freq='D'))
        ds = PTPDataset(s)
        ds.sync()
        assert ds.id == 'DS123'
        assert 'col1' in ds._fields
        # date, updateTime, datasetId should be excluded
        assert 'col2' not in ds._fields
        assert 'col3' not in ds._fields
        assert 'col4' not in ds._fields
        mock_post.assert_called_once()

    def test_sync_with_name(self, mocker):
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        mocker.patch.object(
            GsSession.current.sync, 'post',
            return_value={
                'fieldMap': {'col1': 'tradePrice'},
                'dataset': {'id': 'DS456'},
            }
        )
        s = pd.Series([1.0], index=pd.date_range('2021-01-01', periods=1, freq='D'))
        ds = PTPDataset(s, name='Custom Name')
        ds.sync()
        assert ds.id == 'DS456'

    def test_sync_with_default_name(self, mocker):
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        mock_post = mocker.patch.object(
            GsSession.current.sync, 'post',
            return_value={
                'fieldMap': {},
                'dataset': {'id': 'DS789'},
            }
        )
        s = pd.Series([1.0], index=pd.date_range('2021-01-01', periods=1, freq='D'))
        ds = PTPDataset(s)
        ds.sync()
        # Should use 'GSQ Default' since _name is None
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]['payload']['name'] == 'GSQ Default'

    def test_plot_single_field(self, mocker):
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        mocker.patch.object(
            GsSession.current.sync, 'post',
            return_value={
                'fieldMap': {'col1': 'tradePrice'},
                'dataset': {'id': 'DS123'},
            }
        )
        mock_open = mocker.patch('gs_quant.data.dataset.webbrowser.open')
        s = pd.Series([1.0], index=pd.date_range('2021-01-01', periods=1, freq='D'))
        ds = PTPDataset(s)
        ds.sync()
        expression = ds.plot(open_in_browser=True)
        assert 'DS123' in expression
        assert 'trade_price' in expression
        mock_open.assert_called_once()

    def test_plot_without_browser(self, mocker):
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        mocker.patch.object(
            GsSession.current.sync, 'post',
            return_value={
                'fieldMap': {'col1': 'tradePrice'},
                'dataset': {'id': 'DS123'},
            }
        )
        mock_open = mocker.patch('gs_quant.data.dataset.webbrowser.open')
        s = pd.Series([1.0], index=pd.date_range('2021-01-01', periods=1, freq='D'))
        ds = PTPDataset(s)
        ds.sync()
        expression = ds.plot(open_in_browser=False)
        assert 'DS123' in expression
        mock_open.assert_not_called()

    def test_plot_with_specific_field(self, mocker):
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        mocker.patch.object(
            GsSession.current.sync, 'post',
            return_value={
                'fieldMap': {'col1': 'tradePrice', 'col2': 'bidPrice'},
                'dataset': {'id': 'DS123'},
            }
        )
        mocker.patch('gs_quant.data.dataset.webbrowser.open')
        s = pd.Series([1.0], index=pd.date_range('2021-01-01', periods=1, freq='D'))
        ds = PTPDataset(s)
        ds.sync()
        expression = ds.plot(open_in_browser=False, field='myField')
        assert 'my_field' in expression

    def test_plot_multiple_fields(self, mocker):
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        mocker.patch.object(
            GsSession.current.sync, 'post',
            return_value={
                'fieldMap': {'col1': 'val1', 'col2': 'val2'},
                'dataset': {'id': 'DS123'},
            }
        )
        mocker.patch('gs_quant.data.dataset.webbrowser.open')
        df = pd.DataFrame({'val1': [1.0], 'val2': [2.0]},
                           index=pd.date_range('2021-01-01', periods=1, freq='D'))
        ds = PTPDataset(df)
        ds.sync()
        expression = ds.plot(open_in_browser=False)
        # Should contain both fields joined
        assert 'DS123' in expression
        assert '%0A' in expression or 'val' in expression


# =====================================================================
# MarqueeDataIngestionLibrary tests
# =====================================================================

class TestMarqueeDataIngestionLibrary:
    """Tests for MarqueeDataIngestionLibrary."""

    def _make_lib(self, mocker, internal=True, drg_name='TestOrg'):
        mocker.patch.object(
            GsSession.__class__, 'default_value',
            return_value=GsSession.get(Environment.QA, 'client_id', 'secret')
        )
        user_info = {'internal': internal, 'drgName': drg_name}
        mocker.patch('gs_quant.data.dataset.GsUsersApi.get_current_user_info', return_value=user_info)
        mocker.patch('gs_quant.data.dataset.GsUsersApi.get_current_app_managers', return_value=['guid:mgr1'])
        return MarqueeDataIngestionLibrary()

    def test_provider_default(self, mocker):
        lib = self._make_lib(mocker)
        assert lib.provider == GsDataApi

    def test_provider_custom(self, mocker):
        mocker.patch('gs_quant.data.dataset.GsUsersApi.get_current_user_info', return_value={})
        mocker.patch('gs_quant.data.dataset.GsUsersApi.get_current_app_managers', return_value=[])
        mock_prov = MagicMock()
        lib = MarqueeDataIngestionLibrary(provider=mock_prov)
        assert lib.provider == mock_prov

    def test_to_upper_underscore(self, mocker):
        lib = self._make_lib(mocker)
        assert lib.to_upper_underscore('camelCase') == 'CAMEL_CASE'
        assert lib.to_upper_underscore('simple') == 'SIMPLE'

    def test_to_camel_case(self, mocker):
        lib = self._make_lib(mocker)
        assert lib.to_camel_case('some_field') == 'someField'

    def test_create_parameters_internal(self, mocker):
        lib = self._make_lib(mocker, internal=True)
        params = lib._create_parameters('date', 'bbid', True)
        assert params.frequency == 'Daily'
        assert params.snowflake_config.db == 'TIMESERIES'

    def test_create_parameters_external(self, mocker):
        lib = self._make_lib(mocker, internal=False)
        params = lib._create_parameters('date', 'bbid', False)
        assert params.snowflake_config.db == 'EXTERNAL'

    def test_create_dataset_empty_data_raises(self, mocker):
        lib = self._make_lib(mocker)
        with pytest.raises(InvalidInputException):
            lib.create_dataset(pd.DataFrame(), 'DS1', 'bbid', 'date')

    def test_create_dataset_empty_dataset_id_raises(self, mocker):
        lib = self._make_lib(mocker)
        df = pd.DataFrame({'date': ['2023-01-01'], 'bbid': ['AAPL'], 'price': [100.0]})
        with pytest.raises(InvalidInputException):
            lib.create_dataset(df, '', 'bbid', 'date')

    def test_create_dataset_empty_symbol_dimension_raises(self, mocker):
        lib = self._make_lib(mocker)
        df = pd.DataFrame({'date': ['2023-01-01'], 'bbid': ['AAPL'], 'price': [100.0]})
        with pytest.raises(InvalidInputException):
            lib.create_dataset(df, 'DS1', '', 'date')

    def test_create_dataset_empty_time_dimension_raises(self, mocker):
        lib = self._make_lib(mocker)
        df = pd.DataFrame({'date': ['2023-01-01'], 'bbid': ['AAPL'], 'price': [100.0]})
        with pytest.raises(InvalidInputException):
            lib.create_dataset(df, 'DS1', 'bbid', '')

    def test_create_dataset_intraday_raises(self, mocker):
        lib = self._make_lib(mocker)
        df = pd.DataFrame({
            'date': ['2023-01-01 10:30:00'],
            'bbid': ['AAPL'],
            'price': [100.0],
        })
        with pytest.raises(InvalidInputException, match="intraday"):
            lib.create_dataset(df, 'DS1', 'bbid', 'date')

    def test_create_dataset_internal_user(self, mocker):
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])
        mocker.patch.object(GsDataApi, 'create_dataset_fields', return_value=None)
        mock_create = mocker.patch.object(GsDataApi, 'create', return_value=DataSetEntity())
        mocker.patch.object(GsDataApi, 'get_catalog_url', return_value='http://example.com')

        df = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'bbid': ['AAPL', 'GOOG'],
            'price': [150.0, 2800.0],
        })
        result = lib.create_dataset(df, 'TEST_DS', 'bbid', 'date')
        assert isinstance(result, DataSetEntity)
        mock_create.assert_called_once()

    def test_create_dataset_external_user_valid_symbol(self, mocker):
        lib = self._make_lib(mocker, internal=False, drg_name='TestOrg Inc.')
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])
        mocker.patch.object(GsDataApi, 'create_dataset_fields', return_value=None)
        mock_create = mocker.patch.object(GsDataApi, 'create', return_value=DataSetEntity())
        mocker.patch.object(GsDataApi, 'get_catalog_url', return_value='http://example.com')

        df = pd.DataFrame({
            'date': ['2023-01-01'],
            'bbid': ['AAPL'],
            'price': [150.0],
        })
        result = lib.create_dataset(df, 'TEST_DS', 'bbid', 'date')
        assert isinstance(result, DataSetEntity)

    def test_create_dataset_external_user_custom_symbol(self, mocker):
        """Test that non-standard symbol dimensions map to 'customId' for external users."""
        lib = self._make_lib(mocker, internal=False, drg_name='TestOrg')
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])
        mocker.patch.object(GsDataApi, 'create_dataset_fields', return_value=None)
        mock_create = mocker.patch.object(GsDataApi, 'create', return_value=DataSetEntity())
        mocker.patch.object(GsDataApi, 'get_catalog_url', return_value='http://example.com')

        df = pd.DataFrame({
            'date': ['2023-01-01'],
            'myCustomId': ['X1'],
            'price': [150.0],
        })
        result = lib.create_dataset(df, 'TEST_DS', 'myCustomId', 'date')
        assert isinstance(result, DataSetEntity)
        # The entity passed to create should have customId as symbol dimension
        call_args = mock_create.call_args[0][0]
        assert 'customId' in call_args.dimensions.symbol_dimensions

    def test_create_dataset_external_user_no_drg_name(self, mocker):
        """Test that missing drgName raises InvalidInputException."""
        lib = self._make_lib(mocker, internal=False, drg_name=None)
        # Override user to have no drgName
        lib.user = {'internal': False, 'drgName': None}
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])

        df = pd.DataFrame({
            'date': ['2023-01-01'],
            'bbid': ['AAPL'],
            'price': [150.0],
        })
        with pytest.raises(InvalidInputException, match='drgName'):
            lib.create_dataset(df, 'TEST_DS', 'bbid', 'date')

    def test_create_dataset_with_dimensions(self, mocker):
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])
        mocker.patch.object(GsDataApi, 'create_dataset_fields', return_value=None)
        mock_create = mocker.patch.object(GsDataApi, 'create', return_value=DataSetEntity())
        mocker.patch.object(GsDataApi, 'get_catalog_url', return_value='http://example.com')

        df = pd.DataFrame({
            'date': ['2023-01-01'],
            'bbid': ['AAPL'],
            'sector': ['Tech'],
            'price': [150.0],
        })
        result = lib.create_dataset(df, 'TEST_DS', 'bbid', 'date', dimensions=['sector'])
        assert isinstance(result, DataSetEntity)

    def test_create_dataset_too_many_measures_raises(self, mocker):
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])

        # Create a df with 26 measure columns
        cols = {'date': ['2023-01-01'], 'bbid': ['AAPL']}
        for i in range(26):
            cols[f'measure_{i}'] = [float(i)]
        df = pd.DataFrame(cols)

        with pytest.raises(ValueError, match='exceeds the allowed limit'):
            lib.create_dataset(df, 'TEST_DS', 'bbid', 'date')

    def test_create_dataset_custom_time_dimension(self, mocker):
        """Test that a non-standard time dimension maps to 'date'."""
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])
        mocker.patch.object(GsDataApi, 'create_dataset_fields', return_value=None)
        mock_create = mocker.patch.object(GsDataApi, 'create', return_value=DataSetEntity())
        mocker.patch.object(GsDataApi, 'get_catalog_url', return_value='http://example.com')

        df = pd.DataFrame({
            'myDate': ['2023-01-01'],
            'bbid': ['AAPL'],
            'price': [150.0],
        })
        result = lib.create_dataset(df, 'TEST_DS', 'bbid', 'myDate')
        assert isinstance(result, DataSetEntity)
        call_args = mock_create.call_args[0][0]
        assert call_args.dimensions.time_field == 'date'

    def test_check_and_create_field_existing(self, mocker):
        """When all fields already exist, no create call is made."""
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[MagicMock()])
        mock_create = mocker.patch.object(GsDataApi, 'create_dataset_fields')

        df = pd.DataFrame({'col1': [1.0]})
        lib._check_and_create_field({'col1': 'field1'}, df)
        mock_create.assert_not_called()

    def test_check_and_create_field_new_number(self, mocker):
        """When a field doesn't exist and is numeric, it gets created."""
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])
        mock_create = mocker.patch.object(GsDataApi, 'create_dataset_fields')

        df = pd.DataFrame({'col1': [1.0]})
        lib._check_and_create_field({'col1': 'field1'}, df)
        mock_create.assert_called_once()
        created_fields = mock_create.call_args[0][0]
        assert len(created_fields) == 1
        assert created_fields[0].type_ == 'number'

    def test_check_and_create_field_new_string(self, mocker):
        """When a field doesn't exist and is a string type, it gets created."""
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])
        mock_create = mocker.patch.object(GsDataApi, 'create_dataset_fields')

        df = pd.DataFrame({'col1': ['abc']})
        lib._check_and_create_field({'col1': 'field1'}, df)
        mock_create.assert_called_once()
        created_fields = mock_create.call_args[0][0]
        assert len(created_fields) == 1
        assert created_fields[0].type_ == 'string'

    def test_check_and_create_field_integer(self, mocker):
        """When a field is integer type, it maps to 'number'."""
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[])
        mock_create = mocker.patch.object(GsDataApi, 'create_dataset_fields')

        df = pd.DataFrame({'col1': [1, 2, 3]})
        lib._check_and_create_field({'col1': 'field1'}, df)
        mock_create.assert_called_once()
        created_fields = mock_create.call_args[0][0]
        assert created_fields[0].type_ == 'number'

    def test_create_dimensions_internal(self, mocker):
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[MagicMock()])

        df = pd.DataFrame({'price': [100.0], 'sector': ['Tech']})
        dims = lib._create_dimensions(df, 'bbid', 'date', ['sector'], ['price'], True)
        assert dims.time_field == 'date'
        assert 'bbid' in dims.symbol_dimensions
        assert len(dims.non_symbol_dimensions) == 1
        assert len(dims.measures) == 1

    def test_create_dimensions_external(self, mocker):
        lib = self._make_lib(mocker, internal=False, drg_name='TestOrg')
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[MagicMock()])

        df = pd.DataFrame({'price': [100.0]})
        dims = lib._create_dimensions(df, 'bbid', 'date', [], ['price'], False)
        assert dims.time_field == 'date'
        assert len(dims.measures) == 1

    def test_create_dimensions_update_time_not_resolvable(self, mocker):
        """updateTime measure should have resolvable=None."""
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[MagicMock()])

        df = pd.DataFrame({'price': [100.0], 'updateTime': ['2023-01-01T00:00:00Z']})
        dims = lib._create_dimensions(df, 'bbid', 'date', [], ['price', 'updateTime'], True)
        update_time_measure = [m for m in dims.measures if m.field_ == 'updateTime']
        assert len(update_time_measure) == 1
        assert update_time_measure[0].resolvable is None

    def test_create_dimensions_non_update_time_resolvable(self, mocker):
        """Non-updateTime measures should have resolvable=True."""
        lib = self._make_lib(mocker, internal=True)
        mocker.patch.object(GsDataApi, 'get_dataset_fields', return_value=[MagicMock()])

        df = pd.DataFrame({'price': [100.0]})
        dims = lib._create_dimensions(df, 'bbid', 'date', [], ['price'], True)
        price_measure = [m for m in dims.measures if m.field_ == 'price']
        assert len(price_measure) == 1
        assert price_measure[0].resolvable is True

    def test_write_data_basic(self, mocker):
        lib = self._make_lib(mocker, internal=True)
        definition = DataSetEntity()
        definition.dimensions = DataSetDimensions()
        definition.dimensions.time_field = 'date'
        definition.dimensions.symbol_dimensions = ['bbid']
        definition.dimensions.non_symbol_dimensions = (
            FieldColumnPair(field_='sector', column='SECTOR'),
        )
        definition.dimensions.measures = (
            FieldColumnPair(field_='price', column='PRICE'),
        )
        definition.parameters = DataSetParameters()
        definition.parameters.snowflake_config = DBConfig()
        definition.parameters.snowflake_config.date_time_column = 'DATE'
        definition.parameters.snowflake_config.id_column = 'BBID'

        mocker.patch.object(GsDataApi, 'get_definition', return_value=definition)
        mock_upload = mocker.patch.object(GsDataApi, 'upload_data', return_value={'uploaded': 1})

        df = pd.DataFrame({
            'date': ['2023-01-01'],
            'bbid': ['AAPL'],
            'sector': ['Tech'],
            'price': [150.0],
        })
        lib.write_data(df, 'TEST_DS')
        mock_upload.assert_called_once()
        call_args = mock_upload.call_args
        assert call_args[0][0] == 'TEST_DS'

    def test_write_data_empty_df_raises(self, mocker):
        lib = self._make_lib(mocker, internal=True)
        with pytest.raises(ValueError, match='empty'):
            lib.write_data(pd.DataFrame(), 'TEST_DS')

    def test_write_data_empty_id_raises(self, mocker):
        lib = self._make_lib(mocker, internal=True)
        df = pd.DataFrame({'col': [1]})
        with pytest.raises(ValueError, match='Dataset ID'):
            lib.write_data(df, '')


class TestVendorEnums:
    """Test the Vendor enum subclasses."""

    def test_gs_vendor_values(self):
        assert Dataset.GS.WEATHER.value == 'WEATHER'
        assert Dataset.GS.HOLIDAY.value == 'HOLIDAY'
        assert Dataset.GS.MA_RANK.value == 'MA_RANK'
        assert Dataset.GS.CBGSSI.value == 'CBGSSI'
        assert Dataset.GS.CB.value == 'CB'
        assert Dataset.GS.STSLEVELS.value == 'STSLEVELS'
        assert Dataset.GS.CENTRAL_BANK_WATCH.value == 'CENTRAL_BANK_WATCH_PREMIUM'

    def test_tr_vendor_values(self):
        assert Dataset.TR.TREOD.value == 'TREOD'
        assert Dataset.TR.TR.value == 'TR'
        assert Dataset.TR.TR_FXSPOT.value == 'TR_FXSPOT'

    def test_fred_vendor_values(self):
        assert Dataset.FRED.GDP.value == 'GDP'

    def test_trading_economics_vendor_values(self):
        assert Dataset.TradingEconomics.MACRO_EVENTS_CALENDAR.value == 'MACRO_EVENTS_CALENDAR'

    def test_vendor_is_enum(self):
        assert issubclass(Dataset.GS, Dataset.Vendor)
        assert issubclass(Dataset.TR, Dataset.Vendor)
        assert issubclass(Dataset.FRED, Dataset.Vendor)
        assert issubclass(Dataset.TradingEconomics, Dataset.Vendor)

    def test_get_dataset_id_str_with_vendor(self):
        ds = Dataset('X')
        assert ds._get_dataset_id_str(Dataset.GS.WEATHER) == 'WEATHER'

    def test_get_dataset_id_str_with_string(self):
        ds = Dataset('X')
        assert ds._get_dataset_id_str('MY_ID') == 'MY_ID'


class TestInvalidInputException:
    """Test the custom exception."""

    def test_exception_message(self):
        exc = InvalidInputException("test error")
        assert str(exc) == "test error"

    def test_exception_is_exception(self):
        assert issubclass(InvalidInputException, Exception)


if __name__ == "__main__":
    pytest.main(args=["test_dataset.py"])
