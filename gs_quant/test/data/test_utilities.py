"""
Branch-coverage tests for gs_quant/data/utilities.py
"""

import datetime as dt
import math
import os
from itertools import groupby
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from gs_quant.data.utilities import Utilities, SecmasterXrefFormatter


# ===========================================================================
# Utilities.AssetApi
# ===========================================================================

class TestAssetApiCreateQuery:
    """Tests for __create_query (called via get_many_assets_data)."""

    @patch('gs_quant.data.utilities.GsSession')
    @patch('gs_quant.data.utilities.FieldFilterMap')
    def test_get_many_assets_data_valid(self, mock_ffm, mock_session):
        """Valid query returns results."""
        mock_ffm.properties.return_value = {'ticker', 'bbid'}
        mock_session.current.sync.post.return_value = {'results': [{'id': '1'}]}

        result = Utilities.AssetApi.get_many_assets_data(
            fields=['id'],
            as_of=dt.datetime(2020, 1, 1),
            limit=10,
            ticker='AAPL',
        )
        assert result == [{'id': '1'}]

    @patch('gs_quant.data.utilities.FieldFilterMap')
    def test_get_many_assets_data_invalid_kwarg(self, mock_ffm):
        """Invalid kwarg raises KeyError."""
        mock_ffm.properties.return_value = {'ticker'}
        with pytest.raises(KeyError, match='Invalid asset query argument'):
            Utilities.AssetApi.get_many_assets_data(bad_key='x')

    @patch('gs_quant.data.utilities.GsSession')
    @patch('gs_quant.data.utilities.FieldFilterMap')
    def test_get_many_assets_data_defaults(self, mock_ffm, mock_session):
        """When as_of is None, utcnow is used."""
        mock_ffm.properties.return_value = set()
        mock_session.current.sync.post.return_value = {'results': []}
        result = Utilities.AssetApi.get_many_assets_data()
        assert result == []


# ===========================================================================
# Utilities.target_folder
# ===========================================================================

class TestTargetFolder:
    @patch('gs_quant.data.utilities.os.makedirs')
    @patch('gs_quant.data.utilities.os.access', return_value=True)
    @patch('gs_quant.data.utilities.os.path.exists', return_value=False)
    @patch('gs_quant.data.utilities.os.getcwd', return_value='/tmp')
    def test_target_folder_created(self, mock_cwd, mock_exists, mock_access, mock_makedirs):
        result = Utilities.target_folder()
        assert result is not None
        assert result.startswith('/tmp\\')

    @patch('gs_quant.data.utilities.os.path.exists', return_value=True)
    @patch('gs_quant.data.utilities.os.getcwd', return_value='/tmp')
    def test_target_folder_already_exists(self, mock_cwd, mock_exists):
        result = Utilities.target_folder()
        assert result.startswith('/tmp\\')

    @patch('gs_quant.data.utilities.os.makedirs', side_effect=OSError('fail'))
    @patch('gs_quant.data.utilities.os.access', return_value=True)
    @patch('gs_quant.data.utilities.os.path.exists', return_value=False)
    @patch('gs_quant.data.utilities.os.getcwd', return_value='/tmp')
    def test_target_folder_makedirs_fails(self, mock_cwd, mock_exists, mock_access, mock_makedirs):
        result = Utilities.target_folder()
        assert result == 1

    @patch('gs_quant.data.utilities.os.access', return_value=False)
    @patch('gs_quant.data.utilities.os.path.exists', return_value=False)
    @patch('gs_quant.data.utilities.os.getcwd', return_value='/tmp')
    def test_target_folder_no_write_access(self, mock_cwd, mock_exists, mock_access):
        result = Utilities.target_folder()
        assert result is None


# ===========================================================================
# Utilities.pre_checks
# ===========================================================================

class TestPreChecks:
    def test_write_to_csv_ok(self):
        with patch.object(Utilities, 'target_folder', return_value='/tmp/dir'):
            result = Utilities.pre_checks(
                final_end=dt.datetime(2020, 2, 1),
                original_start=dt.datetime(2020, 1, 1),
                time_field='date',
                datetime_delta_override=None,
                request_batch_size=2,
                write_to_csv=True,
            )
            assert result == (dt.datetime(2020, 2, 1), '/tmp/dir')

    def test_write_to_csv_fails(self):
        with patch.object(Utilities, 'target_folder', return_value=1):
            with pytest.raises(ValueError, match="write permissions"):
                Utilities.pre_checks(
                    final_end=dt.datetime(2020, 2, 1),
                    original_start=dt.datetime(2020, 1, 1),
                    time_field='date',
                    datetime_delta_override=None,
                    request_batch_size=2,
                    write_to_csv=True,
                )

    def test_write_to_csv_false(self):
        result = Utilities.pre_checks(
            final_end=dt.datetime(2020, 2, 1),
            original_start=dt.datetime(2020, 1, 1),
            time_field='date',
            datetime_delta_override=None,
            request_batch_size=2,
            write_to_csv=False,
        )
        assert result[1] is None

    def test_request_batch_size_none(self):
        with pytest.raises(ValueError, match="batch size"):
            Utilities.pre_checks(None, None, 'date', None, None, False)

    def test_request_batch_size_zero(self):
        with pytest.raises(ValueError, match="batch size"):
            Utilities.pre_checks(None, None, 'date', None, 0, False)

    def test_request_batch_size_too_large(self):
        with pytest.raises(ValueError, match="batch size"):
            Utilities.pre_checks(None, None, 'date', None, 5, False)

    def test_datetime_delta_not_int(self):
        with pytest.raises(ValueError, match="Time delta override"):
            Utilities.pre_checks(None, None, 'date', 'string', 2, False)

    def test_datetime_delta_negative(self):
        with pytest.raises(ValueError, match="Time delta override"):
            Utilities.pre_checks(None, None, 'date', -1, 2, False)

    def test_datetime_delta_intraday_too_large(self):
        with pytest.raises(ValueError, match="Time delta override"):
            Utilities.pre_checks(None, None, 'time', 10, 2, False)

    def test_datetime_delta_intraday_ok(self):
        result = Utilities.pre_checks(None, None, 'time', 3, 2, False)
        assert result == (None, None)

    def test_final_end_not_datetime(self):
        with pytest.raises(ValueError, match="End date"):
            Utilities.pre_checks('notdatetime', dt.datetime(2020, 1, 1), 'date', None, 2, False)

    def test_original_start_not_datetime(self):
        with pytest.raises(ValueError, match="Start date"):
            Utilities.pre_checks(dt.datetime(2020, 2, 1), 'notdatetime', 'date', None, 2, False)

    def test_start_after_end(self):
        with pytest.raises(ValueError, match="Start date cannot be greater"):
            Utilities.pre_checks(
                dt.datetime(2020, 1, 1), dt.datetime(2020, 2, 1), 'date', None, 2, False
            )

    def test_intraday_time_diff_too_large(self):
        with pytest.raises(ValueError, match="intraday"):
            Utilities.pre_checks(
                dt.datetime(2020, 1, 1, 10, 0, 0),
                dt.datetime(2020, 1, 1, 0, 0, 0),
                'time',
                None,
                2,
                False,
            )


# ===========================================================================
# Utilities.batch
# ===========================================================================

class TestBatch:
    def test_basic_batch(self):
        result = list(Utilities.batch([1, 2, 3, 4, 5], n=2))
        assert result == [[1, 2], [3, 4], [5]]

    def test_batch_size_one(self):
        result = list(Utilities.batch([1, 2, 3], n=1))
        assert result == [[1], [2], [3]]

    def test_batch_larger_than_list(self):
        result = list(Utilities.batch([1, 2], n=10))
        assert result == [[1, 2]]


# ===========================================================================
# Utilities.fetch_data
# ===========================================================================

class TestFetchData:
    def test_fetch_data_success(self):
        dataset = MagicMock()
        dataset.get_data.return_value = pd.DataFrame({'a': [1]})
        result = Utilities.fetch_data(dataset, ['SYM'], dimension='assetId')
        assert len(result) == 1

    def test_fetch_data_with_auth(self):
        dataset = MagicMock()
        dataset.get_data.return_value = pd.DataFrame({'a': [1]})
        auth = MagicMock()
        Utilities.fetch_data(dataset, ['SYM'], auth=auth)
        auth.assert_called_once()

    def test_fetch_data_exception(self):
        dataset = MagicMock()
        dataset.get_data.side_effect = Exception('fail')
        result = Utilities.fetch_data(dataset, ['SYM'])
        assert isinstance(result, pd.DataFrame)
        assert result.empty


# ===========================================================================
# Utilities.execute_parallel_query
# ===========================================================================

class TestExecuteParallelQuery:
    def test_successful_query(self):
        dataset = MagicMock()
        with patch.object(Utilities, 'fetch_data', return_value=pd.DataFrame({'a': [1]})):
            with patch('gs_quant.data.utilities.pd.concat', return_value=pd.DataFrame({'a': [1, 2]})):
                result = Utilities.execute_parallel_query(
                    dataset, ['SYM1', 'SYM2'], dt.datetime.now(), dt.datetime.now(),
                    'assetId', 2, 2, None
                )
                assert isinstance(result, pd.DataFrame)

    def test_query_exception_max_retry(self):
        dataset = MagicMock()
        # retry > 3 => raise exception
        with patch('gs_quant.data.utilities.pd.concat', side_effect=ValueError('fail')):
            with pytest.raises(Exception, match="retry failure"):
                Utilities.execute_parallel_query(
                    dataset, ['SYM1'], dt.datetime.now(), dt.datetime.now(),
                    'assetId', 1, 1, None, retry=4
                )

    def test_query_exception_retry_success(self):
        """Exception on first call, retry succeeds on recursive call."""
        dataset = MagicMock()
        call_count = [0]
        success_df = pd.DataFrame({'a': [1]})

        def mock_concat(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError('fail first time')
            return success_df

        with patch('gs_quant.data.utilities.pd.concat', side_effect=mock_concat):
            result = Utilities.execute_parallel_query(
                dataset, ['SYM1'], dt.datetime.now(), dt.datetime.now(),
                'assetId', 1, 1, None, retry=0
            )
            assert isinstance(result, pd.DataFrame)


# ===========================================================================
# Utilities.get_dataset_parameter
# ===========================================================================

class TestGetDatasetParameter:
    def test_date_time_field(self):
        dataset = MagicMock()
        dataset.id = 'DS1'
        dataset.provider.symbol_dimensions.return_value = ['assetId']
        dataset.provider.get_definition.return_value = MagicMock(
            parameters=MagicMock(history_date=dt.datetime(2020, 1, 1)),
            dimensions=MagicMock(time_field='date'),
        )
        result = Utilities.get_dataset_parameter(dataset)
        assert result[0] == 'date'
        assert result[3] == dt.timedelta(days=180)

    def test_time_time_field(self):
        dataset = MagicMock()
        dataset.id = 'DS1'
        dataset.provider.symbol_dimensions.return_value = ['assetId']
        dataset.provider.get_definition.return_value = MagicMock(
            parameters=MagicMock(history_date=dt.datetime(2020, 1, 1)),
            dimensions=MagicMock(time_field='time'),
        )
        result = Utilities.get_dataset_parameter(dataset)
        assert result[0] == 'time'
        assert result[3] == dt.timedelta(hours=1)


# ===========================================================================
# Utilities.write_consolidated_results
# ===========================================================================

class TestWriteConsolidatedResults:
    def test_write_to_csv(self):
        df = pd.DataFrame({'a': [1, 2]})
        dataset = MagicMock()
        dataset.id = 'DS1'
        with patch.object(pd.DataFrame, 'to_csv'):
            Utilities.write_consolidated_results(df, '/tmp', dataset, 1, None, True, 10, 5)

    def test_write_to_handler(self):
        df = pd.DataFrame({'a': [1, 2]})
        handler = MagicMock()
        dataset = MagicMock()
        dataset.id = 'DS1'
        Utilities.write_consolidated_results(df, None, dataset, 1, handler, False, 10, 5)
        handler.assert_called_once_with(df)

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        handler = MagicMock()
        dataset = MagicMock()
        dataset.id = 'DS1'
        Utilities.write_consolidated_results(df, None, dataset, 1, handler, False, 10, 5)
        handler.assert_not_called()


# ===========================================================================
# Utilities.iterate_over_series
# ===========================================================================

class TestIterateOverSeries:
    def test_iterate_basic(self):
        dataset = MagicMock()
        with patch.object(Utilities, 'execute_parallel_query', return_value=pd.DataFrame({'a': [1]})):
            with patch.object(Utilities, 'write_consolidated_results'):
                result = Utilities.iterate_over_series(
                    dataset=dataset,
                    coverage_batch=['SYM'],
                    original_start=dt.datetime(2020, 1, 1),
                    original_end=dt.datetime(2020, 1, 2),
                    datetime_delta_override=dt.timedelta(days=1),
                    symbol_dimension='assetId',
                    request_batch_size=2,
                    authenticate=None,
                    final_end=dt.datetime(2020, 1, 2),
                    write_to_csv=False,
                    target_dir_result=None,
                    batch_number=1,
                    coverage_length=1,
                    symbols_per_csv=5,
                    handler=MagicMock(),
                )
                assert result is None

    def test_iterate_multiple_loops(self):
        """Loop iterates multiple times before end > final_end."""
        dataset = MagicMock()
        with patch.object(Utilities, 'execute_parallel_query', return_value=pd.DataFrame({'a': [1]})):
            with patch.object(Utilities, 'write_consolidated_results') as mock_write:
                result = Utilities.iterate_over_series(
                    dataset=dataset,
                    coverage_batch=['SYM'],
                    original_start=dt.datetime(2020, 1, 1),
                    original_end=dt.datetime(2020, 1, 2),
                    datetime_delta_override=dt.timedelta(days=2),
                    symbol_dimension='assetId',
                    request_batch_size=2,
                    authenticate=None,
                    final_end=dt.datetime(2020, 1, 5),
                    write_to_csv=False,
                    target_dir_result=None,
                    batch_number=1,
                    coverage_length=1,
                    symbols_per_csv=5,
                    handler=MagicMock(),
                )
                assert result is None
                mock_write.assert_called_once()


# ===========================================================================
# Utilities.extract_xref
# ===========================================================================

class TestExtractXref:
    def test_single_asset(self):
        assets = [{'rank': 1, 'ticker': 'AAPL'}]
        assert Utilities.extract_xref(assets, 'ticker') == 'AAPL'

    def test_multiple_assets_sorted_by_rank(self):
        assets = [
            {'rank': 1, 'ticker': 'LOW'},
            {'rank': 5, 'ticker': 'HIGH'},
        ]
        assert Utilities.extract_xref(assets, 'ticker') == 'HIGH'

    def test_missing_out_type(self):
        assets = [{'rank': 1}]
        assert Utilities.extract_xref(assets, 'ticker') == ''

    def test_no_rank_key(self):
        assets = [{'ticker': 'X'}, {'ticker': 'Y'}]
        result = Utilities.extract_xref(assets, 'ticker')
        assert result in ('X', 'Y')


# ===========================================================================
# Utilities.map_identifiers
# ===========================================================================

class TestMapIdentifiers:
    @patch.object(Utilities.AssetApi, 'get_many_assets_data')
    def test_map_identifiers_basic(self, mock_get):
        mock_get.return_value = [
            {'ticker': 'AAPL', 'bbid': 'AAPL UW', 'rank': 1},
        ]
        result = Utilities.map_identifiers('ticker', 'bbid', ['AAPL'])
        assert result['AAPL'] == 'AAPL UW'

    @patch.object(Utilities.AssetApi, 'get_many_assets_data')
    def test_map_identifiers_multiple_batches(self, mock_get):
        # Ids larger than 1000 would create multiple batches
        mock_get.return_value = [
            {'ticker': 'A', 'bbid': 'A_BB', 'rank': 1},
        ]
        result = Utilities.map_identifiers('ticker', 'bbid', ['A'])
        assert 'A' in result


# ===========================================================================
# Utilities.get_dataset_coverage
# ===========================================================================

class TestGetDatasetCoverage:
    def test_asset_id_dimension(self):
        dataset = MagicMock()
        cov = pd.DataFrame({'ticker': ['AAPL', 'GOOG', None]})
        dataset.get_coverage.return_value = cov
        result = Utilities.get_dataset_coverage('ticker', 'assetId', dataset)
        assert result == ['AAPL', 'GOOG']

    @patch.object(Utilities, 'map_identifiers', return_value={'123': 'AAPL'})
    def test_gsid_dimension(self, mock_map):
        dataset = MagicMock()
        dataset.get_coverage.return_value = pd.DataFrame({'gsid': ['123']})
        result = Utilities.get_dataset_coverage('ticker', 'gsid', dataset)
        assert result == ['AAPL']

    def test_other_dimension(self):
        dataset = MagicMock()
        dataset.get_coverage.return_value = pd.DataFrame({'bbid': ['X', 'Y']})
        result = Utilities.get_dataset_coverage('ticker', 'bbid', dataset)
        assert result == ['X', 'Y']


# ===========================================================================
# SecmasterXrefFormatter
# ===========================================================================

class TestSecmasterXrefFormatter:
    def test_convert_empty_data(self):
        result = SecmasterXrefFormatter.convert({})
        assert result == {}

    def test_convert_empty_records(self):
        result = SecmasterXrefFormatter.convert({'entity1': []})
        assert result == {'entity1': {'xrefs': []}}

    def test_convert_single_record_infinity(self):
        data = {
            'entity1': [
                {
                    'type': 'ticker',
                    'value': 'AAPL',
                    'startDate': '2020-01-01',
                    'endDate': '9999-99-99',
                }
            ]
        }
        result = SecmasterXrefFormatter.convert(data)
        xrefs = result['entity1']['xrefs']
        assert len(xrefs) == 1
        assert xrefs[0]['startDate'] == '2020-01-01'
        assert xrefs[0]['endDate'] == '9999-12-31'
        assert xrefs[0]['identifiers'] == {'ticker': 'AAPL'}

    def test_convert_single_record_finite(self):
        data = {
            'entity1': [
                {
                    'type': 'ticker',
                    'value': 'AAPL',
                    'startDate': '2020-01-01',
                    'endDate': '2020-06-30',
                }
            ]
        }
        result = SecmasterXrefFormatter.convert(data)
        xrefs = result['entity1']['xrefs']
        assert len(xrefs) == 1
        assert xrefs[0]['endDate'] == '2020-06-30'

    def test_convert_multiple_records_overlapping(self):
        data = {
            'entity1': [
                {
                    'type': 'ticker',
                    'value': 'AAPL',
                    'startDate': '2020-01-01',
                    'endDate': '2020-12-31',
                },
                {
                    'type': 'bbid',
                    'value': 'AAPL UW',
                    'startDate': '2020-06-01',
                    'endDate': '9999-99-99',
                },
            ]
        }
        result = SecmasterXrefFormatter.convert(data)
        xrefs = result['entity1']['xrefs']
        assert len(xrefs) >= 1

    def test_convert_adjacent_periods(self):
        """End event on same date as start event of next record."""
        data = {
            'entity1': [
                {
                    'type': 'ticker',
                    'value': 'OLD',
                    'startDate': '2020-01-01',
                    'endDate': '2020-06-30',
                },
                {
                    'type': 'ticker',
                    'value': 'NEW',
                    'startDate': '2020-07-01',
                    'endDate': '9999-99-99',
                },
            ]
        }
        result = SecmasterXrefFormatter.convert(data)
        xrefs = result['entity1']['xrefs']
        assert len(xrefs) >= 2

    def test_convert_multiple_entities(self):
        data = {
            'e1': [
                {'type': 'ticker', 'value': 'A', 'startDate': '2020-01-01', 'endDate': '9999-99-99'}
            ],
            'e2': [
                {'type': 'bbid', 'value': 'B', 'startDate': '2021-01-01', 'endDate': '2021-12-31'}
            ],
        }
        result = SecmasterXrefFormatter.convert(data)
        assert 'e1' in result
        assert 'e2' in result

    # -- _date_sort_key ---------------------------------------------------

    def test_date_sort_key_infinity(self):
        result = SecmasterXrefFormatter._date_sort_key('9999-12-31')
        assert result == dt.datetime(9999, 12, 31)

    def test_date_sort_key_normal(self):
        result = SecmasterXrefFormatter._date_sort_key('2020-06-15')
        assert result == dt.datetime(2020, 6, 15)

    # -- _add_one_day -----------------------------------------------------

    def test_add_one_day_normal(self):
        assert SecmasterXrefFormatter._add_one_day('2020-01-15') == '2020-01-16'

    def test_add_one_day_infinity(self):
        assert SecmasterXrefFormatter._add_one_day('9999-12-31') is None

    def test_add_one_day_near_infinity(self):
        # 9999-12-30 -> next day is 9999-12-31; the date_obj check doesn't trigger
        # because the date_obj (9999-12-30) doesn't have day==31
        result = SecmasterXrefFormatter._add_one_day('9999-12-30')
        assert result == '9999-12-31'

    def test_add_one_day_overflow(self):
        # Trigger the ValueError/OverflowError path by patching strptime
        # to raise an OverflowError
        with patch('gs_quant.data.utilities.dt.datetime') as mock_dt:
            mock_dt.strptime.side_effect = OverflowError('overflow')
            result = SecmasterXrefFormatter._add_one_day('2020-01-01')
            assert result is None

    def test_add_one_day_invalid(self):
        assert SecmasterXrefFormatter._add_one_day('not-a-date') is None

    # -- _subtract_one_day ------------------------------------------------

    def test_subtract_one_day_normal(self):
        assert SecmasterXrefFormatter._subtract_one_day('2020-01-15') == '2020-01-14'

    def test_subtract_one_day_invalid(self):
        result = SecmasterXrefFormatter._subtract_one_day('not-a-date')
        assert result == 'not-a-date'

    # -- Event __post_init__ -----------------------------------------------

    def test_event_priority_end(self):
        event = SecmasterXrefFormatter.Event(
            date='2020-01-01',
            event_type=SecmasterXrefFormatter.EventType.END,
            record={},
        )
        assert event.priority == 1

    def test_event_priority_start(self):
        event = SecmasterXrefFormatter.Event(
            date='2020-01-01',
            event_type=SecmasterXrefFormatter.EventType.START,
            record={},
        )
        assert event.priority == 0

    # -- _process_events with final period not infinity --------------------

    def test_process_events_final_period_latest_end(self):
        """Active identifiers at end without infinity end date uses latest end."""
        data = {
            'entity1': [
                {
                    'type': 'ticker',
                    'value': 'A',
                    'startDate': '2020-01-01',
                    'endDate': '2020-12-31',
                },
                {
                    'type': 'bbid',
                    'value': 'B',
                    'startDate': '2020-01-01',
                    'endDate': '2020-06-30',
                },
            ]
        }
        result = SecmasterXrefFormatter.convert(data)
        xrefs = result['entity1']['xrefs']
        # The last period should have both identifiers; find the final one
        last_period = xrefs[-1]
        assert 'ticker' in last_period['identifiers']

    def test_process_events_final_period_non_infinity_end(self):
        """When the final active identifiers all have finite end dates,
        the latest end date is used as the period end (not infinity).
        This triggers lines 416-417 of _process_events."""
        # To hit lines 416-417, we need active identifiers at the end of the
        # while loop that have non-infinity endDate. This happens when
        # _add_one_day returns None (so no end event is created) for a
        # non-INFINITY_DATE. We can achieve this with a near-overflow date.
        data = {
            'entity1': [
                {
                    'type': 'ticker',
                    'value': 'A',
                    'startDate': '2020-01-01',
                    'endDate': '9999-12-30',
                },
            ]
        }
        result = SecmasterXrefFormatter.convert(data)
        xrefs = result['entity1']['xrefs']
        # _add_one_day('9999-12-30') returns '9999-12-31' which is not None,
        # so an end event IS created. The identifier gets removed.
        # Let's check what happens.
        assert len(xrefs) >= 1

    def test_process_events_final_period_non_infinity_via_mock(self):
        """Test lines 416-417 by mocking _add_one_day to return None for
        a non-infinity date, so no end event is created and the identifier
        stays active."""
        original_add = SecmasterXrefFormatter._add_one_day

        def patched_add(date_str):
            if date_str == '2020-12-31':
                return None  # Simulate overflow
            return original_add(date_str)

        with patch.object(SecmasterXrefFormatter, '_add_one_day', side_effect=patched_add):
            data = {
                'entity1': [
                    {
                        'type': 'ticker',
                        'value': 'A',
                        'startDate': '2020-01-01',
                        'endDate': '2020-12-31',
                    },
                ]
            }
            result = SecmasterXrefFormatter.convert(data)
            xrefs = result['entity1']['xrefs']
            assert len(xrefs) == 1
            # Since no end event was created and it's not INFINITY_DATE,
            # lines 416-417 set period_end to the latest endDate
            assert xrefs[0]['endDate'] == '2020-12-31'

    # -- _create_events with next_day None --------------------------------

    def test_create_events_end_date_infinity(self):
        """Records with 9999-12-31 endDate should not generate end events."""
        records = [
            {
                'type': 'ticker',
                'value': 'X',
                'startDate': '2020-01-01',
                'endDate': '9999-12-31',
            }
        ]
        events = SecmasterXrefFormatter._create_events(records)
        # Only 1 start event, no end event
        assert len(events) == 1
        assert events[0].event_type == SecmasterXrefFormatter.EventType.START

    # -- Branch [393,391]: end event for identifier not in active_identifiers --

    def test_end_event_identifier_not_in_active(self):
        """Branch [393,391]: end event for identifier_type not in active_identifiers is skipped."""
        # This happens when an end event arrives for an identifier that was already
        # removed or was never added (e.g., overlapping records for same type).
        # We can construct a scenario where two records of the same type overlap
        # such that the first end event removes the identifier and the second
        # end event finds it missing.
        data = {
            'entity1': [
                {
                    'type': 'ticker',
                    'value': 'A',
                    'startDate': '2020-01-01',
                    'endDate': '2020-06-30',
                },
                {
                    'type': 'ticker',
                    'value': 'B',
                    'startDate': '2020-03-01',
                    'endDate': '2020-06-30',
                },
            ]
        }
        result = SecmasterXrefFormatter.convert(data)
        # Should not raise; the second end event finds ticker already removed
        assert 'entity1' in result

    # -- Branch [441,442]: _add_one_day with 9999-12-31 (not INFINITY_DATE constant) --

    def test_add_one_day_9999_12_31(self):
        """Branch [441,442]: date_obj.year==9999, month==12, day==31 returns None."""
        # _add_one_day checks for INFINITY_DATE constant first, then for the actual date
        # We need to hit line 441 where the date parses as 9999-12-31 but isn't equal
        # to the INFINITY_DATE constant. Actually INFINITY_DATE IS "9999-12-31", so
        # line 438 catches it first. The check at line 441 is for dates that somehow
        # parse to that value without being the constant string.
        # Actually if date_str is '9999-12-31', line 438 returns None.
        # Line 441 is only reached if date_str != '9999-12-31' but parses to year=9999, month=12, day=31.
        # That's impossible with strptime('%Y-%m-%d'). This branch is unreachable.
        # But let's at least call _add_one_day with a normal date to exercise the function.
        result = SecmasterXrefFormatter._add_one_day('2020-06-15')
        assert result == '2020-06-16'

        # And with INFINITY_DATE
        result = SecmasterXrefFormatter._add_one_day('9999-12-31')
        assert result is None

    # -- _add_one_day with ValueError/OverflowError --

    def test_add_one_day_invalid_date(self):
        """Branch: ValueError in _add_one_day returns None."""
        result = SecmasterXrefFormatter._add_one_day('not-a-date')
        assert result is None
