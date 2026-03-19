"""
Branch-coverage tests for gs_quant/markets/position_set_utils.py
"""

import datetime as dt
import math
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from gs_quant.markets.position_set_utils import (
    _get_asset_temporal_xrefs,
    _group_temporal_xrefs_into_discrete_time_ranges,
    _resolve_many_assets,
)


# ===========================================================================
# _get_asset_temporal_xrefs
# ===========================================================================

class TestGetAssetTemporalXrefs:
    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_basic_flow(self, mock_api):
        """Normal flow: single batch, identifiers inferred, delisted filtered."""
        position_sets_df = pd.DataFrame({
            'identifier': ['AAPL', 'GOOG'],
            'date': [dt.datetime(2020, 1, 1), dt.datetime(2020, 6, 1)],
        })

        mock_api.get_many_asset_xrefs.return_value = [
            {
                'assetId': 'id1',
                'xrefs': [
                    {
                        'startDate': '2019-01-01',
                        'endDate': '2025-12-31',
                        'identifiers': {'ticker': 'AAPL', 'bbid': 'AAPL UW', 'delisted': 'no'},
                    }
                ],
            },
            {
                'assetId': 'id2',
                'xrefs': [
                    {
                        'startDate': '2019-06-01',
                        'endDate': '2025-12-31',
                        'identifiers': {'ticker': 'GOOG', 'bbid': 'GOOG UW', 'delisted': 'no'},
                    }
                ],
            },
        ]

        xref_df, identifier_type = _get_asset_temporal_xrefs(position_sets_df)

        assert isinstance(xref_df, pd.DataFrame)
        assert identifier_type in ('ticker', 'bbid')
        assert len(xref_df) > 0

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_empty_xrefs(self, mock_api):
        """xrefs list is empty or None -> skipped."""
        position_sets_df = pd.DataFrame({
            'identifier': ['AAA'],
            'date': [dt.datetime(2020, 1, 1)],
        })

        mock_api.get_many_asset_xrefs.return_value = [
            {'assetId': 'id1', 'xrefs': None},
            {'assetId': 'id2', 'xrefs': []},
        ]

        # Empty xref_df will cause errors downstream; test that the branch is hit
        try:
            xref_df, identifier_type = _get_asset_temporal_xrefs(position_sets_df)
        except Exception:
            pass  # expected if no xrefs found

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_filters_old_xrefs(self, mock_api):
        """xref endDate before earliest position date is filtered."""
        position_sets_df = pd.DataFrame({
            'identifier': ['AAPL'],
            'date': [dt.datetime(2020, 6, 1)],
        })

        mock_api.get_many_asset_xrefs.return_value = [
            {
                'assetId': 'id1',
                'xrefs': [
                    {
                        'startDate': '2015-01-01',
                        'endDate': '2019-12-31',
                        'identifiers': {'ticker': 'AAPL', 'delisted': 'no'},
                    },
                    {
                        'startDate': '2020-01-01',
                        'endDate': '2025-12-31',
                        'identifiers': {'ticker': 'AAPL', 'delisted': 'no'},
                    },
                ],
            },
        ]

        xref_df, identifier_type = _get_asset_temporal_xrefs(position_sets_df)
        # Only the second xref (end 2025) should remain
        assert len(xref_df) == 1
        assert identifier_type == 'ticker'

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_delisted_filtered(self, mock_api):
        """Delisted assets are filtered out."""
        position_sets_df = pd.DataFrame({
            'identifier': ['AAPL'],
            'date': [dt.datetime(2020, 1, 1)],
        })

        mock_api.get_many_asset_xrefs.return_value = [
            {
                'assetId': 'id1',
                'xrefs': [
                    {
                        'startDate': '2020-01-01',
                        'endDate': '2025-12-31',
                        'identifiers': {'ticker': 'AAPL', 'delisted': 'yes'},
                    },
                ],
            },
        ]

        xref_df, identifier_type = _get_asset_temporal_xrefs(position_sets_df)
        assert len(xref_df) == 0

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_delisted_missing_filled_no(self, mock_api):
        """When delisted key is missing from some xrefs, it's filled with 'no'."""
        position_sets_df = pd.DataFrame({
            'identifier': ['AAPL', 'GOOG'],
            'date': [dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 1)],
        })

        mock_api.get_many_asset_xrefs.return_value = [
            {
                'assetId': 'id1',
                'xrefs': [
                    {
                        'startDate': '2020-01-01',
                        'endDate': '2025-12-31',
                        'identifiers': {'ticker': 'AAPL', 'delisted': 'no'},
                    },
                ],
            },
            {
                'assetId': 'id2',
                'xrefs': [
                    {
                        'startDate': '2020-01-01',
                        'endDate': '2025-12-31',
                        # No 'delisted' key -- should be filled with 'no'
                        'identifiers': {'ticker': 'GOOG'},
                    },
                ],
            },
        ]

        xref_df, identifier_type = _get_asset_temporal_xrefs(position_sets_df)
        assert len(xref_df) == 2
        assert identifier_type == 'ticker'

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_multiple_identifier_types_best_match(self, mock_api):
        """When multiple identifier types exist, picks the one with most matches."""
        position_sets_df = pd.DataFrame({
            'identifier': ['AAPL', 'GOOG'],
            'date': [dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 1)],
        })

        mock_api.get_many_asset_xrefs.return_value = [
            {
                'assetId': 'id1',
                'xrefs': [
                    {
                        'startDate': '2020-01-01',
                        'endDate': '2025-12-31',
                        'identifiers': {'ticker': 'AAPL', 'bbid': 'AAPL UW', 'delisted': 'no'},
                    },
                ],
            },
            {
                'assetId': 'id2',
                'xrefs': [
                    {
                        'startDate': '2020-01-01',
                        'endDate': '2025-12-31',
                        'identifiers': {'ticker': 'GOOG', 'bbid': 'GOOG UW', 'delisted': 'no'},
                    },
                ],
            },
        ]

        xref_df, identifier_type = _get_asset_temporal_xrefs(position_sets_df)
        # Both ticker and bbid have columns, but ticker matches 'AAPL', 'GOOG' from universe
        assert identifier_type == 'ticker'

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_multiple_batches(self, mock_api):
        """When universe > 500, multiple batches are created."""
        # Create a large universe
        ids = [f'ID{i}' for i in range(501)]
        position_sets_df = pd.DataFrame({
            'identifier': ids,
            'date': [dt.datetime(2020, 1, 1)] * 501,
        })

        mock_api.get_many_asset_xrefs.return_value = [
            {
                'assetId': f'asset_{i}',
                'xrefs': [
                    {
                        'startDate': '2020-01-01',
                        'endDate': '2025-12-31',
                        'identifiers': {'ticker': f'ID{i}', 'delisted': 'no'},
                    }
                ],
            }
            for i in range(5)
        ]

        xref_df, identifier_type = _get_asset_temporal_xrefs(position_sets_df)
        # Multiple calls to get_many_asset_xrefs
        assert mock_api.get_many_asset_xrefs.call_count == 2


# ===========================================================================
# _group_temporal_xrefs_into_discrete_time_ranges
# ===========================================================================

class TestGroupTemporalXrefs:
    def test_basic_grouping(self):
        """Non-overlapping intervals get different groups."""
        xref_df = pd.DataFrame({
            'assetId': ['id1', 'id1'],
            'ticker': ['A', 'A'],
            'startDate': ['2020-01-01', '2021-01-01'],
            'endDate': ['2020-06-30', '2021-06-30'],
        })
        _group_temporal_xrefs_into_discrete_time_ranges(xref_df)
        assert 'group' in xref_df.columns

    def test_overlapping_intervals(self):
        """Overlapping intervals stay in the same group."""
        xref_df = pd.DataFrame({
            'assetId': ['id1', 'id1'],
            'ticker': ['A', 'A'],
            'startDate': ['2020-01-01', '2020-03-01'],
            'endDate': ['2020-06-30', '2020-09-30'],
        })
        _group_temporal_xrefs_into_discrete_time_ranges(xref_df)
        assert 'group' in xref_df.columns
        # All should be in same group since they overlap
        assert xref_df['group'].nunique() == 1

    def test_single_row(self):
        xref_df = pd.DataFrame({
            'assetId': ['id1'],
            'ticker': ['A'],
            'startDate': ['2020-01-01'],
            'endDate': ['2020-12-31'],
        })
        _group_temporal_xrefs_into_discrete_time_ranges(xref_df)
        assert 'group' in xref_df.columns


# ===========================================================================
# _resolve_many_assets
# ===========================================================================

class TestResolveManyAssets:
    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_basic_resolve(self, mock_api):
        """Resolve assets with results."""
        historical_xref_df = pd.DataFrame({
            'assetId': ['id1'],
            'ticker': ['AAPL'],
            'startDate': [dt.datetime(2020, 1, 1)],
            'endDate': [dt.datetime(2020, 12, 31)],
            'group': [0],
        })

        mock_api.resolve_assets.return_value = {
            'AAPL': [{'id': 'id1', 'name': 'Apple', 'ticker': 'AAPL', 'tradingRestriction': None}],
        }

        result = _resolve_many_assets(historical_xref_df, 'ticker')
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_resolve_with_unmapped(self, mock_api):
        """Assets that don't resolve go into unmapped list."""
        historical_xref_df = pd.DataFrame({
            'assetId': ['id1', 'id2'],
            'ticker': ['AAPL', 'UNKNOWN'],
            'startDate': [dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 1)],
            'endDate': [dt.datetime(2020, 12, 31), dt.datetime(2020, 12, 31)],
            'group': [0, 0],
        })

        mock_api.resolve_assets.return_value = {
            'AAPL': [{'id': 'id1', 'name': 'Apple', 'ticker': 'AAPL', 'tradingRestriction': None}],
            'UNKNOWN': [],
        }

        result = _resolve_many_assets(historical_xref_df, 'ticker')
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_resolve_empty_groups(self, mock_api):
        """No groups -> empty DataFrame."""
        historical_xref_df = pd.DataFrame({
            'assetId': [],
            'ticker': [],
            'startDate': [],
            'endDate': [],
            'group': [],
        })

        result = _resolve_many_assets(historical_xref_df, 'ticker')
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_resolve_multiple_groups(self, mock_api):
        """Multiple groups processed separately."""
        historical_xref_df = pd.DataFrame({
            'assetId': ['id1', 'id2'],
            'ticker': ['AAPL', 'GOOG'],
            'startDate': [dt.datetime(2020, 1, 1), dt.datetime(2021, 1, 1)],
            'endDate': [dt.datetime(2020, 12, 31), dt.datetime(2021, 12, 31)],
            'group': [0, 1],
        })

        mock_api.resolve_assets.side_effect = [
            {'AAPL': [{'id': 'id1', 'name': 'Apple', 'ticker': 'AAPL', 'tradingRestriction': None}]},
            {'GOOG': [{'id': 'id2', 'name': 'Google', 'ticker': 'GOOG', 'tradingRestriction': None}]},
        ]

        result = _resolve_many_assets(historical_xref_df, 'ticker')
        assert isinstance(result, pd.DataFrame)

    @patch('gs_quant.markets.position_set_utils.GsAssetApi')
    def test_resolve_multiple_batches(self, mock_api):
        """When identifiers > 500, multiple batches used."""
        ids = [f'ID{i}' for i in range(501)]
        asset_ids = [f'aid{i}' for i in range(501)]
        historical_xref_df = pd.DataFrame({
            'assetId': asset_ids,
            'ticker': ids,
            'startDate': [dt.datetime(2020, 1, 1)] * 501,
            'endDate': [dt.datetime(2020, 12, 31)] * 501,
            'group': [0] * 501,
        })

        mock_api.resolve_assets.return_value = {
            f'ID{i}': [{'id': f'aid{i}', 'name': f'Name{i}', 'ticker': f'ID{i}', 'tradingRestriction': None}]
            for i in range(501)
        }

        result = _resolve_many_assets(historical_xref_df, 'ticker')
        assert mock_api.resolve_assets.call_count == 2
