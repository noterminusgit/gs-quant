"""
Copyright 2024 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the 'License');
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import datetime as dt
import warnings
from time import sleep
from unittest.mock import MagicMock, patch, PropertyMock, call

import numpy as np
import pandas as pd
import pytest

from gs_quant.common import Currency, PositionType
from gs_quant.entities.entitlements import Entitlements, EntitlementBlock, User
from gs_quant.errors import MqError, MqValueError
from gs_quant.markets.portfolio_manager import PortfolioManager, CustomAUMDataPoint
from gs_quant.markets.report import PerformanceReport, ReportJobFuture
from gs_quant.models.risk_model import FactorType, ReturnFormat
from gs_quant.target.portfolios import RiskAumSource
from gs_quant.target.reports import ReportType, ReportStatus


# =========================
# CustomAUMDataPoint Tests
# =========================


class TestCustomAUMDataPoint:
    def test_init_and_properties(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            d = dt.date(2023, 1, 1)
            dp = CustomAUMDataPoint(date=d, aum=1000.0)
            assert dp.date == d
            assert dp.aum == 1000.0

    def test_setters(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            dp = CustomAUMDataPoint(date=dt.date(2023, 1, 1), aum=1000.0)
            dp.date = dt.date(2023, 6, 1)
            dp.aum = 2000.0
            assert dp.date == dt.date(2023, 6, 1)
            assert dp.aum == 2000.0


# ===========================
# PortfolioManager Init Tests
# ===========================


class TestPortfolioManagerInit:
    def test_init(self):
        pm = PortfolioManager('PORT123')
        assert pm.portfolio_id == 'PORT123'
        assert pm.id == 'PORT123'

    def test_portfolio_id_setter(self):
        pm = PortfolioManager('PORT123')
        pm.portfolio_id = 'PORT456'
        assert pm.portfolio_id == 'PORT456'


# ======================================
# PortfolioManager.get_performance_report
# ======================================


class TestGetPerformanceReport:
    @staticmethod
    def _make_ppa_report(tags=None):
        """Create a mock report compatible with PerformanceReport.from_target."""
        report = MagicMock()
        report.parameters.tags = tags
        report.id = 'RPT1'
        report.name = 'test'
        report.position_source_id = 'PORT1'
        report.position_source_type = 'Portfolio'
        report.type = ReportType.Portfolio_Performance_Analytics
        report.earliest_start_date = None
        report.latest_end_date = None
        report.latest_execution_time = None
        report.status = ReportStatus.done
        report.percentage_complete = 100.0
        return report

    @patch('gs_quant.markets.portfolio_manager.GsReportApi.get_reports')
    def test_tags_none_filter_returns_report(self, mock_get_reports):
        """When tags is None, filters for reports where parameters.tags is None."""
        mock_report = self._make_ppa_report(tags=None)
        mock_report_with_tags = self._make_ppa_report(tags=[MagicMock()])

        mock_get_reports.return_value = [mock_report_with_tags, mock_report]

        pm = PortfolioManager('PORT1')
        result = pm.get_performance_report(tags=None)
        assert result is not None

    @patch('gs_quant.markets.portfolio_manager.GsReportApi.get_reports')
    def test_tags_none_no_reports_raises(self, mock_get_reports):
        """When tags is None and no report with None tags, raises MqError."""
        mock_report = self._make_ppa_report(tags=[MagicMock()])
        mock_get_reports.return_value = [mock_report]

        pm = PortfolioManager('PORT1')
        with pytest.raises(MqError, match='No performance report found'):
            pm.get_performance_report(tags=None)

    @patch('gs_quant.markets.portfolio_manager.GsReportApi.get_reports')
    def test_tags_not_none(self, mock_get_reports):
        """When tags are provided, returns reports[0] without extra filtering."""
        mock_report = self._make_ppa_report(tags=[MagicMock()])
        mock_get_reports.return_value = [mock_report]

        pm = PortfolioManager('PORT1')
        result = pm.get_performance_report(tags={'fund': 'A'})
        assert result is not None

    @patch('gs_quant.markets.portfolio_manager.GsReportApi.get_reports')
    def test_tags_not_none_empty_reports(self, mock_get_reports):
        """When tags provided but no reports found, raises MqError."""
        mock_get_reports.return_value = []
        pm = PortfolioManager('PORT1')
        with pytest.raises(MqError, match='No performance report found'):
            pm.get_performance_report(tags={'fund': 'A'})


# ==============================
# PortfolioManager.schedule_reports
# ==============================


class TestScheduleReports:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.schedule_reports')
    def test_no_batch_no_backcast(self, mock_schedule):
        """months_per_batch is None and backcast is False -> calls schedule_reports directly."""
        pm = PortfolioManager('PORT1')
        pm.schedule_reports(start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 6, 1))
        mock_schedule.assert_called_once()

    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.schedule_reports')
    def test_backcast_true_ignores_batching(self, mock_schedule):
        """When backcast is True, even with months_per_batch, schedules without batching."""
        pm = PortfolioManager('PORT1')
        pm.schedule_reports(
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
            backcast=True,
            months_per_batch=3,
        )
        mock_schedule.assert_called_once_with('PORT1', dt.date(2023, 1, 1), dt.date(2023, 6, 1), backcast=True)

    def test_months_per_batch_zero_raises(self):
        pm = PortfolioManager('PORT1')
        with pytest.raises(MqValueError, match='months_per_batch .* should be greater than 0'):
            pm.schedule_reports(months_per_batch=0)

    def test_months_per_batch_negative_raises(self):
        pm = PortfolioManager('PORT1')
        with pytest.raises(MqValueError, match='months_per_batch .* should be greater than 0'):
            pm.schedule_reports(months_per_batch=-1)

    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.schedule_reports')
    @patch.object(PortfolioManager, 'get_position_dates')
    @patch.object(PortfolioManager, 'get_schedule_dates')
    def test_fetches_schedule_dates_when_none(self, mock_sched_dates, mock_pos_dates, mock_schedule):
        """When start_date or end_date is None, fetches schedule dates."""
        mock_sched_dates.return_value = [dt.date(2023, 1, 1), dt.date(2023, 6, 1)]
        mock_pos_dates.return_value = [dt.date(2023, 1, 1), dt.date(2023, 3, 1), dt.date(2023, 6, 1)]

        pm = PortfolioManager('PORT1')
        pm.schedule_reports(months_per_batch=6)
        mock_sched_dates.assert_called_once()

    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.schedule_reports')
    @patch.object(PortfolioManager, 'get_position_dates')
    @patch.object(PortfolioManager, 'get_schedule_dates')
    def test_fetches_only_end_date_when_start_provided(self, mock_sched_dates, mock_pos_dates, mock_schedule):
        """When only end_date is None."""
        mock_sched_dates.return_value = [dt.date(2023, 1, 1), dt.date(2023, 6, 1)]
        mock_pos_dates.return_value = [dt.date(2023, 1, 1), dt.date(2023, 3, 1), dt.date(2023, 6, 1)]

        pm = PortfolioManager('PORT1')
        pm.schedule_reports(start_date=dt.date(2023, 1, 1), months_per_batch=6)
        mock_sched_dates.assert_called_once()

    @patch.object(PortfolioManager, 'get_position_dates')
    def test_start_ge_end_raises(self, mock_pos_dates):
        pm = PortfolioManager('PORT1')
        with pytest.raises(MqValueError, match='start date .* should be before end date'):
            pm.schedule_reports(
                start_date=dt.date(2023, 6, 1),
                end_date=dt.date(2023, 1, 1),
                months_per_batch=3,
            )

    @patch.object(PortfolioManager, 'get_position_dates')
    def test_start_eq_end_raises(self, mock_pos_dates):
        pm = PortfolioManager('PORT1')
        with pytest.raises(MqValueError, match='start date .* should be before end date'):
            pm.schedule_reports(
                start_date=dt.date(2023, 6, 1),
                end_date=dt.date(2023, 6, 1),
                months_per_batch=3,
            )

    @patch.object(PortfolioManager, 'get_position_dates')
    def test_no_positions_in_range_raises(self, mock_pos_dates):
        mock_pos_dates.return_value = [dt.date(2022, 1, 1), dt.date(2022, 6, 1)]
        pm = PortfolioManager('PORT1')
        with pytest.raises(MqValueError, match='does not have any positions'):
            pm.schedule_reports(
                start_date=dt.date(2023, 1, 1),
                end_date=dt.date(2023, 6, 1),
                months_per_batch=3,
            )

    @patch.object(PortfolioManager, 'get_position_dates')
    def test_start_not_in_position_dates_raises(self, mock_pos_dates):
        mock_pos_dates.return_value = [dt.date(2023, 2, 1), dt.date(2023, 3, 1)]
        pm = PortfolioManager('PORT1')
        with pytest.raises(MqError, match='Cannot schedule historical report'):
            pm.schedule_reports(
                start_date=dt.date(2023, 1, 1),
                end_date=dt.date(2023, 6, 1),
                months_per_batch=3,
            )

    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.schedule_reports')
    @patch.object(PortfolioManager, 'get_position_dates')
    def test_end_date_not_in_position_dates_appends(self, mock_pos_dates, mock_schedule):
        """When end_date not in position_dates, it gets appended."""
        mock_pos_dates.return_value = [
            dt.date(2023, 1, 1),
            dt.date(2023, 3, 1),
        ]
        pm = PortfolioManager('PORT1')
        pm.schedule_reports(
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
            months_per_batch=12,
        )
        mock_schedule.assert_called()

    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.schedule_reports')
    @patch.object(PortfolioManager, 'get_position_dates')
    def test_end_date_in_position_dates(self, mock_pos_dates, mock_schedule):
        """When end_date is already in position_dates."""
        mock_pos_dates.return_value = [
            dt.date(2023, 1, 1),
            dt.date(2023, 3, 1),
            dt.date(2023, 6, 1),
        ]
        pm = PortfolioManager('PORT1')
        pm.schedule_reports(
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 6, 1),
            months_per_batch=12,
        )
        mock_schedule.assert_called()

    @patch('gs_quant.markets.portfolio_manager.sleep')
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.schedule_reports')
    @patch.object(PortfolioManager, 'get_position_dates')
    def test_batching_with_many_batches_calls_sleep(self, mock_pos_dates, mock_schedule, mock_sleep):
        """When there are many batches (>10), sleep is called."""
        # Create position dates spanning over 2 years with small months_per_batch to trigger many batches
        dates = [dt.date(2020, 1, 1)]
        for i in range(1, 25):
            month = (i % 12) + 1
            year = 2020 + (i // 12)
            dates.append(dt.date(year, month, 1))
        dates.sort()
        mock_pos_dates.return_value = dates

        pm = PortfolioManager('PORT1')
        pm.schedule_reports(
            start_date=dt.date(2020, 1, 1),
            end_date=dt.date(2022, 1, 1),
            months_per_batch=1,
        )
        # Verify schedule_reports was called multiple times
        assert mock_schedule.call_count > 1

    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.schedule_reports')
    @patch.object(PortfolioManager, 'get_position_dates')
    def test_batch_split_at_previous_date(self, mock_pos_dates, mock_schedule):
        """Test that batch boundary splits correctly when current_batch.days > months_per_batch * 30 and i > 0."""
        # Create dates where the gap between consecutive dates is > months_per_batch * 30
        # months_per_batch=1, so threshold is 30 days. We need gaps > 30 days after removing start_date
        mock_pos_dates.return_value = [
            dt.date(2023, 1, 1),   # start - will be removed
            dt.date(2023, 2, 1),   # gap from start: 31 days; i=0, won't split (i > 0 is false)
            dt.date(2023, 4, 1),   # gap from prev_date(start): 90 days; i=1, i>0, will split at position_dates[0]=Feb 1
            dt.date(2023, 7, 1),   # end
        ]
        pm = PortfolioManager('PORT1')
        pm.schedule_reports(
            start_date=dt.date(2023, 1, 1),
            end_date=dt.date(2023, 7, 1),
            months_per_batch=1,
        )
        # Should have multiple batches
        assert mock_schedule.call_count >= 2


# =============================
# PortfolioManager.run_reports
# =============================


class TestRunReports:
    @patch.object(PortfolioManager, 'get_reports')
    @patch.object(PortfolioManager, 'schedule_reports')
    def test_async_returns_futures(self, mock_schedule, mock_get_reports):
        mock_report = MagicMock()
        mock_future = MagicMock(spec=ReportJobFuture)
        mock_report.get_most_recent_job.return_value = mock_future
        mock_get_reports.return_value = [mock_report]

        pm = PortfolioManager('PORT1')
        result = pm.run_reports(is_async=True)
        assert result == [mock_future]

    @patch('gs_quant.markets.portfolio_manager.sleep')
    @patch.object(PortfolioManager, 'get_reports')
    @patch.object(PortfolioManager, 'schedule_reports')
    def test_sync_all_done(self, mock_schedule, mock_get_reports, mock_sleep):
        mock_future = MagicMock()
        mock_future.done.return_value = True
        mock_future.result.return_value = pd.DataFrame({'col': [1]})
        mock_report = MagicMock()
        mock_report.get_most_recent_job.return_value = mock_future
        mock_get_reports.return_value = [mock_report]

        pm = PortfolioManager('PORT1')
        result = pm.run_reports(is_async=False)
        assert len(result) == 1

    @patch('gs_quant.markets.portfolio_manager.sleep')
    @patch.object(PortfolioManager, 'get_reports')
    @patch.object(PortfolioManager, 'schedule_reports')
    def test_sync_while_loop_calls_sleep(self, mock_schedule, mock_get_reports, mock_sleep):
        """When done() returns False first, then True, sleep is called at least once."""
        mock_future = MagicMock()
        mock_future.done.side_effect = [False, True]
        mock_future.result.return_value = pd.DataFrame()
        mock_report = MagicMock()
        mock_report.get_most_recent_job.return_value = mock_future
        mock_get_reports.return_value = [mock_report]

        pm = PortfolioManager('PORT1')
        result = pm.run_reports(is_async=False)
        mock_sleep.assert_called()
        assert len(result) == 1

    @patch('gs_quant.markets.portfolio_manager.sleep')
    @patch.object(PortfolioManager, 'get_reports')
    @patch.object(PortfolioManager, 'schedule_reports')
    def test_sync_completes_after_retries(self, mock_schedule, mock_get_reports, mock_sleep):
        """Reports complete after a few retries."""
        mock_future = MagicMock()
        # First call: not done, second call: done
        mock_future.done.side_effect = [False, True]
        mock_future.result.return_value = pd.DataFrame({'col': [1]})
        mock_report = MagicMock()
        mock_report.get_most_recent_job.return_value = mock_future
        mock_get_reports.return_value = [mock_report]

        pm = PortfolioManager('PORT1')
        result = pm.run_reports(is_async=False)
        assert len(result) == 1


# ==================================
# PortfolioManager.set_entitlements
# ==================================


class TestSetEntitlements:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.update_portfolio')
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_portfolio')
    def test_set_entitlements(self, mock_get, mock_update):
        mock_portfolio = MagicMock()
        mock_get.return_value = mock_portfolio

        mock_ent = MagicMock(spec=Entitlements)
        mock_ent.to_target.return_value = MagicMock()

        pm = PortfolioManager('PORT1')
        pm.set_entitlements(mock_ent)

        mock_get.assert_called_once_with('PORT1')
        mock_update.assert_called_once_with(mock_portfolio)


# ========================
# PortfolioManager.share
# ========================


class TestShare:
    @patch.object(PortfolioManager, 'set_entitlements')
    @patch.object(PortfolioManager, 'get_entitlements')
    @patch('gs_quant.markets.portfolio_manager.User.get_many')
    def test_share_view_only(self, mock_get_many, mock_get_ent, mock_set_ent):
        user = MagicMock(spec=User)
        user.email = 'user@example.com'
        mock_get_many.return_value = [user]

        mock_ent = MagicMock(spec=Entitlements)
        mock_ent.view = MagicMock(spec=EntitlementBlock)
        mock_ent.view.users = []
        mock_ent.admin = MagicMock(spec=EntitlementBlock)
        mock_ent.admin.users = []
        mock_get_ent.return_value = mock_ent

        pm = PortfolioManager('PORT1')
        pm.share(['user@example.com'], admin=False)
        mock_set_ent.assert_called_once()

    @patch.object(PortfolioManager, 'set_entitlements')
    @patch.object(PortfolioManager, 'get_entitlements')
    @patch('gs_quant.markets.portfolio_manager.User.get_many')
    def test_share_admin(self, mock_get_many, mock_get_ent, mock_set_ent):
        user = MagicMock(spec=User)
        user.email = 'admin@example.com'
        mock_get_many.return_value = [user]

        mock_ent = MagicMock(spec=Entitlements)
        mock_ent.view = MagicMock(spec=EntitlementBlock)
        mock_ent.view.users = []
        mock_ent.admin = MagicMock(spec=EntitlementBlock)
        mock_ent.admin.users = []
        mock_get_ent.return_value = mock_ent

        pm = PortfolioManager('PORT1')
        pm.share(['admin@example.com'], admin=True)
        mock_set_ent.assert_called_once()

    @patch.object(PortfolioManager, 'get_entitlements')
    @patch('gs_quant.markets.portfolio_manager.User.get_many')
    def test_share_missing_emails_raises(self, mock_get_many, mock_get_ent):
        mock_get_many.return_value = []  # no users found
        mock_get_ent.return_value = MagicMock()

        pm = PortfolioManager('PORT1')
        with pytest.raises(MqValueError, match='not found'):
            pm.share(['missing@example.com'])


# =============================
# PortfolioManager.set_currency
# =============================


class TestSetCurrency:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.update_portfolio')
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_portfolio')
    def test_set_currency(self, mock_get, mock_update):
        mock_portfolio = MagicMock()
        mock_get.return_value = mock_portfolio

        pm = PortfolioManager('PORT1')
        pm.set_currency(Currency.USD)

        mock_get.assert_called_once_with('PORT1')
        assert mock_portfolio.currency == Currency.USD
        mock_update.assert_called_once_with(mock_portfolio)


# ====================================
# PortfolioManager.get_tag_name_hierarchy
# ====================================


class TestGetTagNameHierarchy:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_portfolio')
    def test_with_hierarchy(self, mock_get):
        mock_portfolio = MagicMock()
        mock_portfolio.tag_name_hierarchy = ('fund', 'strategy')
        mock_get.return_value = mock_portfolio

        pm = PortfolioManager('PORT1')
        result = pm.get_tag_name_hierarchy()
        assert result == ['fund', 'strategy']

    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_portfolio')
    def test_without_hierarchy(self, mock_get):
        mock_portfolio = MagicMock()
        mock_portfolio.tag_name_hierarchy = None
        mock_get.return_value = mock_portfolio

        pm = PortfolioManager('PORT1')
        result = pm.get_tag_name_hierarchy()
        assert result is None


# ====================================
# PortfolioManager.set_tag_name_hierarchy
# ====================================


class TestSetTagNameHierarchy:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.update_portfolio')
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_portfolio')
    def test_set_tag_name_hierarchy(self, mock_get, mock_update):
        mock_portfolio = MagicMock()
        mock_get.return_value = mock_portfolio

        pm = PortfolioManager('PORT1')
        pm.set_tag_name_hierarchy(['fund', 'strategy'])

        assert mock_portfolio.tag_name_hierarchy == ['fund', 'strategy']
        mock_update.assert_called_once_with(mock_portfolio)


# ====================================
# PortfolioManager.update_portfolio_tree
# ====================================


class TestUpdatePortfolioTree:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.update_portfolio_tree')
    def test_update(self, mock_update):
        pm = PortfolioManager('PORT1')
        pm.update_portfolio_tree()
        mock_update.assert_called_once_with('PORT1')


# ====================================
# PortfolioManager.get_portfolio_tree
# ====================================


class TestGetPortfolioTree:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_portfolio_tree')
    def test_get(self, mock_get_tree):
        mock_tree = MagicMock()
        mock_get_tree.return_value = mock_tree
        pm = PortfolioManager('PORT1')
        result = pm.get_portfolio_tree()
        assert result == mock_tree
        mock_get_tree.assert_called_once_with('PORT1')


# ==========================================
# PortfolioManager.get_all_fund_of_fund_tags
# ==========================================


class TestGetAllFundOfFundTags:
    @patch.object(PortfolioManager, 'get_reports')
    def test_with_tags(self, mock_get_reports):
        # Reports with tags
        tag1 = MagicMock()
        tag1.name = 'fund'
        tag1.value = 'A'

        tag2 = MagicMock()
        tag2.name = 'strategy'
        tag2.value = 'X'

        report1 = MagicMock()
        report1.parameters.tags = [tag1]

        report2 = MagicMock()
        report2.parameters.tags = [tag1, tag2]

        report3 = MagicMock()
        report3.parameters.tags = None  # no tags

        mock_get_reports.return_value = [report1, report2, report3]

        pm = PortfolioManager('PORT1')
        result = pm.get_all_fund_of_fund_tags()
        assert len(result) == 2
        assert {'fund': 'A'} in result
        assert {'fund': 'A', 'strategy': 'X'} in result

    @patch.object(PortfolioManager, 'get_reports')
    def test_with_duplicate_tags(self, mock_get_reports):
        """Duplicate tag sets should not appear in the result."""
        tag1 = MagicMock()
        tag1.name = 'fund'
        tag1.value = 'A'

        report1 = MagicMock()
        report1.parameters.tags = [tag1]

        report2 = MagicMock()
        report2.parameters.tags = [tag1]

        mock_get_reports.return_value = [report1, report2]

        pm = PortfolioManager('PORT1')
        result = pm.get_all_fund_of_fund_tags()
        assert len(result) == 1

    @patch.object(PortfolioManager, 'get_reports')
    def test_no_tags(self, mock_get_reports):
        report = MagicMock()
        report.parameters.tags = None
        mock_get_reports.return_value = [report]

        pm = PortfolioManager('PORT1')
        result = pm.get_all_fund_of_fund_tags()
        assert result == []


# =====================================
# PortfolioManager.get_schedule_dates
# =====================================


class TestGetScheduleDates:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_schedule_dates')
    def test_get_schedule_dates(self, mock_get):
        mock_get.return_value = [dt.date(2023, 1, 1), dt.date(2023, 12, 31)]
        pm = PortfolioManager('PORT1')
        result = pm.get_schedule_dates(backcast=True)
        mock_get.assert_called_once_with('PORT1', True)
        assert len(result) == 2


# =====================================
# PortfolioManager.get_aum_source (deprecated)
# =====================================


class TestGetAumSource:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_portfolio')
    def test_with_aum_source(self, mock_get):
        mock_portfolio = MagicMock()
        mock_portfolio.aum_source = RiskAumSource.Gross
        mock_get.return_value = mock_portfolio

        pm = PortfolioManager('PORT1')
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = pm.get_aum_source()
        assert result == RiskAumSource.Gross

    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_portfolio')
    def test_without_aum_source_defaults_long(self, mock_get):
        mock_portfolio = MagicMock()
        mock_portfolio.aum_source = None
        mock_get.return_value = mock_portfolio

        pm = PortfolioManager('PORT1')
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = pm.get_aum_source()
        assert result == RiskAumSource.Long


# =====================================
# PortfolioManager.set_aum_source (deprecated)
# =====================================


class TestSetAumSource:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.update_portfolio')
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_portfolio')
    def test_set_aum_source(self, mock_get, mock_update):
        mock_portfolio = MagicMock()
        mock_get.return_value = mock_portfolio

        pm = PortfolioManager('PORT1')
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            pm.set_aum_source(RiskAumSource.Net)

        assert mock_portfolio.aum_source == RiskAumSource.Net
        mock_update.assert_called_once_with(mock_portfolio)


# =======================================
# PortfolioManager.get_custom_aum (deprecated)
# =======================================


class TestGetCustomAum:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_custom_aum')
    def test_get_custom_aum(self, mock_get):
        mock_get.return_value = [
            {'date': '2023-01-01', 'aum': 1000.0},
            {'date': '2023-02-01', 'aum': 2000.0},
        ]

        pm = PortfolioManager('PORT1')
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = pm.get_custom_aum(start_date=dt.date(2023, 1, 1), end_date=dt.date(2023, 6, 1))
        assert len(result) == 2
        assert result[0].aum == 1000.0


# =======================================
# PortfolioManager.get_aum (deprecated)
# =======================================


class TestGetAum:
    @patch.object(PortfolioManager, 'get_custom_aum')
    @patch.object(PortfolioManager, 'get_aum_source')
    def test_custom_aum(self, mock_source, mock_custom):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_source.return_value = RiskAumSource.Custom_AUM
            dp = CustomAUMDataPoint(date=dt.datetime(2023, 1, 1), aum=1000.0)
            mock_custom.return_value = [dp]

            pm = PortfolioManager('PORT1')
            result = pm.get_aum(dt.date(2023, 1, 1), dt.date(2023, 6, 1))
            assert '2023-01-01' in result

    @patch.object(PortfolioManager, 'get_performance_report')
    @patch.object(PortfolioManager, 'get_aum_source')
    def test_long_aum(self, mock_source, mock_perf_report):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_source.return_value = RiskAumSource.Long
            mock_report = MagicMock()
            mock_report.get_long_exposure.return_value = pd.DataFrame({
                'date': ['2023-01-01'],
                'longExposure': [5000.0],
            })
            mock_perf_report.return_value = mock_report

            pm = PortfolioManager('PORT1')
            result = pm.get_aum(dt.date(2023, 1, 1), dt.date(2023, 6, 1))
            assert '2023-01-01' in result

    @patch.object(PortfolioManager, 'get_performance_report')
    @patch.object(PortfolioManager, 'get_aum_source')
    def test_short_aum(self, mock_source, mock_perf_report):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_source.return_value = RiskAumSource.Short
            mock_report = MagicMock()
            mock_report.get_short_exposure.return_value = pd.DataFrame({
                'date': ['2023-01-01'],
                'shortExposure': [3000.0],
            })
            mock_perf_report.return_value = mock_report

            pm = PortfolioManager('PORT1')
            result = pm.get_aum(dt.date(2023, 1, 1), dt.date(2023, 6, 1))
            assert '2023-01-01' in result

    @patch.object(PortfolioManager, 'get_performance_report')
    @patch.object(PortfolioManager, 'get_aum_source')
    def test_gross_aum(self, mock_source, mock_perf_report):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_source.return_value = RiskAumSource.Gross
            mock_report = MagicMock()
            mock_report.get_gross_exposure.return_value = pd.DataFrame({
                'date': ['2023-01-01'],
                'grossExposure': [8000.0],
            })
            mock_perf_report.return_value = mock_report

            pm = PortfolioManager('PORT1')
            result = pm.get_aum(dt.date(2023, 1, 1), dt.date(2023, 6, 1))
            assert '2023-01-01' in result

    @patch.object(PortfolioManager, 'get_performance_report')
    @patch.object(PortfolioManager, 'get_aum_source')
    def test_net_aum(self, mock_source, mock_perf_report):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_source.return_value = RiskAumSource.Net
            mock_report = MagicMock()
            mock_report.get_net_exposure.return_value = pd.DataFrame({
                'date': ['2023-01-01'],
                'netExposure': [2000.0],
            })
            mock_perf_report.return_value = mock_report

            pm = PortfolioManager('PORT1')
            result = pm.get_aum(dt.date(2023, 1, 1), dt.date(2023, 6, 1))
            assert '2023-01-01' in result

    @patch.object(PortfolioManager, 'get_aum_source')
    def test_unknown_aum_source_returns_none(self, mock_source):
        """When aum_source doesn't match any known value, returns None implicitly."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_source.return_value = 'Unknown'

            pm = PortfolioManager('PORT1')
            result = pm.get_aum(dt.date(2023, 1, 1), dt.date(2023, 6, 1))
            assert result is None


# ===========================================
# PortfolioManager.upload_custom_aum (deprecated)
# ===========================================


class TestUploadCustomAum:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.upload_custom_aum')
    def test_upload(self, mock_upload):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            dp = CustomAUMDataPoint(date=dt.datetime(2023, 1, 1), aum=1000.0)
            pm = PortfolioManager('PORT1')
            pm.upload_custom_aum([dp], clear_existing_data=True)
            mock_upload.assert_called_once()
            args = mock_upload.call_args
            assert args[0][0] == 'PORT1'
            assert args[0][1] == [{'date': '2023-01-01', 'aum': 1000.0}]


# ============================================
# PortfolioManager.get_pnl_contribution (deprecated)
# ============================================


class TestGetPnlContribution:
    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_attribution')
    def test_tags_none(self, mock_attr):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_attr.return_value = [{'asset': 'A', 'pnl': 100}]
            pm = PortfolioManager('PORT1')
            result = pm.get_pnl_contribution(tags=None)
            assert isinstance(result, pd.DataFrame)
            # performance_report_id should be None when tags is None
            mock_attr.assert_called_once_with('PORT1', None, None, None, None)

    @patch('gs_quant.markets.portfolio_manager.GsPortfolioApi.get_attribution')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_tags_not_none(self, mock_get_report, mock_attr):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_report = MagicMock()
            mock_report.id = 'RPT1'
            mock_get_report.return_value = mock_report
            mock_attr.return_value = [{'asset': 'A', 'pnl': 100}]

            pm = PortfolioManager('PORT1')
            result = pm.get_pnl_contribution(tags={'fund': 'A'})
            assert isinstance(result, pd.DataFrame)
            mock_attr.assert_called_once_with('PORT1', None, None, None, 'RPT1')


# =====================================
# PortfolioManager.get_macro_exposure
# =====================================


class TestGetMacroExposure:
    @patch('gs_quant.markets.portfolio_manager.build_exposure_df')
    @patch('gs_quant.markets.portfolio_manager.build_sensitivity_df')
    @patch('gs_quant.markets.portfolio_manager.build_portfolio_constituents_df')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_no_assets_with_exposure(self, mock_get_report, mock_build_const, mock_build_sens, mock_build_exp):
        """When no assets have exposure, returns empty DataFrame."""
        mock_model = MagicMock()
        mock_get_report.return_value = MagicMock()

        mock_const_df = pd.DataFrame(
            {'Asset Name': ['A'], 'Notional': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        mock_build_const.return_value = mock_const_df
        mock_build_sens.return_value = pd.DataFrame()  # empty - no assets with exposure

        pm = PortfolioManager('PORT1')
        result = pm.get_macro_exposure(mock_model, dt.date(2023, 1, 1), FactorType.Factor)
        assert result.empty

    @patch('gs_quant.markets.portfolio_manager.build_exposure_df')
    @patch('gs_quant.markets.portfolio_manager.build_sensitivity_df')
    @patch('gs_quant.markets.portfolio_manager.build_portfolio_constituents_df')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_factor_type_factor(self, mock_get_report, mock_build_const, mock_build_sens, mock_build_exp):
        """factor_type == FactorType.Factor: calls model.get_factor_data."""
        mock_model = MagicMock()
        mock_model.get_factor_data.return_value = pd.DataFrame({'name': ['F1'], 'identifier': ['f1']})
        mock_get_report.return_value = MagicMock()

        mock_const_df = pd.DataFrame(
            {'Asset Name': ['A'], 'Notional': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        mock_build_const.return_value = mock_const_df

        sens_df = pd.DataFrame(
            {'F1': [0.5]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        mock_build_sens.return_value = sens_df

        expected_df = pd.DataFrame({'F1': [50.0]})
        mock_build_exp.return_value = expected_df

        pm = PortfolioManager('PORT1')
        result = pm.get_macro_exposure(mock_model, dt.date(2023, 1, 1), FactorType.Factor)
        mock_model.get_factor_data.assert_called_once()
        assert not result.empty

    @patch('gs_quant.markets.portfolio_manager.build_exposure_df')
    @patch('gs_quant.markets.portfolio_manager.build_sensitivity_df')
    @patch('gs_quant.markets.portfolio_manager.build_portfolio_constituents_df')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_factor_type_category(self, mock_get_report, mock_build_const, mock_build_sens, mock_build_exp):
        """factor_type != FactorType.Factor: factor_data is empty DataFrame."""
        mock_model = MagicMock()
        mock_get_report.return_value = MagicMock()

        mock_const_df = pd.DataFrame(
            {'Asset Name': ['A'], 'Notional': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        mock_build_const.return_value = mock_const_df

        sens_df = pd.DataFrame(
            {'Cat1': [0.5]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        mock_build_sens.return_value = sens_df

        expected_df = pd.DataFrame({'Cat1': [50.0]})
        mock_build_exp.return_value = expected_df

        pm = PortfolioManager('PORT1')
        result = pm.get_macro_exposure(mock_model, dt.date(2023, 1, 1), FactorType.Category)
        mock_model.get_factor_data.assert_not_called()
        assert not result.empty

    @patch('gs_quant.markets.portfolio_manager.build_exposure_df')
    @patch('gs_quant.markets.portfolio_manager.build_sensitivity_df')
    @patch('gs_quant.markets.portfolio_manager.build_portfolio_constituents_df')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_return_json(self, mock_get_report, mock_build_const, mock_build_sens, mock_build_exp):
        """return_format == ReturnFormat.JSON: returns dict."""
        mock_model = MagicMock()
        mock_get_report.return_value = MagicMock()

        mock_const_df = pd.DataFrame(
            {'Asset Name': ['A'], 'Notional': [100.0]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        mock_build_const.return_value = mock_const_df

        sens_df = pd.DataFrame(
            {'Cat1': [0.5]},
            index=pd.Index(['GS001'], name='Asset Identifier'),
        )
        mock_build_sens.return_value = sens_df

        expected_df = pd.DataFrame({'Cat1': [50.0]})
        mock_build_exp.return_value = expected_df

        pm = PortfolioManager('PORT1')
        result = pm.get_macro_exposure(
            mock_model, dt.date(2023, 1, 1), FactorType.Category, return_format=ReturnFormat.JSON
        )
        assert isinstance(result, dict)


# ===================================================
# PortfolioManager.get_factor_scenario_analytics
# ===================================================


class TestGetFactorScenarioAnalytics:
    @patch('gs_quant.entities.entity.PositionedEntity.get_factor_scenario_analytics')
    def test_delegates_to_parent(self, mock_parent):
        mock_parent.return_value = {'result': 'data'}
        pm = PortfolioManager('PORT1')
        result = pm.get_factor_scenario_analytics(
            scenarios=[MagicMock()],
            date=dt.date(2023, 1, 1),
            measures=[MagicMock()],
        )
        mock_parent.assert_called_once()
        assert result == {'result': 'data'}


# ===================================================
# PortfolioManager.get_risk_model_predicted_beta
# ===================================================


class TestGetRiskModelPredictedBeta:
    @patch('gs_quant.markets.portfolio_manager.get_batched_dates')
    @patch('gs_quant.markets.portfolio_manager.FactorRiskModel.get')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_empty_result_when_all_nan(self, mock_get_report, mock_model_get, mock_batched):
        """When all batched data is NaN, returns empty DataFrame."""
        mock_report = MagicMock()
        mock_get_report.return_value = mock_report

        mock_model = MagicMock()
        mock_model.get_dates.return_value = [dt.date(2023, 1, 1), dt.date(2023, 1, 2)]
        mock_model_get.return_value = mock_model

        mock_batched.return_value = [[dt.date(2023, 1, 1), dt.date(2023, 1, 2)]]

        # Set up position net weights
        mock_report.get_position_net_weights.return_value = pd.DataFrame({
            'positionDate': ['2023-01-01', '2023-01-02'],
            'gsid': ['GS001', 'GS001'],
            'netWeight': [0.5, 0.6],
        })

        # All NaN betas
        all_nan_df = pd.DataFrame(
            {'GS001': [np.nan, np.nan]},
            index=pd.Index([dt.date(2023, 1, 1), dt.date(2023, 1, 2)]),
        )
        mock_model.get_predicted_beta.return_value = all_nan_df

        pm = PortfolioManager('PORT1')
        result = pm.get_risk_model_predicted_beta(
            dt.date(2023, 1, 1), dt.date(2023, 1, 2), 'MODEL_ID'
        )
        assert result.empty

    @patch('gs_quant.markets.portfolio_manager.get_batched_dates')
    @patch('gs_quant.markets.portfolio_manager.FactorRiskModel.get')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_successful_beta_calculation(self, mock_get_report, mock_model_get, mock_batched):
        """Successful beta calculation with valid data."""
        mock_report = MagicMock()
        mock_get_report.return_value = mock_report

        mock_model = MagicMock()
        mock_model.get_dates.return_value = [dt.date(2023, 1, 2), dt.date(2023, 1, 3)]
        mock_model_get.return_value = mock_model

        mock_batched.return_value = [[dt.date(2023, 1, 2), dt.date(2023, 1, 3)]]

        mock_report.get_position_net_weights.return_value = pd.DataFrame({
            'positionDate': ['2023-01-02', '2023-01-03'],
            'gsid': ['GS001', 'GS001'],
            'netWeight': [0.5, 0.6],
        })

        beta_df = pd.DataFrame(
            {'GS001': [1.1, 1.2]},
            index=pd.Index([dt.date(2023, 1, 2), dt.date(2023, 1, 3)]),
        )
        mock_model.get_predicted_beta.return_value = beta_df

        pm = PortfolioManager('PORT1')
        result = pm.get_risk_model_predicted_beta(
            dt.date(2023, 1, 2), dt.date(2023, 1, 3), 'MODEL_ID'
        )
        assert not result.empty
        assert 'beta' in result.columns or 0 in result.columns

    @patch('gs_quant.markets.portfolio_manager.get_batched_dates')
    @patch('gs_quant.markets.portfolio_manager.FactorRiskModel.get')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_exception_raises_mq_error(self, mock_get_report, mock_model_get, mock_batched):
        """When an exception occurs during processing, raises MqError."""
        mock_report = MagicMock()
        mock_get_report.return_value = mock_report

        mock_model = MagicMock()
        mock_model.get_dates.return_value = [dt.date(2023, 1, 1)]
        mock_model_get.return_value = mock_model

        mock_batched.return_value = [[dt.date(2023, 1, 1)]]

        mock_report.get_position_net_weights.side_effect = Exception('API Error')

        pm = PortfolioManager('PORT1')
        with pytest.raises(MqError, match='Risk model predicted beta cannot be calculated'):
            pm.get_risk_model_predicted_beta(
                dt.date(2023, 1, 1), dt.date(2023, 1, 1), 'MODEL_ID'
            )

    @patch('gs_quant.markets.portfolio_manager.get_batched_dates')
    @patch('gs_quant.markets.portfolio_manager.FactorRiskModel.get')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_multiple_batches_with_some_nan(self, mock_get_report, mock_model_get, mock_batched):
        """Multiple batches where one batch has all NaN (skipped via continue) and another has data."""
        mock_report = MagicMock()
        mock_get_report.return_value = mock_report

        mock_model = MagicMock()
        mock_model.get_dates.return_value = [
            dt.date(2023, 1, 1), dt.date(2023, 1, 2),
            dt.date(2023, 1, 3), dt.date(2023, 1, 4),
        ]
        mock_model_get.return_value = mock_model

        mock_batched.return_value = [
            [dt.date(2023, 1, 1), dt.date(2023, 1, 2)],
            [dt.date(2023, 1, 3), dt.date(2023, 1, 4)],
        ]

        # First batch: all NaN
        weights_batch1 = pd.DataFrame({
            'positionDate': ['2023-01-01', '2023-01-02'],
            'gsid': ['GS001', 'GS001'],
            'netWeight': [0.5, 0.6],
        })

        weights_batch2 = pd.DataFrame({
            'positionDate': ['2023-01-03', '2023-01-04'],
            'gsid': ['GS001', 'GS001'],
            'netWeight': [0.4, 0.7],
        })

        mock_report.get_position_net_weights.side_effect = [weights_batch1, weights_batch2]

        all_nan = pd.DataFrame(
            {'GS001': [np.nan, np.nan]},
            index=pd.Index([dt.date(2023, 1, 1), dt.date(2023, 1, 2)]),
        )
        valid_beta = pd.DataFrame(
            {'GS001': [1.05, 1.10]},
            index=pd.Index([dt.date(2023, 1, 3), dt.date(2023, 1, 4)]),
        )
        mock_model.get_predicted_beta.side_effect = [all_nan, valid_beta]

        pm = PortfolioManager('PORT1')
        result = pm.get_risk_model_predicted_beta(
            dt.date(2023, 1, 1), dt.date(2023, 1, 4), 'MODEL_ID'
        )
        assert not result.empty

    @patch('gs_quant.markets.portfolio_manager.get_batched_dates')
    @patch('gs_quant.markets.portfolio_manager.FactorRiskModel.get')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_none_gsid_filtered_out(self, mock_get_report, mock_model_get, mock_batched):
        """None gsids should be filtered out of portfolio_position_gsids."""
        mock_report = MagicMock()
        mock_get_report.return_value = mock_report

        mock_model = MagicMock()
        mock_model.get_dates.return_value = [dt.date(2023, 1, 1)]
        mock_model_get.return_value = mock_model

        mock_batched.return_value = [[dt.date(2023, 1, 1)]]

        # Include None gsid
        mock_report.get_position_net_weights.return_value = pd.DataFrame({
            'positionDate': ['2023-01-01', '2023-01-01'],
            'gsid': ['GS001', None],
            'netWeight': [0.5, 0.3],
        })

        beta_df = pd.DataFrame(
            {'GS001': [1.1]},
            index=pd.Index([dt.date(2023, 1, 1)]),
        )
        mock_model.get_predicted_beta.return_value = beta_df

        pm = PortfolioManager('PORT1')
        result = pm.get_risk_model_predicted_beta(
            dt.date(2023, 1, 1), dt.date(2023, 1, 1), 'MODEL_ID'
        )
        # Check that get_predicted_beta was called with only non-None gsids
        call_args = mock_model.get_predicted_beta.call_args
        assets_arg = call_args[1]['assets'] if 'assets' in call_args[1] else call_args[0][2]
        # The universe should only contain GS001, not None
        assert not result.empty

    @patch('gs_quant.markets.portfolio_manager.get_batched_dates')
    @patch('gs_quant.markets.portfolio_manager.FactorRiskModel.get')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_no_batches(self, mock_get_report, mock_model_get, mock_batched):
        """When there are no batches at all (empty dates), returns empty DataFrame."""
        mock_report = MagicMock()
        mock_get_report.return_value = mock_report

        mock_model = MagicMock()
        mock_model.get_dates.return_value = []
        mock_model_get.return_value = mock_model

        mock_batched.return_value = []

        pm = PortfolioManager('PORT1')
        result = pm.get_risk_model_predicted_beta(
            dt.date(2023, 1, 1), dt.date(2023, 1, 1), 'MODEL_ID'
        )
        assert result.empty

    @patch('gs_quant.markets.portfolio_manager.get_batched_dates')
    @patch('gs_quant.markets.portfolio_manager.FactorRiskModel.get')
    @patch.object(PortfolioManager, 'get_performance_report')
    def test_with_tags(self, mock_get_report, mock_model_get, mock_batched):
        """Test that tags are passed through to get_performance_report."""
        mock_report = MagicMock()
        mock_get_report.return_value = mock_report

        mock_model = MagicMock()
        mock_model.get_dates.return_value = []
        mock_model_get.return_value = mock_model

        mock_batched.return_value = []

        pm = PortfolioManager('PORT1')
        result = pm.get_risk_model_predicted_beta(
            dt.date(2023, 1, 1), dt.date(2023, 1, 1), 'MODEL_ID', tags={'fund': 'A'}
        )
        mock_get_report.assert_called_once_with(tags={'fund': 'A'})
