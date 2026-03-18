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

from gs_quant.quote_reports.core import (
    quote_report_from_dict,
    quote_reports_from_dicts,
    custom_comment_from_dict,
    custom_comments_from_dicts,
    hedge_type_from_dict,
    hedge_type_from_dicts,
)
from gs_quant.workflow import (
    VisualStructuringReport,
    BinaryImageComments,
    HyperLinkImageComments,
    CustomDeltaHedge,
    DeltaHedge,
    HedgeTypes,
)
from gs_quant.base import CustomComments


class TestQuoteReportFromDict:
    def test_none(self):
        assert quote_report_from_dict(None) is None

    def test_already_report(self):
        report = VisualStructuringReport()
        result = quote_report_from_dict(report)
        assert result is report

    def test_visual_structuring_report(self):
        result = quote_report_from_dict({'reportType': 'VisualStructuringReport'})
        assert isinstance(result, VisualStructuringReport)

    def test_unknown_type(self):
        result = quote_report_from_dict({'reportType': 'Unknown'})
        assert result is None

    def test_no_type(self):
        result = quote_report_from_dict({})
        assert result is None


class TestQuoteReportsFromDicts:
    def test_none(self):
        assert quote_reports_from_dicts(None) is None

    def test_list(self):
        result = quote_reports_from_dicts([
            {'reportType': 'VisualStructuringReport'},
            {'reportType': 'Unknown'},
        ])
        assert len(result) == 2
        assert isinstance(result[0], VisualStructuringReport)
        assert result[1] is None


class TestCustomCommentFromDict:
    def test_none(self):
        assert custom_comment_from_dict(None) is None

    def test_already_custom_comments(self):
        obj = BinaryImageComments()
        result = custom_comment_from_dict(obj)
        assert result is obj

    def test_binary_image_comments(self):
        result = custom_comment_from_dict({'commentType': 'binaryImageComments'})
        assert isinstance(result, BinaryImageComments)

    def test_hyperlink_image_comments(self):
        result = custom_comment_from_dict({'commentType': 'hyperLinkImageComments'})
        assert isinstance(result, HyperLinkImageComments)

    def test_unknown_type(self):
        result = custom_comment_from_dict({'commentType': 'unknown'})
        assert result is None


class TestCustomCommentsFromDicts:
    def test_none(self):
        assert custom_comments_from_dicts(None) is None

    def test_list(self):
        result = custom_comments_from_dicts([
            {'commentType': 'binaryImageComments'},
            {'commentType': 'hyperLinkImageComments'},
        ])
        assert len(result) == 2


class TestHedgeTypeFromDict:
    def test_none(self):
        assert hedge_type_from_dict(None) is None

    def test_already_hedge_type(self):
        obj = DeltaHedge()
        result = hedge_type_from_dict(obj)
        assert result is obj

    def test_custom_delta_hedge(self):
        result = hedge_type_from_dict({'type': 'CustomDeltaHedge'})
        assert isinstance(result, CustomDeltaHedge)

    def test_delta_hedge(self):
        result = hedge_type_from_dict({'type': 'DeltaHedge'})
        assert isinstance(result, DeltaHedge)

    def test_unknown_type(self):
        result = hedge_type_from_dict({'type': 'Unknown'})
        assert result is None


class TestHedgeTypeFromDicts:
    def test_none(self):
        assert hedge_type_from_dicts(None) is None

    def test_list(self):
        result = hedge_type_from_dicts([
            {'type': 'DeltaHedge'},
            {'type': 'CustomDeltaHedge'},
        ])
        assert len(result) == 2
