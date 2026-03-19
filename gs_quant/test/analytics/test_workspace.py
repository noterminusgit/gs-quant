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

import logging
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from gs_quant.analytics.workspaces.components import (
    PlotComponent,
    DataGridComponent,
    MonitorComponent,
    DataScreenerComponent,
    RelatedLink,
    RelatedLinkType,
)
from gs_quant.analytics.workspaces.workspace import (
    Workspace,
    WorkspaceRow,
    WorkspaceColumn,
    WorkspaceCallToAction,
    WorkspaceTab,
)
from gs_quant.common import Entitlements as Entitlements_
from gs_quant.entities.entitlements import Entitlements
from gs_quant.errors import MqValueError, MqRequestError


# --------------------------------------------------------------------------- #
#  WorkspaceCallToAction
# --------------------------------------------------------------------------- #

class TestWorkspaceCallToAction:
    def test_as_dict_with_related_link_actions(self):
        link = RelatedLink(type_=RelatedLinkType.external, name='L1', link='https://example.com')
        cta = WorkspaceCallToAction(actions=[link], text='Click here', name='CTA')
        d = cta.as_dict()
        assert d['text'] == 'Click here'
        assert d['name'] == 'CTA'
        assert len(d['actions']) == 1
        assert d['actions'][0]['type'] == 'external'

    def test_as_dict_with_plain_dict_actions(self):
        """Branch: action is not a RelatedLink instance."""
        raw_action = {'type': 'internal', 'name': 'raw', 'link': '/some/path'}
        cta = WorkspaceCallToAction(actions=[raw_action], text='Some text')
        d = cta.as_dict()
        assert d['actions'][0] == raw_action
        assert 'name' not in d  # name is None

    def test_as_dict_without_name(self):
        """Branch: self.name is falsy."""
        cta = WorkspaceCallToAction(actions=[], text='Hi', name=None)
        d = cta.as_dict()
        assert 'name' not in d

    def test_from_dict_with_dict_actions(self):
        obj = {
            'actions': [{'type': 'external', 'name': 'L', 'link': 'http://x'}],
            'text': 'txt',
            'name': 'N',
        }
        cta = WorkspaceCallToAction.from_dict(obj)
        assert isinstance(cta.actions[0], RelatedLink)
        assert cta.text == 'txt'
        assert cta.name == 'N'

    def test_from_dict_with_non_dict_actions(self):
        """Branch: action is not a Dict."""
        link = RelatedLink(type_=RelatedLinkType.external, name='X', link='http://y')
        obj = {'actions': [link], 'text': 'T', 'name': 'N'}
        cta = WorkspaceCallToAction.from_dict(obj)
        assert cta.actions[0] is link


# --------------------------------------------------------------------------- #
#  WorkspaceTab
# --------------------------------------------------------------------------- #

class TestWorkspaceTab:
    def test_as_dict(self):
        tab = WorkspaceTab(id_='alias-1', name='Tab 1')
        assert tab.as_dict() == {'id': 'alias-1', 'name': 'Tab 1'}

    def test_from_dict(self):
        tab = WorkspaceTab.from_dict({'id': 'a', 'name': 'b'})
        assert tab.id_ == 'a'
        assert tab.name == 'b'


# --------------------------------------------------------------------------- #
#  WorkspaceColumn
# --------------------------------------------------------------------------- #

class TestWorkspaceColumn:
    def _make_component(self, width=None, height=200):
        return PlotComponent(height, id_='test-id', width=width)

    def test_components_setter_exceeds_12(self):
        comps = [self._make_component() for _ in range(13)]
        with pytest.raises(MqValueError):
            WorkspaceColumn(components=comps)

    def test_components_setter_total_width_exceeds_12(self):
        """Branch: width_sum + without_width_count > 12."""
        comps = [self._make_component(width=7), self._make_component(width=6)]
        with pytest.raises(MqValueError):
            WorkspaceColumn(components=comps)

    def test_width_property(self):
        col = WorkspaceColumn(components=[self._make_component()], width=6)
        assert col.width == 6
        col.width = 8
        assert col.width == 8

    def test_get_layout_equal_spread(self):
        c1 = self._make_component()
        c2 = self._make_component()
        col = WorkspaceColumn(components=[c1, c2])
        layout, count = col.get_layout(0)
        assert layout == 'c6($0)c6($1)'
        assert count == 2

    def test_get_layout_equal_spread_with_remainder(self):
        """Branch: last_size != 0, last component gets extra width."""
        c1 = self._make_component()
        c2 = self._make_component()
        c3 = self._make_component()
        c4 = self._make_component()
        c5 = self._make_component()
        col = WorkspaceColumn(components=[c1, c2, c3, c4, c5])
        layout, count = col.get_layout(0)
        # 12 / 5 = 2, remainder = 2; last gets 2+2=4
        assert '$4' in layout
        assert count == 5

    def test_get_layout_with_nested_row(self):
        """Branch: component is a WorkspaceRow inside WorkspaceColumn.get_layout."""
        c1 = self._make_component()
        nested_row = WorkspaceRow(components=[c1])
        col = WorkspaceColumn(components=[nested_row])
        layout, count = col.get_layout(0)
        assert 'r(' in layout
        assert count == 1

    def test_get_layout_with_nested_column(self):
        """Branch: component is a WorkspaceColumn inside WorkspaceColumn.get_layout."""
        c1 = self._make_component()
        inner_col = WorkspaceColumn(components=[c1])
        col = WorkspaceColumn(components=[inner_col])
        layout, count = col.get_layout(0)
        assert count == 1

    def test_get_layout_with_widths(self):
        """Branch: width_sum > 0 path."""
        c1 = self._make_component(width=8)
        c2 = self._make_component()  # width is None
        col = WorkspaceColumn(components=[c1, c2])
        layout, count = col.get_layout(0)
        assert 'c8($0)' in layout
        assert count == 2

    def test_get_layout_all_widths_sum_12(self):
        """Branch: width_sum == 12 => default_width = 0."""
        c1 = self._make_component(width=6)
        c2 = self._make_component(width=6)
        col = WorkspaceColumn(components=[c1, c2])
        layout, count = col.get_layout(0)
        assert layout == 'c6($0)c6($1)'
        assert count == 2

    def test_get_layout_last_component_no_width(self):
        """Branch: i == components_length - 1 and not component.width."""
        c1 = self._make_component(width=4)
        c2 = self._make_component()
        col = WorkspaceColumn(components=[c1, c2])
        layout, count = col.get_layout(0)
        assert 'c4($0)' in layout
        assert 'c8($1)' in layout

    def test_get_layout_component_width_none_default(self):
        """Branch: component.width is None (not last) in non-equal path."""
        c1 = self._make_component(width=4)
        c2 = self._make_component()  # width None, not last
        c3 = self._make_component()  # width None, last
        col = WorkspaceColumn(components=[c1, c2, c3])
        layout, count = col.get_layout(0)
        assert count == 3

    def test_add_components_recursive(self):
        c1 = self._make_component()
        c2 = self._make_component()
        inner_row = WorkspaceRow(components=[c1])
        col = WorkspaceColumn(components=[inner_row, c2])
        components = []
        col._add_components(components)
        assert len(components) == 2

    def test_add_components_nested_column(self):
        c1 = self._make_component()
        inner_col = WorkspaceColumn(components=[c1])
        col = WorkspaceColumn(components=[inner_col])
        components = []
        col._add_components(components)
        assert len(components) == 1

    def test_components_setter_existing_components_have_width(self):
        """Branch: existing __components is non-empty => width_sum accumulates from existing."""
        c1 = self._make_component(width=3)
        col = WorkspaceColumn(components=[c1])
        # Now set again with small component that fits
        c2 = self._make_component(width=3)
        col.components = [c2]
        assert len(col.components) == 1

    def test_components_setter_existing_exceeds_width_12(self):
        """Branch: existing width_sum > 12 => raises error."""
        # Create column with wide components first
        c1 = self._make_component(width=6)
        c2 = self._make_component(width=6)
        col = WorkspaceColumn(components=[c1, c2])
        # Now try to set again - the existing width_sum = 12, new = 1, total > 12
        with pytest.raises(MqValueError):
            col.components = [self._make_component(width=1)]

    def test_components_setter_with_workspace_row_in_existing(self):
        """Branch: isinstance(component, WorkspaceRow) in existing components => skip width."""
        inner = self._make_component()
        row = WorkspaceRow(components=[inner])
        col = WorkspaceColumn(components=[row])
        # Rows don't contribute to width_sum
        col.components = [self._make_component(width=6)]
        assert len(col.components) == 1


# --------------------------------------------------------------------------- #
#  WorkspaceRow
# --------------------------------------------------------------------------- #

class TestWorkspaceRow:
    def _make_component(self, width=None, height=200):
        return PlotComponent(height, id_='test-id', width=width)

    def test_components_setter_exceeds_12(self):
        comps = [self._make_component() for _ in range(13)]
        with pytest.raises(MqValueError):
            WorkspaceRow(components=comps)

    def test_components_setter_total_width_exceeds_12(self):
        comps = [self._make_component(width=7), self._make_component(width=6)]
        with pytest.raises(MqValueError):
            WorkspaceRow(components=comps)

    def test_get_layout_equal_spread(self):
        c1 = self._make_component()
        c2 = self._make_component()
        row = WorkspaceRow(components=[c1, c2])
        layout, count = row.get_layout(0)
        assert layout == 'r(c6($0)c6($1))'
        assert count == 2

    def test_get_layout_equal_spread_with_column(self):
        """Branch: isinstance(component, WorkspaceColumn) in equal-spread path."""
        c1 = self._make_component()
        inner_col = WorkspaceColumn(components=[c1])
        row = WorkspaceRow(components=[inner_col])
        layout, count = row.get_layout(0)
        assert 'r(' in layout
        assert count == 1

    def test_get_layout_equal_spread_last_with_remainder(self):
        """Branch: last_size != 0 in equal-spread path."""
        comps = [self._make_component() for _ in range(5)]
        row = WorkspaceRow(components=comps)
        layout, count = row.get_layout(0)
        assert count == 5

    def test_get_layout_widths_sum_12(self):
        """Branch: width_sum == 12 => default_width = 0."""
        c1 = self._make_component(width=6)
        c2 = self._make_component(width=6)
        row = WorkspaceRow(components=[c1, c2])
        layout, count = row.get_layout(0)
        assert 'c6($0)c6($1)' in layout

    def test_get_layout_single_component_with_width(self):
        """Branch: len(self.components) == 1 => default_width = self.components[0].width."""
        c1 = self._make_component(width=8)
        row = WorkspaceRow(components=[c1])
        layout, count = row.get_layout(0)
        assert 'c8($0)' in layout

    def test_get_layout_last_no_width_column(self):
        """Branch: last component without width, isinstance WorkspaceColumn."""
        c1 = self._make_component(width=4)
        inner_comp = self._make_component()
        inner_col = WorkspaceColumn(components=[inner_comp])
        row = WorkspaceRow(components=[c1, inner_col])
        layout, count = row.get_layout(0)
        assert 'c8(' in layout

    def test_get_layout_last_no_width_component(self):
        """Branch: last component without width, regular component."""
        c1 = self._make_component(width=4)
        c2 = self._make_component()
        row = WorkspaceRow(components=[c1, c2])
        layout, count = row.get_layout(0)
        assert 'c8($1)' in layout

    def test_get_layout_middle_none_width_column(self):
        """Branch: component.width is None (not last), isinstance WorkspaceColumn."""
        inner_comp = self._make_component()
        inner_col = WorkspaceColumn(components=[inner_comp])
        c1 = self._make_component(width=4)
        c2 = self._make_component(width=4)
        row = WorkspaceRow(components=[inner_col, c1, c2])
        layout, count = row.get_layout(0)
        assert 'r(' in layout

    def test_get_layout_middle_none_width_component(self):
        """Branch: component.width is None, not last, regular component."""
        c0 = self._make_component(width=4)
        c1 = self._make_component()  # width None
        c2 = self._make_component()  # width None, last
        row = WorkspaceRow(components=[c0, c1, c2])
        layout, count = row.get_layout(0)
        assert count == 3

    def test_get_layout_with_width_column(self):
        """Branch: component has width, isinstance WorkspaceColumn."""
        inner_comp = self._make_component()
        inner_col = WorkspaceColumn(components=[inner_comp], width=6)
        c2 = self._make_component(width=6)
        row = WorkspaceRow(components=[inner_col, c2])
        layout, count = row.get_layout(0)
        assert 'c6(' in layout

    def test_get_layout_with_width_component(self):
        """Branch: component has width, regular component."""
        c1 = self._make_component(width=8)
        c2 = self._make_component(width=4)
        row = WorkspaceRow(components=[c1, c2])
        layout, count = row.get_layout(0)
        assert 'c8($0)c4($1)' in layout

    def test_add_components(self):
        c1 = self._make_component()
        row = WorkspaceRow(components=[c1])
        components = []
        row._add_components(components)
        assert len(components) == 1

    def test_add_components_nested(self):
        c1 = self._make_component()
        inner_row = WorkspaceRow(components=[c1])
        row = WorkspaceRow(components=[inner_row])
        components = []
        row._add_components(components)
        assert len(components) == 1

    def test_components_setter_existing_components_have_width(self):
        """Branch: existing __components is non-empty => width_sum accumulates from existing."""
        c1 = self._make_component(width=3)
        row = WorkspaceRow(components=[c1])
        c2 = self._make_component(width=3)
        row.components = [c2]
        assert len(row.components) == 1

    def test_components_setter_existing_exceeds_width_12(self):
        """Branch: existing width_sum > 12 => raises error."""
        c1 = self._make_component(width=6)
        c2 = self._make_component(width=6)
        row = WorkspaceRow(components=[c1, c2])
        with pytest.raises(MqValueError):
            row.components = [self._make_component(width=1)]

    def test_components_setter_with_workspace_row_in_existing(self):
        """Branch: isinstance(component, WorkspaceRow) in existing => skip width."""
        inner = self._make_component()
        inner_row = WorkspaceRow(components=[inner])
        row = WorkspaceRow(components=[inner_row])
        row.components = [self._make_component(width=6)]
        assert len(row.components) == 1

    def test_get_layout_equal_spread_last_col_with_remainder(self):
        """Branch: last component is WorkspaceColumn with remainder."""
        c1 = self._make_component()
        inner_col = WorkspaceColumn(components=[c1])
        c2 = self._make_component()
        c3 = self._make_component()
        c4 = self._make_component()
        c5 = self._make_component()
        row = WorkspaceRow(components=[c2, c3, c4, c5, inner_col])
        layout, count = row.get_layout(0)
        assert 'r(' in layout
        assert count == 5

    def test_get_layout_with_nested_row_for_width_sum(self):
        """Branch 236->235: WorkspaceRow as component skips width_sum (isinstance check false)."""
        c1 = self._make_component()
        inner_row = WorkspaceRow(components=[c1])
        c2 = self._make_component()
        row = WorkspaceRow(components=[inner_row, c2])
        layout, count = row.get_layout(0)
        assert 'r(' in layout
        # inner_row doesn't contribute to width_sum (line 236-237 skipped for it)


# --------------------------------------------------------------------------- #
#  Workspace
# --------------------------------------------------------------------------- #

class TestWorkspace:
    def _make_component(self, width=None, height=200, id_='test-id'):
        return PlotComponent(height, id_=id_, width=width)

    def _simple_workspace(self):
        c1 = self._make_component()
        row = WorkspaceRow(components=[c1])
        return Workspace(name='Test', rows=[row], alias='test-alias')

    # --- Properties ---

    def test_properties(self):
        ws = Workspace(
            name='N',
            alias='A',
            description='D',
            disclaimer='DIS',
            maintainers=['m1'],
            tags=['t1'],
        )
        assert ws.name == 'N'
        assert ws.alias == 'A'
        assert ws.description == 'D'
        assert ws.disclaimer == 'DIS'
        assert ws.maintainers == ['m1']
        assert ws.tags == ['t1']
        assert ws.id is None

        ws.name = 'N2'
        ws.alias = 'A2'
        ws.description = 'D2'
        ws.disclaimer = 'DIS2'
        ws.maintainers = ['m2']
        ws.tags = ['t2']
        assert ws.name == 'N2'
        assert ws.alias == 'A2'

    def test_rows_property(self):
        ws = Workspace(name='Test')
        assert ws.rows == []
        c = self._make_component()
        new_rows = [WorkspaceRow(components=[c])]
        ws.rows = new_rows
        assert ws.rows is new_rows

    def test_entitlements_property(self):
        ws = Workspace(name='Test')
        assert ws.entitlements is None
        ent = MagicMock()
        ws.entitlements = ent
        assert ws.entitlements is ent

    def test_tabs_property(self):
        ws = Workspace(name='Test')
        assert ws.tabs == []
        tabs = [WorkspaceTab(id_='a', name='b')]
        ws.tabs = tabs
        assert ws.tabs is tabs

    def test_selector_components_property(self):
        ws = Workspace(name='Test')
        assert ws.selector_components == []
        ws.selector_components = [self._make_component()]
        assert len(ws.selector_components) == 1

    def test_call_to_action_property(self):
        ws = Workspace(name='Test')
        assert ws.call_to_action is None
        cta = WorkspaceCallToAction(actions=[], text='hi')
        ws.call_to_action = cta
        assert ws.call_to_action is cta

    # --- get_by_id ---

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_get_by_id(self, mock_gs):
        mock_session = MagicMock()
        mock_gs.current = mock_session
        # Construct a valid workspace dict
        ws_dict = {
            'name': 'W1',
            'parameters': {
                'layout': 'r(c12($0))',
                'components': [
                    {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
                ],
            },
        }
        mock_session.sync.get.return_value = ws_dict
        ws = Workspace.get_by_id('ws-123')
        assert ws.name == 'W1'
        mock_session.sync.get.assert_called_once_with('/workspaces/markets/ws-123')

    # --- get_by_alias ---

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_get_by_alias_found(self, mock_gs):
        mock_session = MagicMock()
        mock_gs.current = mock_session
        ws_dict = {
            'name': 'W1',
            'parameters': {
                'layout': 'r(c12($0))',
                'components': [
                    {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
                ],
            },
        }
        mock_session.sync.get.return_value = {'results': [ws_dict]}
        ws = Workspace.get_by_alias('my-alias')
        assert ws.name == 'W1'

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_get_by_alias_not_found(self, mock_gs):
        mock_session = MagicMock()
        mock_gs.current = mock_session
        mock_session.sync.get.return_value = {'results': []}
        with pytest.raises(MqValueError):
            Workspace.get_by_alias('missing')

    # --- save ---

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_save_no_id_no_alias(self, mock_gs):
        """Branch: self.__id is None AND self.__alias is None -> no action."""
        mock_session = MagicMock()
        mock_gs.current = mock_session
        ws = Workspace(name='Test')
        ws.save()
        # Neither put nor post should be called
        mock_session.sync.put.assert_not_called()
        mock_session.sync.post.assert_not_called()

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_save_with_id(self, mock_gs):
        mock_session = MagicMock()
        mock_gs.current = mock_session
        ws = self._simple_workspace()
        ws._Workspace__id = 'existing-id'
        ws.save()
        mock_session.sync.put.assert_called_once()

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_save_with_alias_existing(self, mock_gs):
        """Branch: alias exists, id found via lookup."""
        mock_session = MagicMock()
        mock_gs.current = mock_session
        mock_session.sync.get.return_value = {'results': [{'id': 'found-id'}]}
        mock_session.sync.put.return_value = {'id': 'found-id'}
        ws = self._simple_workspace()
        ws.save()
        mock_session.sync.put.assert_called_once()

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_save_with_alias_new(self, mock_gs):
        """Branch: alias exists, but id NOT found -> post."""
        mock_session = MagicMock()
        mock_gs.current = mock_session
        mock_session.sync.get.return_value = {'results': []}
        mock_session.sync.post.return_value = {'id': 'new-id'}
        ws = self._simple_workspace()
        ws.save()
        mock_session.sync.post.assert_called_once()

    # --- open ---

    @patch('gs_quant.analytics.workspaces.workspace.webbrowser')
    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_open_no_id(self, mock_gs, mock_wb):
        ws = Workspace(name='Test')
        with pytest.raises(MqValueError):
            ws.open()

    @patch('gs_quant.analytics.workspaces.workspace.webbrowser')
    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_open_with_api_gs_domain(self, mock_gs, mock_wb):
        """Branch: domain == 'https://api.gs.com'."""
        mock_session = MagicMock()
        mock_session.domain = 'https://api.gs.com'
        mock_gs.current = mock_session
        ws = Workspace(name='Test', alias='my-alias')
        ws._Workspace__id = 'ws-id'
        ws.open()
        mock_wb.open.assert_called_once_with('https://marquee.gs.com/s/markets/my-alias')

    @patch('gs_quant.analytics.workspaces.workspace.webbrowser')
    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_open_with_other_domain(self, mock_gs, mock_wb):
        """Branch: domain != 'https://api.gs.com' after .web replace."""
        mock_session = MagicMock()
        mock_session.domain = 'https://other.marquee.gs.com'
        mock_gs.current = mock_session
        ws = Workspace(name='Test')
        ws._Workspace__id = 'ws-id'
        ws.open()
        mock_wb.open.assert_called_once_with('https://other.marquee.gs.com/s/markets/ws-id')

    @patch('gs_quant.analytics.workspaces.workspace.webbrowser')
    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_open_with_alias(self, mock_gs, mock_wb):
        """Branch: self.__alias or self.__id uses alias."""
        mock_session = MagicMock()
        mock_session.domain = 'https://other.gs.com'
        mock_gs.current = mock_session
        ws = Workspace(name='Test', alias='my-ws')
        ws._Workspace__id = 'some-id'
        ws.open()
        mock_wb.open.assert_called_once_with('https://other.gs.com/s/markets/my-ws')

    # --- create ---

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_create(self, mock_gs):
        mock_session = MagicMock()
        mock_gs.current = mock_session
        mock_session.sync.post.return_value = {'id': 'new-id'}
        ws = self._simple_workspace()
        ws.create()
        assert ws.id == 'new-id'

    # --- delete ---

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_delete_no_id(self, mock_gs):
        ws = Workspace(name='Test')
        with pytest.raises(MqValueError):
            ws.delete()

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_delete_with_id(self, mock_gs):
        mock_session = MagicMock()
        mock_gs.current = mock_session
        mock_session.sync.delete.return_value = {'id': 'del-id'}
        ws = Workspace(name='Test')
        ws._Workspace__id = 'del-id'
        ws.delete()
        mock_session.sync.delete.assert_called_once()

    # --- delete_all ---

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_delete_all_with_persisted_components(self, mock_gs):
        """Branch: type_ in PERSISTED_COMPONENTS, delete succeeds."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        dg = DataGridComponent(200, id_='dg-1')
        mon = MonitorComponent(200, id_='mon-1')
        plot = PlotComponent(200, id_='plot-1')
        screener = DataScreenerComponent(200, id_='scr-1')

        row = WorkspaceRow(components=[dg, mon, plot, screener])
        ws = Workspace(name='Test', rows=[row])
        ws.delete_all()
        assert mock_session.sync.delete.call_count == 4

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_delete_all_with_non_persisted_component(self, mock_gs):
        """Branch: type_ NOT in PERSISTED_COMPONENTS => no delete call."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        # PlotComponent is technically persisted, but let's verify behavior
        # with a component whose type IS in PERSISTED_COMPONENTS
        c = self._make_component()  # PlotComponent IS persisted
        row = WorkspaceRow(components=[c])
        ws = Workspace(name='Test', rows=[row])
        ws.delete_all()
        assert mock_session.sync.delete.call_count == 1

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_delete_all_handles_request_error(self, mock_gs):
        """Branch: MqRequestError caught during delete."""
        mock_session = MagicMock()
        mock_gs.current = mock_session
        mock_session.sync.delete.side_effect = MqRequestError(404, 'not found')

        dg = DataGridComponent(200, id_='dg-1')
        row = WorkspaceRow(components=[dg])
        ws = Workspace(name='Test', rows=[row])
        # Should not raise
        ws.delete_all()

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_delete_all_with_tabs(self, mock_gs):
        """Branch: include_tabs=True."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        tab = WorkspaceTab(id_='tab-alias', name='Tab')
        ws = Workspace(name='Test', rows=[], tabs=[tab])

        # get_by_alias call
        tab_ws_dict = {
            'name': 'TabWS',
            'parameters': {'layout': '', 'components': []},
        }
        mock_session.sync.get.return_value = {'results': [tab_ws_dict]}

        ws.delete_all(include_tabs=True)
        mock_session.sync.get.assert_called_once()

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_delete_all_selector_components(self, mock_gs):
        """Branch: delete_all iterates selector_components."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        dg = DataGridComponent(200, id_='sel-dg')
        ws = Workspace(name='Test', rows=[], selector_components=[dg])
        ws.delete_all()
        mock_session.sync.delete.assert_called_once()

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_delete_all_nested_row_column(self, mock_gs):
        """Branch: nested WorkspaceRow/Column recursion in __delete_components."""
        mock_session = MagicMock()
        mock_gs.current = mock_session

        dg = DataGridComponent(200, id_='nested-dg')
        inner_col = WorkspaceColumn(components=[dg])
        row = WorkspaceRow(components=[inner_col])
        ws = Workspace(name='Test', rows=[row])
        ws.delete_all()
        mock_session.sync.delete.assert_called_once()

    # --- as_dict ---

    def test_as_dict_minimal(self):
        ws = Workspace(name='Test')
        d = ws.as_dict()
        assert d['name'] == 'Test'
        assert 'alias' not in d
        assert 'entitlements' not in d
        assert 'description' not in d
        assert 'tags' not in d

    def test_as_dict_with_alias(self):
        ws = Workspace(name='Test', alias='my-alias')
        d = ws.as_dict()
        assert d['alias'] == 'my-alias'

    def test_as_dict_with_entitlements_common(self):
        """Branch: isinstance(self.__entitlements, Entitlements_)."""
        ent = Entitlements_()
        ws = Workspace(name='Test', entitlements=ent)
        d = ws.as_dict()
        assert 'entitlements' in d

    def test_as_dict_with_entitlements_entity(self):
        """Branch: isinstance(self.__entitlements, Entitlements)."""
        ent = Entitlements.from_dict({})
        ws = Workspace(name='Test', entitlements=ent)
        d = ws.as_dict()
        assert 'entitlements' in d

    def test_as_dict_with_entitlements_raw_dict(self):
        """Branch: else (entitlements is neither Entitlements_ nor Entitlements)."""
        ws = Workspace(name='Test', entitlements={'view': ['user1']})
        d = ws.as_dict()
        assert d['entitlements'] == {'view': ['user1']}

    def test_as_dict_with_description(self):
        ws = Workspace(name='Test', description='Desc')
        d = ws.as_dict()
        assert d['description'] == 'Desc'

    def test_as_dict_with_tags(self):
        ws = Workspace(name='Test', tags=['tag1'])
        d = ws.as_dict()
        assert d['tags'] == ['tag1']

    def test_as_dict_with_maintainers(self):
        ws = Workspace(name='Test', maintainers=['user1'])
        d = ws.as_dict()
        assert d['parameters']['maintainers'] == ['user1']

    def test_as_dict_with_disclaimer(self):
        ws = Workspace(name='Test', disclaimer='Disclaimer text')
        d = ws.as_dict()
        assert d['parameters']['disclaimer'] == 'Disclaimer text'

    def test_as_dict_with_call_to_action_object(self):
        """Branch: isinstance(self.__call_to_action, WorkspaceCallToAction)."""
        cta = WorkspaceCallToAction(actions=[], text='CTA text')
        ws = Workspace(name='Test', call_to_action=cta)
        d = ws.as_dict()
        assert d['parameters']['callToAction']['text'] == 'CTA text'

    def test_as_dict_with_call_to_action_dict(self):
        """Branch: call_to_action is not a WorkspaceCallToAction."""
        cta = {'text': 'CTA text', 'actions': []}
        ws = Workspace(name='Test', call_to_action=cta)
        d = ws.as_dict()
        assert d['parameters']['callToAction'] == cta

    def test_as_dict_with_tabs(self):
        tab = WorkspaceTab(id_='t', name='T')
        ws = Workspace(name='Test', tabs=[tab])
        d = ws.as_dict()
        assert d['parameters']['tabs'] == [{'id': 't', 'name': 'T'}]

    def test_as_dict_with_rows_containing_row_column(self):
        """Branch: isinstance(component, (WorkspaceRow, WorkspaceColumn))."""
        c1 = self._make_component(id_='c1')
        inner_col = WorkspaceColumn(components=[c1])
        row = WorkspaceRow(components=[inner_col])
        ws = Workspace(name='Test', rows=[row])
        d = ws.as_dict()
        assert len(d['parameters']['components']) == 1

    def test_as_dict_with_selector_components(self):
        c = self._make_component()
        ws = Workspace(name='Test', selector_components=[c])
        d = ws.as_dict()
        assert len(d['parameters']['components']) >= 1

    # --- from_dict ---

    def test_from_dict_with_tabs(self):
        ws_dict = {
            'name': 'W1',
            'parameters': {
                'layout': 'r(c12($0))',
                'components': [
                    {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
                ],
                'tabs': [{'id': 'tab1', 'name': 'Tab1'}],
                'disclaimer': 'Some disclaimer',
                'maintainers': ['user1'],
            },
            'alias': 'w1-alias',
            'description': 'Description',
            'entitlements': {},
        }
        ws = Workspace.from_dict(ws_dict)
        assert ws.name == 'W1'
        assert ws.alias == 'w1-alias'
        assert ws.description == 'Description'
        assert ws.disclaimer == 'Some disclaimer'
        assert ws.maintainers == ['user1']
        assert len(ws.tabs) == 1

    def test_from_dict_no_tabs(self):
        ws_dict = {
            'name': 'W1',
            'parameters': {
                'layout': 'r(c12($0))',
                'components': [
                    {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
                ],
            },
        }
        ws = Workspace.from_dict(ws_dict)
        assert ws.tabs == []

    def test_from_dict_extra_selector_components(self):
        """Branch: component_count < len(workspace_components) -> selector_components populated."""
        ws_dict = {
            'name': 'W1',
            'parameters': {
                'layout': '',
                'components': [
                    {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
                    {'id': 'c2', 'type': 'monitor', 'parameters': {'height': 100}},
                ],
            },
        }
        ws = Workspace.from_dict(ws_dict)
        # With empty layout, no rows, but all components become selector_components
        assert len(ws.selector_components) == 2

    # --- _parse ---

    def test_parse_component_case(self):
        components = [
            {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
        ]
        result = Workspace._parse('c12($0)', components)
        assert len(result) == 1
        assert isinstance(result[0], PlotComponent)

    def test_parse_column_case(self):
        components = [
            {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
        ]
        result = Workspace._parse('c6(c12($0))', components)
        assert len(result) == 1
        assert isinstance(result[0], WorkspaceColumn)

    def test_parse_row_case(self):
        components = [
            {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
        ]
        result = Workspace._parse('r(c12($0))', components)
        assert len(result) == 1
        assert isinstance(result[0], WorkspaceRow)

    def test_parse_multiple_components(self):
        """Ensures _parse handles multiple components at top level."""
        components = [
            {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
            {'id': 'c2', 'type': 'monitor', 'parameters': {'height': 100}},
        ]
        result = Workspace._parse('c6($0)c6($1)', components)
        assert len(result) == 2

    def test_parse_row_inside_column(self):
        """Branch: row case inside a column parse (514->519 branch)."""
        components = [
            {'id': 'c1', 'type': 'plot', 'parameters': {'height': 200}},
        ]
        # This is a column containing a row
        result = Workspace._parse('c12(r(c12($0)))', components)
        assert len(result) == 1
        assert isinstance(result[0], WorkspaceColumn)

    @patch('gs_quant.analytics.workspaces.workspace.GsSession')
    def test_delete_all_non_persisted_type(self, mock_gs):
        """Branch: type_ NOT in PERSISTED_COMPONENTS -> no delete call.
        Use a component type that's not in PERSISTED_COMPONENTS (e.g., ArticleComponent, SeparatorComponent).
        """
        from gs_quant.analytics.workspaces.components import SeparatorComponent
        mock_session = MagicMock()
        mock_gs.current = mock_session

        sep = SeparatorComponent(200, id_='sep-1', name='Sep')
        row = WorkspaceRow(components=[sep])
        ws = Workspace(name='Test', rows=[row])
        ws.delete_all()
        # SeparatorComponent is NOT in PERSISTED_COMPONENTS, so delete should not be called
        mock_session.sync.delete.assert_not_called()


# --------------------------------------------------------------------------- #
#  Layout creation tests (from original file)
# --------------------------------------------------------------------------- #

def test_layout_creation():
    plot_component_1 = PlotComponent(200, id_='CHCHF6NW1KXKFDAG')
    plot_component_2 = PlotComponent(200, id_='CHCHF6NW1KXKFDAG')
    plot_component_3 = PlotComponent(550, id_='CHCHF6NW1KXKFDAG')
    plot_component_4 = PlotComponent(550, id_='CHCHF6NW1KXKFDAG', width=8)

    # Case 1: Simple layout
    rows = [WorkspaceRow(components=[plot_component_1, plot_component_2])]
    workspace = Workspace(rows=rows, alias='testing-something', name='Testing')
    workspace = workspace.as_dict()
    assert workspace['parameters']['layout'] == 'r(c6($0)c6($1))'

    # Case 2: Extra columns equal spaced layout
    rows = [
        WorkspaceRow(
            components=[
                WorkspaceColumn(
                    components=[
                        WorkspaceRow(components=[plot_component_1]),
                        WorkspaceRow(components=[plot_component_2]),
                    ]
                ),
                plot_component_3,
            ]
        )
    ]
    workspace = Workspace(rows=rows, alias='testing-something', name='Testing')
    workspace = workspace.as_dict()
    assert workspace['parameters']['layout'] == 'r(c6(r(c12($0))r(c12($1)))c6($2))'

    # Case 3: Simple non-equal layout
    rows = [WorkspaceRow(components=[plot_component_4, plot_component_2])]
    workspace = Workspace(rows=rows, alias='testing-something', name='Testing')
    workspace = workspace.as_dict()
    assert workspace['parameters']['layout'] == 'r(c8($0)c4($1))'

    # Case 4: Non-equal spacing
    rows = [
        WorkspaceRow(
            components=[
                WorkspaceColumn(
                    width=8,
                    components=[
                        WorkspaceRow(components=[plot_component_1]),
                        WorkspaceRow(components=[plot_component_2]),
                    ],
                ),
                plot_component_3,
            ]
        )
    ]
    workspace = Workspace(rows=rows, alias='testing-something', name='Testing')
    workspace = workspace.as_dict()
    assert workspace['parameters']['layout'] == 'r(c8(r(c12($0))r(c12($1)))c4($2))'


def test_layout_parsing():
    plot_component_1 = PlotComponent(1, id_='CHCHF6NW1KXKFDAG')
    plot_component_2 = PlotComponent(2, id_='CHCHF6NW1KXKFDAG')
    plot_component_3 = PlotComponent(3, id_='CHCHF6NW1KXKFDAG')

    # Case 1: Single Component
    rows = [WorkspaceRow(components=[plot_component_1])]
    workspace = Workspace(rows=rows, alias='testing-something', name='Testing')
    workspace = Workspace.from_dict(workspace.as_dict())

    assert isinstance(workspace.rows[0], WorkspaceRow)
    assert isinstance(workspace.rows[0].components[0], PlotComponent)
    assert workspace.rows[0].components[0].width == 12
    assert workspace.rows[0].components[0].height == 1

    # Case 2: 2 Components
    rows = [WorkspaceRow(components=[plot_component_1, plot_component_2])]
    workspace = Workspace(rows=rows, alias='testing-something', name='Testing')
    workspace = Workspace.from_dict(workspace.as_dict())

    assert isinstance(workspace.rows[0], WorkspaceRow)
    assert isinstance(workspace.rows[0].components[0], PlotComponent)
    assert workspace.rows[0].components[0].width == 6
    assert workspace.rows[0].components[0].height == 1
    assert isinstance(workspace.rows[0], WorkspaceRow)
    assert isinstance(workspace.rows[0].components[1], PlotComponent)
    assert workspace.rows[0].components[1].width == 6
    assert workspace.rows[0].components[1].height == 2

    # Case 3: Nested Columns
    rows = [
        WorkspaceRow(
            components=[
                WorkspaceColumn(
                    components=[
                        WorkspaceRow(components=[plot_component_1]),
                        WorkspaceRow(components=[plot_component_2]),
                    ]
                ),
                plot_component_3,
            ]
        )
    ]
    workspace = Workspace(rows=rows, alias='testing-something', name='Testing')
    workspace = Workspace.from_dict(workspace.as_dict())

    assert isinstance(workspace.rows[0], WorkspaceRow)
    assert isinstance(workspace.rows[0].components[0], WorkspaceColumn)
    assert workspace.rows[0].components[0].width == 6
    assert len(workspace.rows[0].components[0].components) == 2
    assert workspace.rows[0].components[0].components[0].components[0].height == 1
    assert workspace.rows[0].components[0].components[1].components[0].height == 2
    assert workspace.rows[0].components[1].height == 3


if __name__ == '__main__':
    pytest.main(args=["test_workspace.py"])
