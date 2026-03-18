"""
Tests for gs_quant.analytics.workspaces.components
"""

from unittest.mock import MagicMock

import pytest

from gs_quant.analytics.workspaces.components import (
    ArticleComponent,
    CommentaryComponent,
    Component,
    ContainerComponent,
    DataGridComponent,
    DataScreenerComponent,
    LegendComponent,
    LegendItem,
    MonitorComponent,
    PlotComponent,
    PromoComponent,
    PromoSize,
    RelatedLink,
    RelatedLinkType,
    RelatedLinksComponent,
    Selection,
    SelectorComponent,
    SeparatorComponent,
    TYPE_TO_COMPONENT,
)


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

class TestSelection:
    def test_as_dict(self):
        s = Selection('sel1', 'tag1')
        assert s.as_dict() == {'selectorId': 'sel1', 'tag': 'tag1'}

    def test_from_dict(self):
        s = Selection.from_dict({'selectorId': 'sel2', 'tag': 'tag2'})
        assert s.selector_id == 'sel2'
        assert s.tag == 'tag2'

    def test_properties(self):
        s = Selection('a', 'b')
        s.selector_id = 'c'
        s.tag = 'd'
        assert s.selector_id == 'c'
        assert s.tag == 'd'


# ---------------------------------------------------------------------------
# LegendItem
# ---------------------------------------------------------------------------

class TestLegendItem:
    def test_as_dict_without_tooltip(self):
        li = LegendItem(color='red', icon='circle', name='item1')
        d = li.as_dict()
        assert d == {'color': 'red', 'icon': 'circle', 'name': 'item1'}
        assert 'tooltip' not in d

    def test_as_dict_with_tooltip(self):
        li = LegendItem(color='blue', icon='square', name='item2', tooltip='tip')
        d = li.as_dict()
        assert d['tooltip'] == 'tip'

    def test_from_dict(self):
        li = LegendItem.from_dict({'color': 'green', 'icon': 'x', 'name': 'n', 'tooltip': 't'})
        assert li.tooltip == 't'


# ---------------------------------------------------------------------------
# RelatedLink
# ---------------------------------------------------------------------------

class TestRelatedLink:
    def test_as_dict_without_description(self):
        rl = RelatedLink(type_=RelatedLinkType.internal, name='link1', link='/page')
        d = rl.as_dict()
        assert d == {'type': 'internal', 'name': 'link1', 'link': '/page'}
        assert 'description' not in d

    def test_as_dict_with_description(self):
        rl = RelatedLink(type_=RelatedLinkType.external, name='ext', link='http://x', description='desc')
        assert rl.as_dict()['description'] == 'desc'

    def test_from_dict(self):
        rl = RelatedLink.from_dict({'type': 'mail', 'name': 'mail', 'link': 'mailto:', 'description': 'd'})
        assert rl.type_ == RelatedLinkType.mail
        assert rl.description == 'd'


# ---------------------------------------------------------------------------
# PlotComponent
# ---------------------------------------------------------------------------

class TestPlotComponent:
    def test_as_dict_basic(self):
        pc = PlotComponent(height=400, id_='plot1')
        d = pc.as_dict()
        assert d['id'] == 'plot1'
        assert d['type'] == 'plot'
        assert d['parameters']['height'] == 400
        assert d['parameters']['hideLegend'] is False

    def test_as_dict_with_tooltip_and_hide_legend(self):
        pc = PlotComponent(height=300, id_='plot2', tooltip='info', hide_legend=True)
        d = pc.as_dict()
        assert d['parameters']['tooltip'] == 'info'
        assert d['parameters']['hideLegend'] is True


# ---------------------------------------------------------------------------
# DataGridComponent
# ---------------------------------------------------------------------------

class TestDataGridComponent:
    def test_as_dict_without_tooltip(self):
        dg = DataGridComponent(height=300, id_='dg1')
        d = dg.as_dict()
        assert d['type'] == 'datagrid'
        assert 'tooltip' not in d['parameters']

    def test_as_dict_with_tooltip(self):
        dg = DataGridComponent(height=300, id_='dg2', tooltip='help')
        d = dg.as_dict()
        assert d['parameters']['tooltip'] == 'help'


# ---------------------------------------------------------------------------
# DataScreenerComponent
# ---------------------------------------------------------------------------

class TestDataScreenerComponent:
    def test_as_dict_without_tooltip(self):
        ds = DataScreenerComponent(height=300, id_='ds1')
        d = ds.as_dict()
        assert d['type'] == 'screener'
        assert 'tooltip' not in d['parameters']

    def test_as_dict_with_tooltip(self):
        ds = DataScreenerComponent(height=300, id_='ds2', tooltip='screen')
        assert ds.as_dict()['parameters']['tooltip'] == 'screen'


# ---------------------------------------------------------------------------
# MonitorComponent
# ---------------------------------------------------------------------------

class TestMonitorComponent:
    def test_as_dict_without_tooltip(self):
        mc = MonitorComponent(height=200, id_='mon1')
        d = mc.as_dict()
        assert d['type'] == 'monitor'
        assert 'tooltip' not in d['parameters']

    def test_as_dict_with_tooltip(self):
        mc = MonitorComponent(height=200, id_='mon2', tooltip='monitor tip')
        assert mc.as_dict()['parameters']['tooltip'] == 'monitor tip'


# ---------------------------------------------------------------------------
# ArticleComponent
# ---------------------------------------------------------------------------

class TestArticleComponent:
    def test_as_dict_basic(self):
        ac = ArticleComponent(height=300, id_='art1')
        d = ac.as_dict()
        assert d['type'] == 'article'
        assert 'commentaryChannels' not in d['parameters']

    def test_as_dict_with_commentary_channels(self):
        ac = ArticleComponent(height=300, id_='art2', commentary_channels=['ch1', 'ch2'],
                              commentary_to_desktop_link=True, tooltip='tip')
        d = ac.as_dict()
        assert d['parameters']['commentaryChannels'] == ['ch1', 'ch2']
        assert d['parameters']['commentaryToDesktopLink'] is True
        assert d['parameters']['tooltip'] == 'tip'


# ---------------------------------------------------------------------------
# CommentaryComponent
# ---------------------------------------------------------------------------

class TestCommentaryComponent:
    def test_as_dict_with_commentary_channels(self):
        cc = CommentaryComponent(height=300, id_='com1', commentary_channels=['ch1'],
                                 commentary_to_desktop_link=True)
        d = cc.as_dict()
        assert d['parameters']['commentaryChannels'] == ['ch1']
        assert d['parameters']['commentaryToDesktopLink'] is True


# ---------------------------------------------------------------------------
# ContainerComponent
# ---------------------------------------------------------------------------

class TestContainerComponent:
    def test_as_dict_no_height(self):
        cc = ContainerComponent(id_='cont1', component_id='cid1')
        d = cc.as_dict()
        assert d['type'] == 'container'
        assert 'height' not in d['parameters']
        assert d['parameters']['componentId'] == 'cid1'

    def test_as_dict_no_component_id(self):
        cc = ContainerComponent(id_='cont2')
        d = cc.as_dict()
        assert 'componentId' not in d['parameters']


# ---------------------------------------------------------------------------
# SelectorComponent
# ---------------------------------------------------------------------------

class TestSelectorComponent:
    def test_as_dict_all_optional_params(self):
        sc = SelectorComponent(
            height=100, id_='sel1', container_ids=['c1', 'c2'],
            title='Pick one', default_option_index=1,
            tooltip='selector tip', parent_selector_id='psel1',
        )
        d = sc.as_dict()
        assert d['type'] == 'selector'
        assert d['parameters']['containerIds'] == ['c1', 'c2']
        assert d['parameters']['title'] == 'Pick one'
        assert d['parameters']['defaultOptionIndex'] == 1
        assert d['parameters']['tooltip'] == 'selector tip'
        assert d['parameters']['parentSelectorId'] == 'psel1'

    def test_as_dict_minimal(self):
        sc = SelectorComponent(height=100, id_='sel2', container_ids=['c1'])
        d = sc.as_dict()
        assert 'title' not in d['parameters']
        assert 'defaultOptionIndex' not in d['parameters']

    def test_from_dict(self):
        obj = {
            'id': 'sel1',
            'type': 'selector',
            'parameters': {
                'height': 150,
                'containerIds': ['c1'],
                'title': 'T',
                'tooltip': 'tip',
                'defaultOptionIndex': 2,
                'parentSelectorId': 'ps1',
            },
        }
        sc = SelectorComponent.from_dict(obj)
        assert sc.id_ == 'sel1'
        assert sc.container_ids == ['c1']
        assert sc.title == 'T'
        assert sc.default_option_index == 2
        assert sc.parent_selector_id == 'ps1'


# ---------------------------------------------------------------------------
# PromoComponent
# ---------------------------------------------------------------------------

class TestPromoComponent:
    def test_as_dict_all_params(self):
        pc = PromoComponent(
            height=200, id_='promo1', tooltip='tip', transparent=True,
            body='<b>Hello</b>', size=PromoSize.LARGE, hide_border=True,
        )
        d = pc.as_dict()
        assert d['type'] == 'promo'
        assert d['parameters']['tooltip'] == 'tip'
        assert d['parameters']['body'] == '<b>Hello</b>'
        assert d['parameters']['size'] == 'large'
        assert d['parameters']['hideBorder'] is True
        assert d['parameters']['transparent'] is True

    def test_as_dict_hide_border_false(self):
        pc = PromoComponent(height=200, id_='promo2', hide_border=False)
        d = pc.as_dict()
        # hide_border is False but not None, so should be included
        assert d['parameters']['hideBorder'] is False

    def test_from_dict(self):
        obj = {
            'id': 'promo1',
            'type': 'promo',
            'parameters': {
                'height': 200,
                'tooltip': 'tip',
                'body': 'text',
                'size': 'large',
                'hideBorder': True,
            },
        }
        pc = PromoComponent.from_dict(obj)
        assert pc.id_ == 'promo1'
        assert pc.size == PromoSize.LARGE
        assert pc.hide_border is True
        assert pc.body == 'text'

    def test_from_dict_no_size(self):
        obj = {'id': 'promo2', 'type': 'promo', 'parameters': {'height': 100}}
        pc = PromoComponent.from_dict(obj)
        assert pc.size is None


# ---------------------------------------------------------------------------
# SeparatorComponent
# ---------------------------------------------------------------------------

class TestSeparatorComponent:
    def test_as_dict_with_all(self):
        sc = SeparatorComponent(height=50, id_='sep1', name='Section', size='large', show_more_url='/more')
        d = sc.as_dict()
        assert d['type'] == 'separator'
        assert d['parameters']['name'] == 'Section'
        assert d['parameters']['size'] == 'large'
        assert d['parameters']['showMoreUrl'] == '/more'

    def test_as_dict_minimal(self):
        sc = SeparatorComponent(height=50, id_='sep2')
        d = sc.as_dict()
        assert 'name' not in d['parameters']


# ---------------------------------------------------------------------------
# LegendComponent
# ---------------------------------------------------------------------------

class TestLegendComponent:
    def test_as_dict(self):
        items = [LegendItem('red', 'circle', 'A'), LegendItem('blue', 'square', 'B')]
        lc = LegendComponent(height=100, id_='leg1', items=items, position='top', transparent=True)
        d = lc.as_dict()
        assert d['type'] == 'legend'
        assert len(d['parameters']['items']) == 2
        assert d['parameters']['position'] == 'top'
        assert d['parameters']['transparent'] is True

    def test_from_dict(self):
        obj = {
            'id': 'leg1',
            'type': 'legend',
            'parameters': {
                'height': 100,
                'items': [{'color': 'red', 'icon': 'c', 'name': 'n'}],
                'position': 'bottom',
                'transparent': False,
            },
        }
        lc = LegendComponent.from_dict(obj)
        assert lc.id_ == 'leg1'
        assert len(lc.items) == 1
        assert lc.position == 'bottom'


# ---------------------------------------------------------------------------
# RelatedLinksComponent
# ---------------------------------------------------------------------------

class TestRelatedLinksComponent:
    def test_as_dict(self):
        links = [RelatedLink(RelatedLinkType.internal, 'link1', '/page')]
        rlc = RelatedLinksComponent(height=100, id_='rl1', links=links, title='Links')
        d = rlc.as_dict()
        assert d['type'] == 'relatedLinks'
        assert d['parameters']['title'] == 'Links'
        assert len(d['parameters']['links']) == 1

    def test_from_dict(self):
        obj = {
            'id': 'rl1',
            'type': 'relatedLinks',
            'parameters': {
                'height': 100,
                'title': 'T',
                'links': [{'type': 'external', 'name': 'ext', 'link': 'http://x'}],
            },
        }
        rlc = RelatedLinksComponent.from_dict(obj)
        assert rlc.title == 'T'
        assert len(rlc.links) == 1
        assert rlc.links[0].type_ == RelatedLinkType.external


# ---------------------------------------------------------------------------
# Component.from_dict (dispatch)
# ---------------------------------------------------------------------------

class TestComponentFromDict:
    @pytest.mark.parametrize('type_key,expected_cls', [
        ('plot', PlotComponent),
        ('datagrid', DataGridComponent),
        ('monitor', MonitorComponent),
        ('screener', DataScreenerComponent),
        ('article', ArticleComponent),
        ('separator', SeparatorComponent),
    ])
    def test_dispatches_to_correct_type(self, type_key, expected_cls):
        obj = {
            'id': 'comp1',
            'type': type_key,
            'parameters': {'height': 200},
        }
        comp = Component.from_dict(obj)
        assert isinstance(comp, expected_cls)

    def test_type_to_component_map_complete(self):
        expected_types = {
            'article', 'container', 'datagrid', 'dataviz', 'legend',
            'monitor', 'plot', 'promo', 'relatedLinks', 'selector',
            'separator', 'screener',
        }
        assert set(TYPE_TO_COMPONENT.keys()) == expected_types

    def test_selections_applied(self):
        obj = {
            'id': 'comp1',
            'type': 'plot',
            'parameters': {'height': 200},
            'selections': [{'selectorId': 's1', 'tag': 't1'}],
        }
        comp = Component.from_dict(obj)
        assert len(comp.selections) == 1
        assert comp.selections[0].selector_id == 's1'
