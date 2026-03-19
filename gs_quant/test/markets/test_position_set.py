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
import math
from copy import deepcopy
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from gs_quant.api.gs.assets import GsAssetApi
from gs_quant.api.gs.price import GsPriceApi
from gs_quant.common import (
    Position as CommonPosition,
    PositionPriceInput,
    PositionSet as CommonPositionSet,
    PositionTag as PositionTagTarget,
    Currency,
    PositionSetWeightingStrategy,
)
from gs_quant.errors import MqValueError, MqRequestError
from gs_quant.markets.position_set import (
    Position,
    PositionSet,
    PositionTag,
)
import gs_quant.markets.position_set as position_set_module


# ────────────────────────────────────────────────────────────
# PositionTag
# ────────────────────────────────────────────────────────────

class TestPositionTag:
    def test_from_dict_single(self):
        tag = PositionTag.from_dict({'sector': 'Tech'})
        assert tag.name == 'sector'
        assert tag.value == 'Tech'

    def test_from_dict_multiple_keys_raises(self):
        with pytest.raises(MqValueError, match='single key-value pair'):
            PositionTag.from_dict({'a': '1', 'b': '2'})


# ────────────────────────────────────────────────────────────
# Position
# ────────────────────────────────────────────────────────────

class TestPosition:
    def test_init_basic(self):
        p = Position(identifier='AAPL UW')
        assert p.identifier == 'AAPL UW'
        assert p.weight is None
        assert p.quantity is None
        assert p.notional is None
        assert p.name is None
        assert p.asset_id is None
        assert p.tags is None
        assert p.restricted is None
        assert p.hard_to_borrow is None

    def test_init_with_all_params(self):
        tags = [PositionTag(name='sector', value='Tech')]
        p = Position(
            identifier='AAPL UW',
            weight=0.5,
            quantity=100.0,
            notional=50000.0,
            name='Apple',
            asset_id='MA123',
            tags=tags,
        )
        assert p.identifier == 'AAPL UW'
        assert p.weight == 0.5
        assert p.quantity == 100.0
        assert p.notional == 50000.0
        assert p.name == 'Apple'
        assert p.asset_id == 'MA123'
        assert len(p.tags) == 1

    def test_init_tags_from_dict(self):
        p = Position(
            identifier='AAPL UW',
            tags=[{'sector': 'Tech'}],
        )
        assert len(p.tags) == 1
        assert isinstance(p.tags[0], PositionTag)
        assert p.tags[0].name == 'sector'

    def test_init_tags_mixed(self):
        p = Position(
            identifier='AAPL UW',
            tags=[PositionTag(name='a', value='1'), {'b': '2'}],
        )
        assert len(p.tags) == 2

    def test_setters(self):
        p = Position(identifier='X')
        p.identifier = 'Y'
        p.weight = 0.3
        p.quantity = 50
        p.notional = 10000
        p.name = 'Test'
        p.asset_id = 'MA999'
        p.tags = [PositionTag(name='t', value='v')]
        assert p.identifier == 'Y'
        assert p.weight == 0.3
        assert p.quantity == 50
        assert p.notional == 10000
        assert p.name == 'Test'
        assert p.asset_id == 'MA999'
        assert len(p.tags) == 1

    def test_eq_same(self):
        p1 = Position(identifier='AAPL UW', asset_id='MA1', weight=0.5)
        p2 = Position(identifier='AAPL UW', asset_id='MA1', weight=0.5)
        assert p1 == p2

    def test_eq_not_position(self):
        p = Position(identifier='AAPL UW')
        assert p != "not a position"

    def test_eq_different_weight(self):
        p1 = Position(identifier='AAPL UW', asset_id='MA1', weight=0.5)
        p2 = Position(identifier='AAPL UW', asset_id='MA1', weight=0.6)
        assert p1 != p2

    def test_eq_with_rounding(self):
        p1 = Position(identifier='AAPL UW', asset_id='MA1', weight=0.500001)
        p2 = Position(identifier='AAPL UW', asset_id='MA1', weight=0.500002)
        assert p1 == p2

    def test_eq_one_none_numeric(self):
        """When one of weight/notional/quantity is None and the other is not,
        the condition `not (slf is None or oth is None)` makes it skip"""
        p1 = Position(identifier='AAPL UW', asset_id='MA1', weight=0.5)
        p2 = Position(identifier='AAPL UW', asset_id='MA1', weight=None)
        assert p1 == p2  # Because if one is None, the numeric comparison is skipped

    def test_eq_different_tags(self):
        p1 = Position(identifier='AAPL UW', asset_id='MA1',
                       tags=[PositionTag(name='a', value='1')])
        p2 = Position(identifier='AAPL UW', asset_id='MA1',
                       tags=[PositionTag(name='b', value='2')])
        assert p1 != p2

    def test_eq_both_tags_none(self):
        p1 = Position(identifier='AAPL UW', asset_id='MA1')
        p2 = Position(identifier='AAPL UW', asset_id='MA1')
        assert p1 == p2

    def test_eq_different_asset_id(self):
        p1 = Position(identifier='AAPL UW', asset_id='MA1')
        p2 = Position(identifier='AAPL UW', asset_id='MA2')
        assert p1 != p2

    def test_hash(self):
        p = Position(identifier='AAPL UW', asset_id='MA1')
        h = hash(p)
        assert isinstance(h, int)

    def test_add_tag(self):
        p = Position(identifier='AAPL UW')
        p.add_tag('sector', 'Tech')
        assert len(p.tags) == 1
        assert p.tags[0].name == 'sector'

    def test_add_tag_to_existing(self):
        p = Position(identifier='AAPL UW', tags=[PositionTag(name='a', value='1')])
        p.add_tag('b', '2')
        assert len(p.tags) == 2

    def test_add_tag_duplicate_raises(self):
        p = Position(identifier='AAPL UW', tags=[PositionTag(name='a', value='1')])
        with pytest.raises(MqValueError, match='already has tag'):
            p.add_tag('a', '2')

    def test_tags_as_dict(self):
        p = Position(identifier='AAPL UW',
                      tags=[PositionTag(name='a', value='1'), PositionTag(name='b', value='2')])
        d = p.tags_as_dict()
        assert d == {'a': '1', 'b': '2'}

    def test_as_dict_basic(self):
        p = Position(identifier='AAPL UW', weight=0.5, asset_id='MA1', name='Apple')
        d = p.as_dict()
        assert d['identifier'] == 'AAPL UW'
        assert d['weight'] == 0.5
        assert 'quantity' not in d  # None values stripped

    def test_as_dict_tags_as_keys(self):
        p = Position(identifier='AAPL UW', weight=0.5,
                      tags=[PositionTag(name='sector', value='Tech')])
        d = p.as_dict(tags_as_keys=True)
        assert 'sector' in d
        assert d['sector'] == 'Tech'

    def test_as_dict_tags_not_as_keys(self):
        p = Position(identifier='AAPL UW', weight=0.5,
                      tags=[PositionTag(name='sector', value='Tech')])
        d = p.as_dict(tags_as_keys=False)
        assert 'tags' in d

    def test_as_dict_no_tags(self):
        p = Position(identifier='AAPL UW', weight=0.5)
        d = p.as_dict(tags_as_keys=True)
        assert 'tags' not in d  # tags is None, no keys added

    def test_from_dict(self):
        d = {'identifier': 'AAPL UW', 'weight': 0.5, 'sector': 'Tech'}
        p = Position.from_dict(d)
        assert p.identifier == 'AAPL UW'
        assert p.weight == 0.5
        assert len(p.tags) == 1
        assert p.tags[0].name == 'sector'

    def test_from_dict_with_id(self):
        d = {'identifier': 'AAPL UW', 'id': 'MA1'}
        p = Position.from_dict(d)
        assert p.asset_id == 'MA1'

    def test_from_dict_both_id_and_asset_id_raises(self):
        d = {'identifier': 'AAPL UW', 'id': 'MA1', 'asset_id': 'MA2'}
        with pytest.raises(MqValueError, match='both id and asset_id'):
            Position.from_dict(d)

    def test_from_dict_no_tags(self):
        d = {'identifier': 'AAPL UW', 'weight': 0.5}
        p = Position.from_dict(d, add_tags=False)
        assert p.tags is None

    def test_clone(self):
        p = Position(identifier='AAPL UW', weight=0.5, asset_id='MA1',
                      tags=[PositionTag(name='a', value='1')])
        cloned = p.clone()
        assert cloned.identifier == 'AAPL UW'
        assert cloned.weight == 0.5
        assert cloned is not p

    def test_to_target_common(self):
        p = Position(identifier='AAPL UW', asset_id='MA1', quantity=100,
                      tags=[PositionTag(name='a', value='1')])
        target = p.to_target(common=True)
        assert isinstance(target, CommonPosition)
        assert target.asset_id == 'MA1'
        assert target.quantity == 100

    def test_to_target_common_no_tags(self):
        p = Position(identifier='AAPL UW', asset_id='MA1', quantity=100)
        target = p.to_target(common=True)
        assert target.tags is None

    def test_to_target_price_input(self):
        p = Position(identifier='AAPL UW', asset_id='MA1', quantity=100,
                      weight=0.5, notional=50000)
        target = p.to_target(common=False)
        assert isinstance(target, PositionPriceInput)

    def test_restricted_and_htb_setters(self):
        p = Position(identifier='AAPL UW')
        p._restricted = True
        p._hard_to_borrow = True
        assert p.restricted is True
        assert p.hard_to_borrow is True


# ────────────────────────────────────────────────────────────
# PositionSet - construction & validation
# ────────────────────────────────────────────────────────────

class TestPositionSetInit:
    def test_basic_init(self):
        positions = [Position(identifier='AAPL UW', weight=0.5)]
        ps = PositionSet(positions=positions, date=dt.date(2024, 1, 1))
        assert len(ps.positions) == 1
        assert ps.date == dt.date(2024, 1, 1)
        assert ps.divisor is None
        assert ps.reference_notional is None
        assert ps.unresolved_positions == []
        assert ps.unpriced_positions == []

    def test_with_reference_notional(self):
        positions = [Position(identifier='AAPL UW', weight=0.5)]
        ps = PositionSet(positions=positions, reference_notional=10000)
        assert ps.reference_notional == 10000

    def test_reference_notional_no_weight_raises(self):
        positions = [Position(identifier='AAPL UW')]
        with pytest.raises(MqValueError, match='must have weights'):
            PositionSet(positions=positions, reference_notional=10000)

    def test_reference_notional_with_notional_raises(self):
        positions = [Position(identifier='AAPL UW', weight=0.5, notional=1000)]
        with pytest.raises(MqValueError, match='cannot have positions with notional'):
            PositionSet(positions=positions, reference_notional=10000)

    def test_reference_notional_with_quantity_raises(self):
        positions = [Position(identifier='AAPL UW', weight=0.5, quantity=100)]
        with pytest.raises(MqValueError, match='cannot have positions with quantities'):
            PositionSet(positions=positions, reference_notional=10000)

    def test_with_unresolved_positions(self):
        ps = PositionSet(
            positions=[],
            unresolved_positions=[Position(identifier='X')],
        )
        assert len(ps.unresolved_positions) == 1

    def test_with_unpriced_positions(self):
        ps = PositionSet(
            positions=[],
            unpriced_positions=[Position(identifier='Y')],
        )
        assert len(ps.unpriced_positions) == 1


# ────────────────────────────────────────────────────────────
# PositionSet equality
# ────────────────────────────────────────────────────────────

class TestPositionSetEquality:
    def test_equal(self):
        p1 = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1', weight=0.5)],
            date=dt.date(2024, 1, 1),
            reference_notional=1000,
        )
        p2 = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1', weight=0.5)],
            date=dt.date(2024, 1, 1),
            reference_notional=1000,
        )
        assert p1 == p2

    def test_different_length(self):
        p1 = PositionSet(positions=[Position(identifier='A', asset_id='MA1', weight=0.5)],
                          date=dt.date(2024, 1, 1))
        p2 = PositionSet(positions=[], date=dt.date(2024, 1, 1))
        assert p1 != p2

    def test_different_date(self):
        p1 = PositionSet(positions=[], date=dt.date(2024, 1, 1))
        p2 = PositionSet(positions=[], date=dt.date(2024, 2, 1))
        assert p1 != p2

    def test_different_ref_notional(self):
        p1 = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1', weight=0.5)],
            reference_notional=1000,
        )
        p2 = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1', weight=0.5)],
            reference_notional=2000,
        )
        assert p1 != p2

    def test_different_positions(self):
        p1 = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1', weight=0.5)],
        )
        p2 = PositionSet(
            positions=[Position(identifier='A', asset_id='MA2', weight=0.5)],
        )
        assert p1 != p2


# ────────────────────────────────────────────────────────────
# PositionSet property setters
# ────────────────────────────────────────────────────────────

class TestPositionSetSetters:
    def test_positions_setter(self):
        ps = PositionSet(positions=[])
        new_positions = [Position(identifier='A')]
        ps.positions = new_positions
        assert len(ps.positions) == 1

    def test_date_setter(self):
        ps = PositionSet(positions=[])
        ps.date = dt.date(2024, 6, 1)
        assert ps.date == dt.date(2024, 6, 1)

    def test_reference_notional_setter(self):
        ps = PositionSet(positions=[Position(identifier='A', weight=0.5)],
                          reference_notional=1000)
        ps.reference_notional = 2000
        assert ps.reference_notional == 2000


# ────────────────────────────────────────────────────────────
# PositionSet methods
# ────────────────────────────────────────────────────────────

class TestPositionSetMethods:
    def test_get_positions(self):
        ps = PositionSet(positions=[
            Position(identifier='AAPL UW', weight=0.5, asset_id='MA1'),
            Position(identifier='MSFT UW', weight=0.5, asset_id='MA2'),
        ])
        df = ps.get_positions()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_get_unresolved_positions(self):
        ps = PositionSet(
            positions=[],
            unresolved_positions=[Position(identifier='BAD')],
        )
        df = ps.get_unresolved_positions()
        assert len(df) == 1

    def test_remove_unresolved_positions(self):
        ps = PositionSet(
            positions=[
                Position(identifier='AAPL UW', asset_id='MA1'),
                Position(identifier='BAD', asset_id=None),
            ],
            unresolved_positions=[Position(identifier='BAD')],
        )
        ps.remove_unresolved_positions()
        assert all(p.asset_id is not None for p in ps.positions)

    def test_get_unpriced_positions(self):
        ps = PositionSet(
            positions=[],
            unpriced_positions=[Position(identifier='X')],
        )
        df = ps.get_unpriced_positions()
        assert len(df) == 1

    def test_remove_unpriced_positions(self):
        ps = PositionSet(
            positions=[],
            unpriced_positions=[Position(identifier='X')],
        )
        ps.remove_unpriced_positions()
        # After removal, accessing unpriced_positions returns None
        assert ps.unpriced_positions is None

    def test_get_restricted_positions(self):
        p1 = Position(identifier='A', asset_id='MA1')
        p1._restricted = True
        p2 = Position(identifier='B', asset_id='MA2')
        ps = PositionSet(positions=[p1, p2])
        df = ps.get_restricted_positions()
        assert len(df) == 1

    def test_remove_restricted_positions(self):
        p1 = Position(identifier='A', asset_id='MA1')
        p1._restricted = True
        p2 = Position(identifier='B', asset_id='MA2')
        ps = PositionSet(positions=[p1, p2])
        ps.remove_restricted_positions()
        assert len(ps.positions) == 1
        assert ps.positions[0].identifier == 'B'

    def test_get_hard_to_borrow_positions(self):
        p1 = Position(identifier='A', asset_id='MA1')
        p1._hard_to_borrow = True
        p2 = Position(identifier='B', asset_id='MA2')
        ps = PositionSet(positions=[p1, p2])
        df = ps.get_hard_to_borrow_positions()
        assert len(df) == 1

    def test_remove_hard_to_borrow_positions(self):
        p1 = Position(identifier='A', asset_id='MA1')
        p1._hard_to_borrow = True
        p2 = Position(identifier='B', asset_id='MA2')
        ps = PositionSet(positions=[p1, p2])
        ps.remove_hard_to_borrow_positions()
        assert len(ps.positions) == 1
        assert ps.positions[0].identifier == 'B'

    def test_equalize_position_weights(self):
        ps = PositionSet(positions=[
            Position(identifier='A', weight=0.7),
            Position(identifier='B', weight=0.3),
        ])
        ps.equalize_position_weights()
        for p in ps.positions:
            assert p.weight == 0.5
            assert p.quantity is None
            assert p.notional is None

    def test_to_frame(self):
        ps = PositionSet(
            positions=[Position(identifier='A', weight=0.5, asset_id='MA1')],
            date=dt.date(2024, 1, 1),
        )
        df = ps.to_frame()
        assert isinstance(df, pd.DataFrame)
        assert 'date' in df.columns
        assert 'identifier' in df.columns

    def test_to_frame_with_divisor(self):
        ps = PositionSet(
            positions=[Position(identifier='A', weight=0.5, asset_id='MA1')],
            date=dt.date(2024, 1, 1),
            divisor=100.0,
        )
        df = ps.to_frame()
        assert 'divisor' in df.columns

    def test_to_frame_add_tags(self):
        ps = PositionSet(
            positions=[Position(identifier='A', weight=0.5,
                                tags=[PositionTag(name='sector', value='Tech')])],
            date=dt.date(2024, 1, 1),
        )
        df = ps.to_frame(add_tags=True)
        assert 'sector' in df.columns

    def test_redistribute_weights(self):
        ps = PositionSet(positions=[
            Position(identifier='A', weight=0.3),
            Position(identifier='B', weight=0.3),
        ])
        ps.redistribute_weights()
        total = sum(p.weight for p in ps.positions)
        assert abs(total - 1.0) < 1e-10

    def test_redistribute_weights_missing_raises(self):
        ps = PositionSet(positions=[
            Position(identifier='A', weight=0.3),
            Position(identifier='B'),
        ])
        with pytest.raises(MqValueError, match='missing weights'):
            ps.redistribute_weights()

    def test_clone_basic(self):
        ps = PositionSet(
            positions=[Position(identifier='A', weight=0.5,
                                tags=[PositionTag(name='t', value='v')])],
            date=dt.date(2024, 1, 1),
        )
        cloned = ps.clone()
        assert cloned.date == ps.date
        assert len(cloned.positions) == 1

    def test_clone_keep_reference_notional(self):
        ps = PositionSet(
            positions=[Position(identifier='A', weight=0.5)],
            date=dt.date(2024, 1, 1),
            reference_notional=10000,
        )
        cloned = ps.clone(keep_reference_notional=True)
        assert cloned.reference_notional == 10000

    def test_clone_drop_reference_notional_when_has_quantity(self):
        # Simulate a state where both quantity and reference_notional exist
        # by having quantity in the frame
        p = Position(identifier='A', weight=0.5)
        ps = PositionSet(
            positions=[p],
            date=dt.date(2024, 1, 1),
            reference_notional=10000,
        )
        # When quantity is not in the frame, clone keeps reference_notional
        cloned = ps.clone(keep_reference_notional=False)
        assert cloned.reference_notional == 10000  # no quantity column so ref_notional preserved


# ────────────────────────────────────────────────────────────
# PositionSet resolve
# ────────────────────────────────────────────────────────────

class TestPositionSetResolve:
    @patch.object(GsAssetApi, 'resolve_assets')
    def test_resolve_success(self, mock_resolve):
        mock_resolve.return_value = {
            'AAPL UW': [{'id': 'MA1', 'name': 'Apple', 'tradingRestriction': False}],
            'MSFT UW': [{'id': 'MA2', 'name': 'Microsoft', 'tradingRestriction': False}],
        }
        ps = PositionSet(
            positions=[
                Position(identifier='AAPL UW', weight=0.5),
                Position(identifier='MSFT UW', weight=0.5),
            ],
            date=dt.date(2024, 1, 1),
        )
        ps.resolve()
        assert all(p.asset_id is not None for p in ps.positions)
        assert len(ps.unresolved_positions) == 0

    @patch.object(GsAssetApi, 'resolve_assets')
    def test_resolve_with_unmapped(self, mock_resolve):
        mock_resolve.return_value = {
            'AAPL UW': [{'id': 'MA1', 'name': 'Apple', 'tradingRestriction': False}],
            'BAD': None,
        }
        ps = PositionSet(
            positions=[
                Position(identifier='AAPL UW', weight=0.5),
                Position(identifier='BAD', weight=0.5),
            ],
            date=dt.date(2024, 1, 1),
        )
        ps.resolve()
        assert len(ps.positions) == 1
        assert len(ps.unresolved_positions) == 1

    @patch.object(GsAssetApi, 'resolve_assets')
    def test_resolve_empty_response(self, mock_resolve):
        mock_resolve.return_value = {
            'AAPL UW': [],
        }
        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', weight=0.5)],
            date=dt.date(2024, 1, 1),
        )
        ps.resolve()
        assert len(ps.positions) == 0
        assert len(ps.unresolved_positions) == 1

    def test_resolve_already_resolved_noop(self):
        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', weight=0.5, asset_id='MA1')],
            date=dt.date(2024, 1, 1),
        )
        ps.resolve()
        # Should not call API since all positions already have asset_id
        assert len(ps.positions) == 1

    @patch.object(GsAssetApi, 'resolve_assets')
    def test_resolve_dot_in_identifier(self, mock_resolve):
        """Test that identifiers with dots are handled correctly (pydash escaping)"""
        mock_resolve.return_value = {
            'BRK.B': [{'id': 'MA9', 'name': 'Berkshire', 'tradingRestriction': None}],
        }
        ps = PositionSet(
            positions=[Position(identifier='BRK.B', weight=1.0)],
            date=dt.date(2024, 1, 1),
        )
        ps.resolve()
        assert len(ps.positions) == 1
        assert ps.positions[0].asset_id == 'MA9'


# ────────────────────────────────────────────────────────────
# PositionSet get_subset
# ────────────────────────────────────────────────────────────

class TestPositionSetGetSubset:
    def test_get_subset_by_tag(self):
        ps = PositionSet(
            positions=[
                Position(identifier='A', weight=0.5,
                         tags=[PositionTag(name='sector', value='Tech')]),
                Position(identifier='B', weight=0.5,
                         tags=[PositionTag(name='sector', value='Finance')]),
            ],
        )
        subset = ps.get_subset(sector='Tech')
        assert len(subset.positions) == 1
        assert subset.positions[0].identifier == 'A'

    def test_get_subset_no_tags_raises(self):
        ps = PositionSet(
            positions=[Position(identifier='A', weight=0.5)],
        )
        with pytest.raises(MqValueError, match='does not have tags'):
            ps.get_subset(sector='Tech')

    def test_get_subset_no_copy(self):
        ps = PositionSet(
            positions=[
                Position(identifier='A', weight=0.5,
                         tags=[PositionTag(name='sector', value='Tech')]),
            ],
        )
        subset = ps.get_subset(copy=False, sector='Tech')
        assert len(subset.positions) == 1
        # Without copy, should be the same object
        assert subset.positions[0] is ps.positions[0]


# ────────────────────────────────────────────────────────────
# PositionSet to_target / from_target
# ────────────────────────────────────────────────────────────

class TestPositionSetTargetConversion:
    def test_to_target_common(self):
        ps = PositionSet(
            positions=[
                Position(identifier='A', asset_id='MA1', quantity=100),
            ],
            date=dt.date(2024, 1, 1),
        )
        target = ps.to_target(common=True)
        assert isinstance(target, CommonPositionSet)

    def test_to_target_price(self):
        ps = PositionSet(
            positions=[
                Position(identifier='A', asset_id='MA1', quantity=100, weight=0.5, notional=50000),
            ],
            date=dt.date(2024, 1, 1),
        )
        target = ps.to_target(common=False)
        assert isinstance(target, list)
        assert len(target) == 1

    @patch.object(GsAssetApi, 'get_many_assets_data')
    def test_from_target(self, mock_assets):
        mock_assets.return_value = [
            {'id': 'MA1', 'name': 'Apple', 'bbid': 'AAPL UW'},
        ]
        positions = (CommonPosition(asset_id='MA1', quantity=100),)
        target_ps = CommonPositionSet(positions, dt.date(2024, 1, 1))
        ps = PositionSet.from_target(target_ps)
        assert len(ps.positions) == 1
        assert ps.positions[0].identifier == 'AAPL UW'

    @patch.object(GsAssetApi, 'get_many_assets_data')
    def test_from_target_with_tags(self, mock_assets):
        mock_assets.return_value = [
            {'id': 'MA1', 'name': 'Apple', 'bbid': 'AAPL UW'},
        ]
        pos = CommonPosition(asset_id='MA1', quantity=100,
                              tags=(PositionTagTarget(name='t', value='v'),))
        target_ps = CommonPositionSet((pos,), dt.date(2024, 1, 1))
        ps = PositionSet.from_target(target_ps)
        assert ps.positions[0].tags is not None


# ────────────────────────────────────────────────────────────
# PositionSet from_list / from_dicts / from_frame
# ────────────────────────────────────────────────────────────

class TestPositionSetFromMethods:
    def test_from_list(self):
        ps = PositionSet.from_list(['AAPL UW', 'MSFT UW'])
        assert len(ps.positions) == 2
        assert ps.positions[0].weight == 0.5

    def test_from_dicts(self):
        dicts = [
            {'identifier': 'AAPL UW', 'weight': 0.6},
            {'identifier': 'MSFT UW', 'weight': 0.4},
        ]
        ps = PositionSet.from_dicts(dicts)
        assert len(ps.positions) == 2

    def test_from_dicts_with_tags(self):
        dicts = [
            {'identifier': 'AAPL UW', 'weight': 0.5, 'sector': 'Tech'},
            {'identifier': 'MSFT UW', 'weight': 0.5, 'sector': 'Tech'},
        ]
        ps = PositionSet.from_dicts(dicts, add_tags=True)
        assert ps.positions[0].tags is not None
        assert len(ps.positions[0].tags) == 1

    def test_from_frame_equalize_weights(self):
        df = pd.DataFrame({'identifier': ['AAPL UW', 'MSFT UW']})
        ps = PositionSet.from_frame(df)
        assert len(ps.positions) == 2
        assert ps.positions[0].weight == 0.5

    def test_from_frame_with_quantity(self):
        df = pd.DataFrame({
            'identifier': ['AAPL UW', 'MSFT UW'],
            'quantity': [100, 200],
        })
        ps = PositionSet.from_frame(df)
        assert ps.positions[0].quantity == 100
        assert ps.positions[0].weight is None

    def test_from_frame_with_weight(self):
        df = pd.DataFrame({
            'identifier': ['AAPL UW'],
            'weight': [1.0],
        })
        ps = PositionSet.from_frame(df, reference_notional=10000)
        assert ps.positions[0].weight == 1.0
        assert ps.reference_notional == 10000

    def test_from_frame_with_notional(self):
        df = pd.DataFrame({
            'identifier': ['AAPL UW'],
            'notional': [50000.0],
        })
        ps = PositionSet.from_frame(df)
        assert ps.positions[0].notional == 50000.0

    def test_from_frame_with_asset_id_column(self):
        df = pd.DataFrame({
            'identifier': ['AAPL UW'],
            'asset_id': ['MA1'],
            'weight': [1.0],
        })
        ps = PositionSet.from_frame(df)
        assert ps.positions[0].asset_id == 'MA1'

    def test_from_frame_filters_na_identifiers(self):
        df = pd.DataFrame({
            'identifier': ['AAPL UW', None],
            'weight': [0.5, 0.5],
        })
        ps = PositionSet.from_frame(df)
        assert len(ps.positions) == 1

    def test_from_frame_with_tags(self):
        df = pd.DataFrame({
            'identifier': ['AAPL UW'],
            'weight': [1.0],
            'sector': ['Tech'],
        })
        ps = PositionSet.from_frame(df, add_tags=True)
        assert ps.positions[0].tags is not None
        assert ps.positions[0].tags[0].name == 'sector'

    def test_from_frame_with_divisor(self):
        df = pd.DataFrame({'identifier': ['AAPL UW'], 'weight': [1.0]})
        ps = PositionSet.from_frame(df, divisor=100.0)
        assert ps.divisor == 100.0


# ────────────────────────────────────────────────────────────
# PositionSet.to_frame_many
# ────────────────────────────────────────────────────────────

class TestToFrameMany:
    def test_basic(self):
        ps1 = PositionSet(
            positions=[Position(identifier='A', weight=0.5, asset_id='MA1')],
            date=dt.date(2024, 1, 1),
        )
        ps2 = PositionSet(
            positions=[Position(identifier='B', weight=0.5, asset_id='MA2')],
            date=dt.date(2024, 1, 2),
        )
        df = PositionSet.to_frame_many([ps1, ps2])
        assert len(df) == 2
        assert 'identifier' in df.columns
        assert 'asset_id' in df.columns

    def test_empty_positions_filtered(self):
        ps1 = PositionSet(
            positions=[Position(identifier='A', weight=0.5, asset_id='MA1')],
            date=dt.date(2024, 1, 1),
        )
        ps_empty = PositionSet(positions=[], date=dt.date(2024, 1, 2))
        df = PositionSet.to_frame_many([ps1, ps_empty])
        assert len(df) == 1


# ────────────────────────────────────────────────────────────
# PositionSet price (single)
# ────────────────────────────────────────────────────────────

class TestPositionSetPrice:
    @patch.object(GsPriceApi, 'price_positions')
    def test_price_by_quantity(self, mock_price):
        from gs_quant.target.price import PositionPriceResponse
        mock_response = MagicMock()
        mock_response.positions = [
            MagicMock(
                asset_id='MA1',
                tags=None,
                quantity=100,
                weight=0.5,
                notional=50000,
                hard_to_borrow=False,
            ),
        ]
        mock_price.return_value = mock_response

        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', asset_id='MA1', quantity=100)],
            date=dt.date(2024, 1, 1),
        )
        ps.price(weighting_strategy=PositionSetWeightingStrategy.Quantity)
        assert len(ps.positions) == 1
        assert ps.positions[0].weight == 0.5

    @patch.object(GsPriceApi, 'price_positions')
    def test_price_by_weight(self, mock_price):
        mock_response = MagicMock()
        mock_response.positions = [
            MagicMock(
                asset_id='MA1',
                tags=None,
                quantity=100,
                weight=0.5,
                notional=50000,
                hard_to_borrow=False,
            ),
        ]
        mock_price.return_value = mock_response

        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', asset_id='MA1', weight=0.5)],
            date=dt.date(2024, 1, 1),
            reference_notional=100000,
        )
        ps.price(weighting_strategy=PositionSetWeightingStrategy.Weight)
        assert len(ps.positions) == 1
        assert ps.positions[0].quantity == 100

    @patch.object(GsPriceApi, 'price_positions')
    def test_price_by_notional(self, mock_price):
        mock_response = MagicMock()
        mock_response.positions = [
            MagicMock(
                asset_id='MA1',
                tags=None,
                quantity=100,
                weight=0.5,
                notional=50000,
                hard_to_borrow=False,
            ),
        ]
        mock_price.return_value = mock_response

        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', asset_id='MA1', notional=50000)],
            date=dt.date(2024, 1, 1),
        )
        ps.price(weighting_strategy=PositionSetWeightingStrategy.Notional)
        assert len(ps.positions) == 1

    @patch.object(GsPriceApi, 'price_positions')
    def test_price_with_unpriced(self, mock_price):
        mock_response = MagicMock()
        mock_response.positions = []  # No positions priced
        mock_price.return_value = mock_response

        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', asset_id='MA1', quantity=100)],
            date=dt.date(2024, 1, 1),
        )
        ps.price(weighting_strategy=PositionSetWeightingStrategy.Quantity)
        assert len(ps.positions) == 0
        assert len(ps.unpriced_positions) == 1

    @patch.object(GsPriceApi, 'price_positions')
    def test_price_fail_on_unpriced(self, mock_price):
        mock_response = MagicMock()
        mock_response.positions = []
        mock_price.return_value = mock_response

        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', asset_id='MA1', quantity=100)],
            date=dt.date(2024, 1, 1),
        )
        with pytest.raises(MqValueError, match='Failed to price'):
            ps.price(weighting_strategy=PositionSetWeightingStrategy.Quantity,
                     fail_on_unpriced_positions=True)

    @patch.object(GsPriceApi, 'price_positions')
    def test_price_handle_long_short(self, mock_price):
        mock_response = MagicMock()
        mock_response.gross_notional = 200000
        mock_response.positions = [
            MagicMock(
                asset_id='MA1',
                tags=None,
                quantity=100,
                weight=0.5,
                notional=100000,
                hard_to_borrow=False,
            ),
        ]
        mock_price.return_value = mock_response

        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', asset_id='MA1', quantity=100)],
            date=dt.date(2024, 1, 1),
        )
        ps.price(
            weighting_strategy=PositionSetWeightingStrategy.Quantity,
            handle_long_short=True,
        )
        # Weight should be copysigned from notional
        assert ps.reference_notional == 200000

    @patch.object(GsPriceApi, 'price_positions')
    def test_price_with_dataset_kwarg(self, mock_price):
        mock_response = MagicMock()
        mock_response.positions = [
            MagicMock(
                asset_id='MA1', tags=None, quantity=100, weight=0.5,
                notional=50000, hard_to_borrow=False,
            ),
        ]
        mock_price.return_value = mock_response

        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', asset_id='MA1', quantity=100)],
            date=dt.date(2024, 1, 1),
        )
        ps.price(weighting_strategy=PositionSetWeightingStrategy.Quantity,
                 dataset='CUSTOM_DS')
        assert len(ps.positions) == 1

    @patch.object(GsPriceApi, 'price_positions')
    def test_price_with_fractional_shares_kwarg(self, mock_price):
        mock_response = MagicMock()
        mock_response.positions = [
            MagicMock(
                asset_id='MA1', tags=None, quantity=100.5, weight=0.5,
                notional=50000, hard_to_borrow=False,
            ),
        ]
        mock_price.return_value = mock_response

        ps = PositionSet(
            positions=[Position(identifier='AAPL UW', asset_id='MA1', quantity=100.5)],
            date=dt.date(2024, 1, 1),
        )
        ps.price(weighting_strategy=PositionSetWeightingStrategy.Quantity,
                 fractional_shares=True)
        assert len(ps.positions) == 1

    @patch.object(GsPriceApi, 'price_positions')
    def test_price_with_tags(self, mock_price):
        mock_response = MagicMock()
        pos_mock = MagicMock()
        pos_mock.asset_id = 'MA1'
        pos_mock.tags = [PositionTag(name='a', value='1')]
        pos_mock.quantity = 100
        pos_mock.weight = 0.5
        pos_mock.notional = 50000
        pos_mock.hard_to_borrow = False
        mock_response.positions = [pos_mock]
        mock_price.return_value = mock_response

        ps = PositionSet(
            positions=[
                Position(identifier='AAPL UW', asset_id='MA1', quantity=100,
                         tags=[PositionTag(name='a', value='1')]),
            ],
            date=dt.date(2024, 1, 1),
        )
        ps.price(weighting_strategy=PositionSetWeightingStrategy.Quantity)
        assert len(ps.positions) == 1


# ────────────────────────────────────────────────────────────
# PositionSet weighting strategy defaults
# ────────────────────────────────────────────────────────────

class TestWeightingStrategyDefaults:
    def test_auto_detect_weight(self):
        ps = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1', weight=0.5)],
            reference_notional=10000,
        )
        # Should automatically detect Weight strategy; no error on price
        # (We test the private method indirectly)
        strategy = PositionSet._PositionSet__get_default_weighting_strategy(
            ps.positions, ps.reference_notional, None,
        )
        assert strategy == PositionSetWeightingStrategy.Weight

    def test_auto_detect_quantity(self):
        ps = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1', quantity=100)],
        )
        strategy = PositionSet._PositionSet__get_default_weighting_strategy(
            ps.positions, None, None,
        )
        assert strategy == PositionSetWeightingStrategy.Quantity

    def test_auto_detect_notional(self):
        ps = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1', notional=50000)],
        )
        strategy = PositionSet._PositionSet__get_default_weighting_strategy(
            ps.positions, None, None,
        )
        assert strategy == PositionSetWeightingStrategy.Notional

    def test_auto_detect_all_missing_raises(self):
        ps = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1')],
        )
        with pytest.raises(MqValueError, match='Unable to determine'):
            PositionSet._PositionSet__get_default_weighting_strategy(
                ps.positions, None, None,
            )

    def test_explicit_weight_missing_raises(self):
        ps = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1')],
        )
        with pytest.raises(MqValueError, match='must input'):
            PositionSet._PositionSet__get_default_weighting_strategy(
                ps.positions, 10000, PositionSetWeightingStrategy.Weight,
            )

    def test_explicit_quantity_missing_raises(self):
        ps = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1')],
        )
        with pytest.raises(MqValueError, match='must input'):
            PositionSet._PositionSet__get_default_weighting_strategy(
                ps.positions, None, PositionSetWeightingStrategy.Quantity,
            )

    def test_explicit_notional_missing_raises(self):
        ps = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1')],
        )
        with pytest.raises(MqValueError, match='must input'):
            PositionSet._PositionSet__get_default_weighting_strategy(
                ps.positions, None, PositionSetWeightingStrategy.Notional,
            )

    def test_weight_no_ref_notional_raises(self):
        ps = PositionSet(
            positions=[Position(identifier='A', asset_id='MA1', weight=0.5)],
        )
        with pytest.raises(MqValueError, match='reference notional'):
            PositionSet._PositionSet__get_default_weighting_strategy(
                ps.positions, None, PositionSetWeightingStrategy.Weight,
            )

    def test_weight_no_quantity_but_has_weight(self):
        """When positions have weight, missing_quantities is non-empty, no ref_notional ->
        should still pick Weight if ref_notional is set"""
        positions = [Position(identifier='A', asset_id='MA1', weight=0.5)]
        strategy = PositionSet._PositionSet__get_default_weighting_strategy(
            positions, 10000, None,
        )
        assert strategy == PositionSetWeightingStrategy.Weight


# ────────────────────────────────────────────────────────────
# PositionSet convert positions for pricing
# ────────────────────────────────────────────────────────────

class TestConvertPositionsForPricing:
    def test_missing_asset_id_raises(self):
        positions = [Position(identifier='A', quantity=100)]
        with pytest.raises(MqValueError, match='missing asset ids'):
            PositionSet._PositionSet__convert_positions_for_pricing(
                positions, PositionSetWeightingStrategy.Quantity,
            )

    def test_quantity_strategy(self):
        positions = [Position(identifier='A', asset_id='MA1', quantity=100, weight=0.5, notional=50000)]
        result = PositionSet._PositionSet__convert_positions_for_pricing(
            positions, PositionSetWeightingStrategy.Quantity,
        )
        assert len(result) == 1
        assert result[0].quantity == 100
        assert result[0].weight is None
        assert result[0].notional is None

    def test_weight_strategy(self):
        positions = [Position(identifier='A', asset_id='MA1', weight=0.5, quantity=100, notional=50000)]
        result = PositionSet._PositionSet__convert_positions_for_pricing(
            positions, PositionSetWeightingStrategy.Weight,
        )
        assert result[0].weight == 0.5
        assert result[0].quantity is None
        assert result[0].notional is None

    def test_notional_strategy(self):
        positions = [Position(identifier='A', asset_id='MA1', notional=50000, weight=0.5, quantity=100)]
        result = PositionSet._PositionSet__convert_positions_for_pricing(
            positions, PositionSetWeightingStrategy.Notional,
        )
        assert result[0].notional == 50000
        assert result[0].weight is None
        assert result[0].quantity is None


# ────────────────────────────────────────────────────────────
# PositionSet hash_position_tag_list
# ────────────────────────────────────────────────────────────

class TestHashPositionTagList:
    def test_none_tags(self):
        result = PositionSet._PositionSet__hash_position_tag_list(None)
        assert result == ''

    def test_empty_tags(self):
        result = PositionSet._PositionSet__hash_position_tag_list([])
        assert result == ''

    def test_with_tags(self):
        tags = [PositionTag(name='a', value='1'), PositionTag(name='b', value='2')]
        result = PositionSet._PositionSet__hash_position_tag_list(tags)
        assert result == 'a-1b-2'


# ────────────────────────────────────────────────────────────
# PositionSet.resolve_many
# ────────────────────────────────────────────────────────────

class TestResolveMany:
    def test_resolve_many_basic(self, mocker):
        xref_results = [
            {"assetId": "MA1", "bbid": "GS UN", "delisted": 'no',
             "startDate": "1952-01-01", "endDate": "2952-12-31"},
            {"assetId": "MA2", "bbid": "AAPL UW", "delisted": 'no',
             "startDate": "1952-01-01", "endDate": "2952-12-31"},
        ]
        resolved_positions = [
            {"assetId": "MA1", "name": "GS", "bbid": "GS UN",
             "tradingRestriction": None,
             "asOfDate": dt.datetime(2952, 12, 31),
             "startDate": dt.datetime(1952, 1, 1),
             "endDate": dt.datetime(2952, 12, 31)},
            {"assetId": "MA2", "name": "Apple", "bbid": "AAPL UW",
             "tradingRestriction": False,
             "asOfDate": dt.datetime(2952, 12, 31),
             "startDate": dt.datetime(1952, 1, 1),
             "endDate": dt.datetime(2952, 12, 31)},
        ]

        mocker.patch.object(
            position_set_module, "_get_asset_temporal_xrefs",
            return_value=(pd.DataFrame(xref_results), "bbid"),
        )
        mocker.patch.object(
            position_set_module, "_resolve_many_assets",
            return_value=pd.DataFrame(resolved_positions),
        )

        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='GS UN', weight=0.5),
                    Position(identifier='AAPL UW', weight=0.5),
                ],
            ),
        ]
        PositionSet.resolve_many(position_sets)
        assert len(position_sets[0].positions) == 2
        assert all(p.asset_id is not None for p in position_sets[0].positions)

    def test_resolve_many_weight_quantity_raises(self, mocker):
        """Should raise if both weight and quantity are present"""
        ps = PositionSet(
            date=dt.date(2024, 4, 30),
            positions=[
                Position(identifier='A', weight=0.5, quantity=100),
            ],
        )
        xref_results = [{"assetId": "MA1", "bbid": "A", "delisted": 'no',
                         "startDate": "1952-01-01", "endDate": "2952-12-31"}]
        mocker.patch.object(
            position_set_module, "_get_asset_temporal_xrefs",
            return_value=(pd.DataFrame(xref_results), "bbid"),
        )
        mocker.patch.object(position_set_module, "_resolve_many_assets",
                            return_value=pd.DataFrame([]))

        with pytest.raises(MqValueError, match="Cannot have both weight and quantity"):
            PositionSet.resolve_many([ps])


# ────────────────────────────────────────────────────────────
# PositionSet.price_many
# ────────────────────────────────────────────────────────────

class TestPriceMany:
    def test_price_many_invalid_strategy_raises(self):
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='A', asset_id='MA1', weight=0.5),
                ],
            ),
        ]
        with pytest.raises(MqValueError, match="Can only specify"):
            PositionSet.price_many(
                position_sets,
                weighting_strategy=PositionSetWeightingStrategy.Market_Capitalization,
            )

    def test_price_many_quantity_missing_raises(self):
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='A', asset_id='MA1', weight=0.5),
                ],
            ),
        ]
        with pytest.raises(MqValueError, match="Unable to price positions"):
            PositionSet.price_many(
                position_sets,
                weighting_strategy=PositionSetWeightingStrategy.Quantity,
            )

    def test_price_many_weight_and_quantity_raises(self):
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                positions=[
                    Position(identifier='A', asset_id='MA1', weight=0.5, quantity=100),
                ],
            ),
        ]
        with pytest.raises(MqValueError, match="Cannot have both weight and quantity"):
            PositionSet.price_many(position_sets)

    def test_price_many_basic(self, mocker):
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', weight=0.5,
                             tags=[PositionTag(name='t', value='v')]),
                    Position(identifier='B', asset_id='MA2', name='B', weight=0.5),
                ],
            ),
        ]
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 0.45, 'closePrice': 100,
                     'fxClosePrice': 1, 'quantity': 10, 'notional': 1000,
                     'referenceWeight': 0.5},
                    {'assetId': 'MA2', 'weight': 0.55, 'closePrice': 200,
                     'fxClosePrice': 1, 'quantity': 5, 'notional': 1000,
                     'referenceWeight': 0.5},
                ],
                'targetNotional': 1000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)

        PositionSet.price_many(position_sets)
        assert len(position_sets[0].positions) == 2

    def test_price_many_empty_positions(self, mocker):
        """Position sets with empty positions should be handled"""
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                positions=[],
            ),
            PositionSet(
                date=dt.date(2024, 5, 1),
                reference_notional=1000,
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', weight=0.5),
                ],
            ),
        ]
        pricing_results = [
            {
                'date': '2024-05-01',
                'positions': [
                    {'assetId': 'MA1', 'weight': 0.5, 'closePrice': 100,
                     'fxClosePrice': 1, 'quantity': 10, 'notional': 1000,
                     'referenceWeight': 0.5},
                ],
                'targetNotional': 1000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)

        PositionSet.price_many(position_sets)
        assert len(position_sets[1].positions) == 1

    def test_price_many_unpriced_results(self, mocker):
        """When pricing returns no results for a date, positions should be unpriced"""
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', weight=0.5),
                ],
            ),
        ]
        # _repeat_try_catch_request returns None when result is falsy (empty list)
        mocker.patch(
            'gs_quant.markets.position_set._repeat_try_catch_request',
            return_value=[],
        )

        PositionSet.price_many(position_sets)
        # When no pricing result for date, positions become None and unpriced_positions gets set
        assert position_sets[0].positions is None

    def test_price_many_notional_strategy(self, mocker):
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', notional=50000),
                ],
            ),
        ]
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 1.0, 'closePrice': 500,
                     'fxClosePrice': 1, 'quantity': 100, 'notional': 50000.0,
                     'referenceWeight': 1.0},
                ],
                'targetNotional': 50000.0,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)
        PositionSet.price_many(position_sets)
        assert len(position_sets[0].positions) == 1

    def test_price_many_quantity_strategy(self, mocker):
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', quantity=100),
                ],
            ),
        ]
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 1.0, 'closePrice': 500,
                     'fxClosePrice': 1, 'quantity': 100, 'notional': 50000,
                     'referenceWeight': 1.0},
                ],
                'targetNotional': 50000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)
        PositionSet.price_many(position_sets, weighting_strategy=PositionSetWeightingStrategy.Quantity)
        assert len(position_sets[0].positions) == 1

    def test_price_many_allow_partial_pricing(self, mocker):
        """When allow_partial_pricing is True and an error occurs, it should not raise"""
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', weight=0.5),
                ],
            ),
        ]
        mocker.patch(
            'gs_quant.markets.position_set._repeat_try_catch_request',
            side_effect=MqRequestError(500, 'Server Error'),
        )
        # Should not raise
        PositionSet.price_many(position_sets, allow_partial_pricing=True)
        # Positions become None because no pricing result
        assert position_sets[0].positions is None

    def test_price_many_error_no_partial_raises(self, mocker):
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', weight=0.5),
                ],
            ),
        ]
        mocker.patch(
            'gs_quant.markets.position_set._repeat_try_catch_request',
            side_effect=MqRequestError(500, 'Server Error'),
        )
        with pytest.raises(MqRequestError):
            PositionSet.price_many(position_sets, allow_partial_pricing=False)


# ────────────────────────────────────────────────────────────
# Branch coverage: clone with quantity + ref_notional
# Branches: [324,325], [325,326], [325,328]
# ────────────────────────────────────────────────────────────

class TestCloneBranches:
    def test_clone_keep_reference_notional_drops_quantity(self):
        """Branch [324,325] -> [325,326]: quantity in frame AND ref_notional is not None AND keep_reference_notional=True"""
        # Build position set with both quantity and weight, then set reference_notional after construction
        ps = PositionSet(
            date=dt.date(2024, 1, 1),
            positions=[
                Position(identifier='A', asset_id='MA1', name='A', weight=0.6, quantity=10),
                Position(identifier='B', asset_id='MA2', name='B', weight=0.4, quantity=20),
            ],
        )
        # Set reference_notional after construction to bypass validation
        ps.reference_notional = 10000
        # Now clone with keep_reference_notional=True
        result = ps.clone(keep_reference_notional=True)
        assert result.reference_notional == 10000
        # Positions should have weight (quantity was dropped)
        for p in result.positions:
            assert p.weight is not None

    def test_clone_no_keep_reference_notional_drops_ref(self):
        """Branch [324,325] -> [325,328]: quantity in frame AND ref_notional not None AND keep_reference_notional=False"""
        ps = PositionSet(
            date=dt.date(2024, 1, 1),
            positions=[
                Position(identifier='A', asset_id='MA1', name='A', weight=0.6, quantity=10),
                Position(identifier='B', asset_id='MA2', name='B', weight=0.4, quantity=20),
            ],
        )
        ps.reference_notional = 10000
        result = ps.clone(keep_reference_notional=False)
        # ref_notional should be set to None
        assert result.reference_notional is None


# ────────────────────────────────────────────────────────────
# Branch coverage: resolve_many additional branches
# Branches: [1264,1266], [1266,1268], [1273,1274],
#           [1308,1312], [1309,1310], [1331,1332], [1340,1341]
# ────────────────────────────────────────────────────────────

class TestResolveManyBranches:
    def test_resolve_many_no_name_no_asset_id(self, mocker):
        """Branch [1264,1266] name not in columns; [1266,1268] asset_id not in columns"""
        # Position without name or asset_id already set -> those columns are None/missing
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='GS UN', weight=0.5),
                    Position(identifier='AAPL UW', weight=0.5),
                ],
            ),
        ]
        xref_results = [
            {"assetId": "MA1", "bbid": "GS UN", "delisted": 'no',
             "startDate": "1952-01-01", "endDate": "2952-12-31"},
            {"assetId": "MA2", "bbid": "AAPL UW", "delisted": 'no',
             "startDate": "1952-01-01", "endDate": "2952-12-31"},
        ]
        resolved_positions = [
            {"assetId": "MA1", "name": "GS", "bbid": "GS UN",
             "tradingRestriction": None,
             "asOfDate": dt.datetime(2952, 12, 31),
             "startDate": dt.datetime(1952, 1, 1),
             "endDate": dt.datetime(2952, 12, 31)},
            {"assetId": "MA2", "name": "Apple", "bbid": "AAPL UW",
             "tradingRestriction": False,
             "asOfDate": dt.datetime(2952, 12, 31),
             "startDate": dt.datetime(1952, 1, 1),
             "endDate": dt.datetime(2952, 12, 31)},
        ]
        mocker.patch.object(
            position_set_module, "_get_asset_temporal_xrefs",
            return_value=(pd.DataFrame(xref_results), "bbid"),
        )
        mocker.patch.object(
            position_set_module, "_group_temporal_xrefs_into_discrete_time_ranges",
        )
        mocker.patch.object(
            position_set_module, "_resolve_many_assets",
            return_value=pd.DataFrame(resolved_positions),
        )
        PositionSet.resolve_many(position_sets)
        assert len(position_sets[0].positions) == 2

    def test_resolve_many_quantity_and_notional_raises(self, mocker):
        """Branch [1273,1274]: both quantity and notional in position sets"""
        ps = PositionSet(
            date=dt.date(2024, 4, 30),
            positions=[
                Position(identifier='A', quantity=100, notional=50000),
            ],
        )
        xref_results = [{"assetId": "MA1", "bbid": "A", "delisted": 'no',
                         "startDate": "1952-01-01", "endDate": "2952-12-31"}]
        mocker.patch.object(
            position_set_module, "_get_asset_temporal_xrefs",
            return_value=(pd.DataFrame(xref_results), "bbid"),
        )
        mocker.patch.object(
            position_set_module, "_group_temporal_xrefs_into_discrete_time_ranges",
        )
        mocker.patch.object(position_set_module, "_resolve_many_assets",
                            return_value=pd.DataFrame([]))
        with pytest.raises(MqValueError, match="Cannot have both weight and notional"):
            PositionSet.resolve_many([ps])

    def test_resolve_many_ref_notional_drops_quantity(self, mocker):
        """Branch [1308,1312] -> [1309,1310]: reference_notional in df, quantity in df -> drop quantity"""
        # Positions with quantity (no weight) and PositionSet with reference_notional.
        # The constructor forbids this, so build without ref_notional and set it afterwards.
        ps = PositionSet(
            date=dt.date(2024, 4, 30),
            positions=[
                Position(identifier='GS UN', quantity=100),
            ],
        )
        ps.reference_notional = 1000  # set after construction to bypass validation
        position_sets = [ps]

        xref_results = [
            {"assetId": "MA1", "bbid": "GS UN", "delisted": 'no',
             "startDate": "1952-01-01", "endDate": "2952-12-31"},
        ]
        resolved_positions = [
            {"assetId": "MA1", "name": "GS", "bbid": "GS UN",
             "tradingRestriction": None,
             "asOfDate": dt.datetime(2952, 12, 31),
             "startDate": dt.datetime(1952, 1, 1),
             "endDate": dt.datetime(2952, 12, 31)},
        ]
        mocker.patch.object(
            position_set_module, "_get_asset_temporal_xrefs",
            return_value=(pd.DataFrame(xref_results), "bbid"),
        )
        mocker.patch.object(
            position_set_module, "_group_temporal_xrefs_into_discrete_time_ranges",
        )
        mocker.patch.object(
            position_set_module, "_resolve_many_assets",
            return_value=pd.DataFrame(resolved_positions),
        )
        PositionSet.resolve_many(position_sets)
        assert len(position_sets[0].positions) == 1

    def test_resolve_many_date_not_date_type(self, mocker):
        """Branch [1331,1332]: position_set.date is not a dt.date -> converts via pd.Timestamp"""
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='GS UN', weight=0.5),
                ],
            ),
        ]
        # Override date to be a string (not a dt.date)
        position_sets[0].date = '2024-04-30'

        xref_results = [
            {"assetId": "MA1", "bbid": "GS UN", "delisted": 'no',
             "startDate": "1952-01-01", "endDate": "2952-12-31"},
        ]
        resolved_positions = [
            {"assetId": "MA1", "name": "GS", "bbid": "GS UN",
             "tradingRestriction": None,
             "asOfDate": dt.datetime(2952, 12, 31),
             "startDate": dt.datetime(1952, 1, 1),
             "endDate": dt.datetime(2952, 12, 31)},
        ]
        mocker.patch.object(
            position_set_module, "_get_asset_temporal_xrefs",
            return_value=(pd.DataFrame(xref_results), "bbid"),
        )
        mocker.patch.object(
            position_set_module, "_group_temporal_xrefs_into_discrete_time_ranges",
        )
        mocker.patch.object(
            position_set_module, "_resolve_many_assets",
            return_value=pd.DataFrame(resolved_positions),
        )
        PositionSet.resolve_many(position_sets)
        assert isinstance(position_sets[0].date, dt.date)
        assert position_sets[0].date == dt.date(2024, 4, 30)

    def test_resolve_many_unresolved_positions(self, mocker):
        """Branch [1340,1341]: unresolved positions (assetId is NaN)"""
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                reference_notional=1000,
                positions=[
                    Position(identifier='GS UN', weight=0.5),
                    Position(identifier='BAD', weight=0.5),
                ],
            ),
        ]
        xref_results = [
            {"assetId": "MA1", "bbid": "GS UN", "delisted": 'no',
             "startDate": "1952-01-01", "endDate": "2952-12-31"},
            {"assetId": None, "bbid": "BAD", "delisted": 'no',
             "startDate": "1952-01-01", "endDate": "2952-12-31"},
        ]
        resolved_positions = [
            {"assetId": "MA1", "name": "GS", "bbid": "GS UN",
             "tradingRestriction": None,
             "asOfDate": dt.datetime(2952, 12, 31),
             "startDate": dt.datetime(1952, 1, 1),
             "endDate": dt.datetime(2952, 12, 31)},
            {"assetId": None, "name": None, "bbid": "BAD",
             "tradingRestriction": None,
             "asOfDate": dt.datetime(2952, 12, 31),
             "startDate": dt.datetime(1952, 1, 1),
             "endDate": dt.datetime(2952, 12, 31)},
        ]
        mocker.patch.object(
            position_set_module, "_get_asset_temporal_xrefs",
            return_value=(pd.DataFrame(xref_results), "bbid"),
        )
        mocker.patch.object(
            position_set_module, "_group_temporal_xrefs_into_discrete_time_ranges",
        )
        mocker.patch.object(
            position_set_module, "_resolve_many_assets",
            return_value=pd.DataFrame(resolved_positions),
        )
        PositionSet.resolve_many(position_sets)
        # One resolved, one unresolved
        assert len(position_sets[0].positions) == 1
        assert len(position_sets[0].unresolved_positions) == 1


# ────────────────────────────────────────────────────────────
# Branch coverage: price_many additional branches
# Branches: [1413,1414], [1419,1422], [1450,1451],
#           [1455,1456], [1459,1460], [1461,1466],
#           [1463,1464], [1534,1536]
# ────────────────────────────────────────────────────────────

class TestPriceManyBranches:
    def test_price_many_notional_and_weight_raises(self):
        """Branch [1413,1414]: notional + weight in columns -> raise"""
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                positions=[
                    Position(identifier='A', asset_id='MA1', weight=0.5, notional=50000),
                ],
            ),
        ]
        with pytest.raises(MqValueError, match="Cannot have both weight and notional"):
            PositionSet.price_many(position_sets)

    def test_price_many_default_notional_strategy(self, mocker):
        """Branch [1419,1422]: notional in columns, no weight -> strategy = Notional"""
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', notional=50000),
                ],
            ),
        ]
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 1.0, 'closePrice': 500,
                     'fxClosePrice': 1, 'quantity': 100, 'notional': 50000,
                     'referenceWeight': 1.0},
                ],
                'targetNotional': 50000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)
        PositionSet.price_many(position_sets)
        assert len(position_sets[0].positions) == 1

    def test_price_many_default_quantity_strategy(self, mocker):
        """Branch [1419,1422] else: no weight, no notional -> strategy = Quantity"""
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', quantity=100),
                ],
            ),
        ]
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 1.0, 'closePrice': 500,
                     'fxClosePrice': 1, 'quantity': 100, 'notional': 50000,
                     'referenceWeight': 1.0},
                ],
                'targetNotional': 50000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)
        # No explicit weighting_strategy, default should pick Quantity
        PositionSet.price_many(position_sets)
        assert len(position_sets[0].positions) == 1

    def test_price_many_kwargs_set_on_params(self, mocker):
        """Branch [1450,1451]: kwargs present -> setattr on pricing parameters"""
        position_sets = [
            PositionSet(
                date=dt.date(2024, 4, 30),
                positions=[
                    Position(identifier='A', asset_id='MA1', name='A', quantity=100),
                ],
            ),
        ]
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 1.0, 'closePrice': 500,
                     'fxClosePrice': 1, 'quantity': 100, 'notional': 50000,
                     'referenceWeight': 1.0},
                ],
                'targetNotional': 50000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)
        # Pass extra kwargs to trigger the branch
        PositionSet.price_many(position_sets, weighting_strategy=PositionSetWeightingStrategy.Quantity,
                               some_custom_param='test_value')
        assert len(position_sets[0].positions) == 1

    def test_price_many_missing_weights_warning(self, mocker):
        """Branch [1455,1456]: weight strategy with some positions missing weights"""
        # Build without reference_notional first, then set it to bypass validation
        ps = PositionSet(
            date=dt.date(2024, 4, 30),
            positions=[
                Position(identifier='A', asset_id='MA1', name='A', weight=0.5),
                Position(identifier='B', asset_id='MA2', name='B'),  # no weight
            ],
        )
        ps.reference_notional = 1000
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 1.0, 'closePrice': 500,
                     'fxClosePrice': 1, 'quantity': 10, 'notional': 1000,
                     'referenceWeight': 0.5},
                ],
                'targetNotional': 1000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)
        with patch('gs_quant.markets.position_set._logger') as mock_logger:
            PositionSet.price_many([ps], weighting_strategy=PositionSetWeightingStrategy.Weight)
            mock_logger.warning.assert_any_call("Some positions do not have weights. These will be filtered out")

    def test_price_many_missing_exposures_warning(self, mocker):
        """Branch [1459,1460]: notional strategy with some positions missing notional"""
        ps = PositionSet(
            date=dt.date(2024, 4, 30),
            positions=[
                Position(identifier='A', asset_id='MA1', name='A', notional=50000),
                Position(identifier='B', asset_id='MA2', name='B'),  # no notional
            ],
        )
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 1.0, 'closePrice': 500,
                     'fxClosePrice': 1, 'quantity': 100, 'notional': 50000,
                     'referenceWeight': 1.0},
                ],
                'targetNotional': 50000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)
        with patch('gs_quant.markets.position_set._logger') as mock_logger:
            PositionSet.price_many([ps], weighting_strategy=PositionSetWeightingStrategy.Notional)
            mock_logger.warning.assert_any_call("Some positions do not have exposures. These will be filtered out")

    def test_price_many_missing_quantities_warning(self, mocker):
        """Branch [1461,1466] -> [1463,1464]: quantity strategy with some positions missing quantities"""
        ps = PositionSet(
            date=dt.date(2024, 4, 30),
            positions=[
                Position(identifier='A', asset_id='MA1', name='A', quantity=100),
                Position(identifier='B', asset_id='MA2', name='B'),  # no quantity
            ],
        )
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 1.0, 'closePrice': 500,
                     'fxClosePrice': 1, 'quantity': 100, 'notional': 50000,
                     'referenceWeight': 1.0},
                ],
                'targetNotional': 50000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)
        with patch('gs_quant.markets.position_set._logger') as mock_logger:
            PositionSet.price_many([ps], weighting_strategy=PositionSetWeightingStrategy.Quantity)
            mock_logger.warning.assert_any_call("Some positions do not have quantities. These will be filtered out")

    def test_price_many_date_not_date_type(self, mocker):
        """Branch [1534,1536]: input_position_set.date is not dt.date -> convert via pd.to_datetime"""
        ps = PositionSet(
            date=dt.date(2024, 4, 30),
            positions=[
                Position(identifier='A', asset_id='MA1', name='A', quantity=100),
            ],
        )
        # Override date to be a string
        ps.date = '2024-04-30'
        pricing_results = [
            {
                'date': '2024-04-30',
                'positions': [
                    {'assetId': 'MA1', 'weight': 1.0, 'closePrice': 500,
                     'fxClosePrice': 1, 'quantity': 100, 'notional': 50000,
                     'referenceWeight': 1.0},
                ],
                'targetNotional': 50000,
            },
        ]
        mocker.patch.object(GsPriceApi, 'price_many_positions', return_value=pricing_results)
        PositionSet.price_many([ps], weighting_strategy=PositionSetWeightingStrategy.Quantity)
        assert isinstance(ps.date, dt.date)
        assert ps.date == dt.date(2024, 4, 30)


if __name__ == '__main__':
    pytest.main(args=[__file__])
