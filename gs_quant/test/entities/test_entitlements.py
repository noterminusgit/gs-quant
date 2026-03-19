"""
Comprehensive branch-coverage tests for gs_quant/entities/entitlements.py.
Covers User, Group, EntitlementBlock, and Entitlements classes fully.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd

from gs_quant.common import Entitlements as TargetEntitlements
from gs_quant.entities.entitlements import (
    User,
    Group,
    EntitlementBlock,
    Entitlements,
)
from gs_quant.errors import MqValueError, MqRequestError
from gs_quant.target.groups import Group as TargetGroup


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_user(user_id='userId', name='Jane Doe', email='jane@gs.com', company='GS'):
    return User(user_id=user_id, name=name, email=email, company=company)


def _fake_group(group_id='groupId', name='fakeGroup'):
    return Group(group_id=group_id, name=name)


def _make_target_user(user_id='userId', name='Jane Doe', email='jane@gs.com', company='GS'):
    m = MagicMock()
    m.id = user_id
    m.name = name
    m.email = email
    m.company = company
    return m


def _make_target_group(group_id='groupId', name='fakeGroup', entitlements=None,
                        description='desc', tags=None):
    m = MagicMock()
    m.id = group_id
    m.name = name
    m.entitlements = entitlements
    m.description = description
    m.tags = tags or []
    return m


# ===========================================================================
# User
# ===========================================================================

class TestUser:
    def test_init_and_properties(self):
        u = User(user_id='u1', name='Test', email='t@t.com', company='Co')
        assert u.id == 'u1'
        assert u.name == 'Test'
        assert u.email == 't@t.com'
        assert u.company == 'Co'

    def test_init_defaults(self):
        u = User(user_id='u1')
        assert u.name is None
        assert u.email is None
        assert u.company is None

    def test_eq(self):
        u1 = User(user_id='u1', name='A')
        u2 = User(user_id='u1', name='B')
        u3 = User(user_id='u2', name='A')
        assert u1 == u2
        assert u1 != u3

    def test_hash(self):
        u1 = User(user_id='u1', name='A')
        u2 = User(user_id='u1', name='A')
        assert hash(u1) == hash(u2)

    def test_save_raises(self):
        u = User(user_id='u1')
        with pytest.raises(NotImplementedError):
            u.save()

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_by_user_id(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user()]
        u = User.get(user_id='userId')
        assert u.id == 'userId'
        assert u.name == 'Jane Doe'

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_by_user_id_with_guid_prefix(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user()]
        u = User.get(user_id='guid:userId')
        assert u.id == 'userId'
        # Should strip 'guid:' prefix
        mock_get_users.assert_called_once_with(
            user_ids=['userId'], user_names=None, user_emails=None
        )

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_by_name(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user()]
        u = User.get(name='Jane Doe')
        assert u.name == 'Jane Doe'

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_by_email(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user()]
        u = User.get(email='jane@gs.com')
        assert u.email == 'jane@gs.com'

    def test_get_no_params_raises(self):
        with pytest.raises(MqValueError, match='Please specify'):
            User.get()

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_multiple_results_raises(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user('u1'), _make_target_user('u2')]
        with pytest.raises(MqValueError, match='more than one user'):
            User.get(name='common name')

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_no_results_raises(self, mock_get_users):
        mock_get_users.return_value = []
        with pytest.raises(MqValueError, match='No user found'):
            User.get(user_id='nonexistent')

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_many(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user('u1'), _make_target_user('u2')]
        users = User.get_many(user_ids=['u1', 'u2'])
        assert len(users) == 2
        assert users[0].id == 'u1'
        assert users[1].id == 'u2'

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_many_with_guid_prefix(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user('u1')]
        users = User.get_many(user_ids=['guid:u1'])
        assert len(users) == 1
        mock_get_users.assert_called_once_with(
            user_ids=['u1'], user_names=[], user_emails=[], user_companies=[]
        )

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_many_by_emails(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user()]
        users = User.get_many(emails=['Jane@GS.com'])
        assert len(users) == 1
        # Emails should be lowercased
        mock_get_users.assert_called_once_with(
            user_ids=[], user_names=[], user_emails=['jane@gs.com'], user_companies=[]
        )

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_many_by_names(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user()]
        users = User.get_many(names=['Jane Doe'])
        assert len(users) == 1

    @patch('gs_quant.entities.entitlements.GsUsersApi.get_users')
    def test_get_many_by_companies(self, mock_get_users):
        mock_get_users.return_value = [_make_target_user()]
        users = User.get_many(companies=['GS'])
        assert len(users) == 1

    def test_get_many_empty(self):
        result = User.get_many()
        assert result == []

    def test_get_many_all_none(self):
        result = User.get_many(user_ids=None, names=None, emails=None, companies=None)
        assert result == []

    def test_get_many_all_empty_lists(self):
        result = User.get_many(user_ids=[], names=[], emails=[], companies=[])
        assert result == []


# ===========================================================================
# Group
# ===========================================================================

class TestGroup:
    def test_init_and_properties(self):
        g = Group(group_id='g1', name='mygroup', description='desc', tags=['t1'])
        assert g.id == 'g1'
        assert g.name == 'mygroup'
        assert g.description == 'desc'
        assert g.tags == ['t1']
        assert g.entitlements is None

    def test_setters(self):
        g = Group(group_id='g1', name='old')
        g.name = 'new'
        g.description = 'new desc'
        g.tags = ['t2']
        g.entitlements = EntitlementBlock()
        assert g.name == 'new'
        assert g.description == 'new desc'
        assert g.tags == ['t2']
        assert g.entitlements is not None

    def test_eq(self):
        g1 = Group(group_id='g1', name='A')
        g2 = Group(group_id='g1', name='B')
        g3 = Group(group_id='g2', name='A')
        assert g1 == g2
        assert g1 != g3

    def test_hash(self):
        g1 = Group(group_id='g1', name='A')
        g2 = Group(group_id='g1', name='A')
        assert hash(g1) == hash(g2)

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_group')
    def test_get(self, mock_get_group):
        mock_get_group.return_value = _make_target_group()
        g = Group.get('groupId')
        assert g.id == 'groupId'
        assert g.name == 'fakeGroup'

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_group')
    def test_get_with_group_prefix(self, mock_get_group):
        mock_get_group.return_value = _make_target_group()
        g = Group.get('group:groupId')
        mock_get_group.assert_called_once_with(group_id='groupId')

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_group')
    def test_get_with_entitlements(self, mock_get_group):
        target_ent = MagicMock(spec=TargetEntitlements)
        target_ent.as_dict.return_value = {'admin': ['role:admin_role']}
        mock_result = _make_target_group(entitlements=target_ent)
        mock_get_group.return_value = mock_result

        with patch('gs_quant.entities.entitlements.Entitlements.from_target') as mock_from:
            mock_from.return_value = Entitlements()
            g = Group.get('groupId')
            mock_from.assert_called_once_with(target_ent)

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_group')
    def test_get_without_entitlements(self, mock_get_group):
        mock_get_group.return_value = _make_target_group(entitlements=None)
        g = Group.get('groupId')
        assert g.entitlements is None

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_groups')
    def test_get_many(self, mock_get_groups):
        mock_get_groups.return_value = [_make_target_group('g1', 'group1'), _make_target_group('g2', 'group2')]
        groups = Group.get_many(group_ids=['g1', 'g2'])
        assert len(groups) == 2

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_groups')
    def test_get_many_with_group_prefix(self, mock_get_groups):
        mock_get_groups.return_value = [_make_target_group('g1')]
        groups = Group.get_many(group_ids=['group:g1'])
        mock_get_groups.assert_called_once_with(ids=['g1'], names=[])

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_groups')
    def test_get_many_by_names(self, mock_get_groups):
        mock_get_groups.return_value = [_make_target_group()]
        groups = Group.get_many(names=['fakeGroup'])
        assert len(groups) == 1

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_groups')
    def test_get_many_with_entitlements(self, mock_get_groups):
        target_ent = MagicMock(spec=TargetEntitlements)
        target_ent.as_dict.return_value = {}
        mock_get_groups.return_value = [_make_target_group(entitlements=target_ent)]
        with patch('gs_quant.entities.entitlements.Entitlements.from_target') as mock_from:
            mock_from.return_value = Entitlements()
            groups = Group.get_many(group_ids=['g1'])
            mock_from.assert_called_once()

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_groups')
    def test_get_many_without_entitlements(self, mock_get_groups):
        mock_get_groups.return_value = [_make_target_group(entitlements=None)]
        groups = Group.get_many(group_ids=['g1'])
        assert groups[0].entitlements is None

    def test_get_many_empty(self):
        result = Group.get_many()
        assert result == []

    def test_get_many_all_empty(self):
        result = Group.get_many(group_ids=[], names=[])
        assert result == []

    @patch('gs_quant.entities.entitlements.GsGroupsApi.update_group')
    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_group')
    def test_save_update(self, mock_get_group, mock_update):
        # _group_exists returns True
        mock_get_group.return_value = _make_target_group()
        mock_update.return_value = _make_target_group()
        g = Group(group_id='g1', name='test')
        result = g.save()
        assert result.id == 'groupId'
        mock_update.assert_called_once()

    @patch('gs_quant.entities.entitlements.GsGroupsApi.create_group')
    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_group')
    def test_save_create(self, mock_get_group, mock_create):
        # _group_exists returns False (404)
        mock_get_group.side_effect = MqRequestError(404, 'Not Found')
        mock_create.return_value = _make_target_group()
        g = Group(group_id='g1', name='test')
        result = g.save()
        mock_create.assert_called_once()

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_group')
    def test_save_other_error_raises(self, mock_get_group):
        mock_get_group.side_effect = MqRequestError(500, 'Server Error')
        g = Group(group_id='g1', name='test')
        with pytest.raises(MqRequestError) as exc:
            g.save()
        assert exc.value.status == 500

    @patch('gs_quant.entities.entitlements.GsGroupsApi.update_group')
    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_group')
    def test_save_update_with_entitlements(self, mock_get_group, mock_update):
        target_ent = MagicMock(spec=TargetEntitlements)
        target_ent.as_dict.return_value = {}
        result_group = _make_target_group(entitlements=target_ent)
        mock_get_group.return_value = _make_target_group()
        mock_update.return_value = result_group
        g = Group(group_id='g1', name='test')
        with patch('gs_quant.entities.entitlements.Entitlements.from_target') as mock_from:
            mock_from.return_value = Entitlements()
            result = g.save()
            mock_from.assert_called()

    @patch('gs_quant.entities.entitlements.GsGroupsApi.create_group')
    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_group')
    def test_save_create_without_entitlements(self, mock_get_group, mock_create):
        mock_get_group.side_effect = MqRequestError(404, 'Not Found')
        mock_create.return_value = _make_target_group(entitlements=None)
        g = Group(group_id='g1', name='test')
        result = g.save()
        assert result.entitlements is None

    @patch('gs_quant.entities.entitlements.GsGroupsApi.delete_group')
    def test_delete(self, mock_delete):
        g = Group(group_id='g1', name='test')
        g.delete()
        mock_delete.assert_called_once_with('g1')

    @patch('gs_quant.entities.entitlements.GsGroupsApi.get_users_in_group')
    def test_get_users(self, mock_get_users):
        mock_get_users.return_value = [
            {'id': 'u1', 'name': 'User 1', 'email': 'u1@gs.com', 'company': 'GS'},
            {'id': 'u2', 'name': 'User 2', 'email': 'u2@gs.com', 'company': 'GS'},
        ]
        g = Group(group_id='g1', name='test')
        users = g.get_users()
        assert len(users) == 2
        assert users[0].id == 'u1'
        assert users[1].id == 'u2'

    @patch('gs_quant.entities.entitlements.GsGroupsApi.add_users_to_group')
    def test_add_users(self, mock_add):
        g = Group(group_id='g1', name='test')
        users = [_fake_user('u1'), _fake_user('u2')]
        g.add_users(users)
        mock_add.assert_called_once_with(group_id='g1', user_ids=['u1', 'u2'])

    @patch('gs_quant.entities.entitlements.GsGroupsApi.delete_users_from_group')
    def test_delete_users(self, mock_del):
        g = Group(group_id='g1', name='test')
        users = [_fake_user('u1')]
        g.delete_users(users)
        mock_del.assert_called_once_with(group_id='g1', user_ids=['u1'])

    def test_to_dict(self):
        g = Group(group_id='g1', name='test', description='d', tags=['t'])
        d = g.to_dict()
        assert d['id'] == 'g1'
        assert d['name'] == 'test'
        assert d['description'] == 'd'
        assert d['tags'] == ['t']
        assert d['entitlements'] is None

    def test_to_dict_with_entitlements(self):
        ent = Entitlements()
        g = Group(group_id='g1', name='test', entitlements=ent)
        d = g.to_dict()
        assert d['entitlements'] is not None

    def test_to_target(self):
        g = Group(group_id='g1', name='test', description='d', tags=['t'])
        t = g.to_target()
        assert isinstance(t, TargetGroup)

    def test_to_target_with_entitlements(self):
        ent = Entitlements(admin=EntitlementBlock(roles=['admin_role']))
        g = Group(group_id='g1', name='test', entitlements=ent)
        t = g.to_target()
        assert t.entitlements is not None

    def test_to_target_without_entitlements(self):
        g = Group(group_id='g1', name='test', entitlements=None)
        t = g.to_target()
        assert t.entitlements is None

    def test_group_exists_true(self):
        g = Group(group_id='g1', name='test')
        with patch('gs_quant.entities.entitlements.GsGroupsApi.get_group') as mock_get:
            mock_get.return_value = _make_target_group()
            assert g._group_exists() is True

    def test_group_exists_false_404(self):
        g = Group(group_id='g1', name='test')
        with patch('gs_quant.entities.entitlements.GsGroupsApi.get_group') as mock_get:
            mock_get.side_effect = MqRequestError(404, 'Not Found')
            assert g._group_exists() is False

    def test_group_exists_other_error(self):
        g = Group(group_id='g1', name='test')
        with patch('gs_quant.entities.entitlements.GsGroupsApi.get_group') as mock_get:
            mock_get.side_effect = MqRequestError(500, 'Error')
            with pytest.raises(MqRequestError):
                g._group_exists()


# ===========================================================================
# EntitlementBlock
# ===========================================================================

class TestEntitlementBlock:
    def test_init_defaults(self):
        eb = EntitlementBlock()
        assert eb.users == []
        assert eb.groups == []
        assert eb.roles == []
        assert eb.unconverted_tokens is None

    def test_init_with_values(self):
        u = _fake_user()
        g = _fake_group()
        eb = EntitlementBlock(users=[u], groups=[g], roles=['role1'], unconverted_tokens=['tok1'])
        assert len(eb.users) == 1
        assert len(eb.groups) == 1
        assert eb.roles == ['role1']
        assert eb.unconverted_tokens == ['tok1']

    def test_init_deduplicates(self):
        u = _fake_user()
        eb = EntitlementBlock(users=[u, u])
        assert len(eb.users) == 1

    def test_setters(self):
        eb = EntitlementBlock()
        u1 = _fake_user('u1')
        u2 = _fake_user('u2')
        eb.users = [u1, u2]
        assert len(eb.users) == 2

        g1 = _fake_group('g1')
        eb.groups = [g1, g1]
        assert len(eb.groups) == 1

        eb.roles = ['r1', 'r1']
        assert len(eb.roles) == 1

    def test_eq_true(self):
        u = _fake_user()
        eb1 = EntitlementBlock(users=[u])
        eb2 = EntitlementBlock(users=[u])
        assert eb1 == eb2

    def test_eq_false_different_users(self):
        eb1 = EntitlementBlock(users=[_fake_user('u1')])
        eb2 = EntitlementBlock(users=[_fake_user('u2')])
        assert eb1 != eb2

    def test_eq_false_not_entitlement_block(self):
        eb = EntitlementBlock()
        assert eb != "not an EntitlementBlock"

    def test_eq_both_none_props(self):
        """When both self and other have None for a property."""
        eb1 = EntitlementBlock()
        eb2 = EntitlementBlock()
        assert eb1 == eb2

    def test_is_empty_true(self):
        eb = EntitlementBlock()
        assert eb.is_empty() is True

    def test_is_empty_false(self):
        eb = EntitlementBlock(users=[_fake_user()])
        assert eb.is_empty() is False

    def test_is_empty_false_role(self):
        eb = EntitlementBlock(roles=['r1'])
        assert eb.is_empty() is False

    def test_is_empty_false_group(self):
        eb = EntitlementBlock(groups=[_fake_group()])
        assert eb.is_empty() is False

    def test_to_list_as_strings(self):
        u = _fake_user('u1')
        g = _fake_group('g1')
        eb = EntitlementBlock(users=[u], groups=[g], roles=['admin'])
        result = eb.to_list()
        assert 'guid:u1' in result
        assert 'group:g1' in result
        assert 'role:admin' in result

    def test_to_list_as_dicts(self):
        u = _fake_user('u1', name='User1')
        g = _fake_group('g1', name='Group1')
        eb = EntitlementBlock(users=[u], groups=[g], roles=['admin'])
        result = eb.to_list(as_dicts=True, action='edit')
        assert len(result) == 3
        user_entry = [r for r in result if r['type'] == 'user'][0]
        assert user_entry['action'] == 'edit'
        assert user_entry['name'] == 'User1'
        assert user_entry['id'] == 'u1'
        group_entry = [r for r in result if r['type'] == 'group'][0]
        assert group_entry['id'] == 'g1'
        role_entry = [r for r in result if r['type'] == 'role'][0]
        assert role_entry['id'] == 'admin'
        assert role_entry['name'] == 'admin'

    def test_to_list_include_all_tokens(self):
        eb = EntitlementBlock(users=[], unconverted_tokens=['unknown:token'])
        result = eb.to_list(include_all_tokens=True)
        assert 'unknown:token' in result

    def test_to_list_no_include_all_tokens(self):
        eb = EntitlementBlock(users=[], unconverted_tokens=['unknown:token'])
        result = eb.to_list(include_all_tokens=False)
        assert 'unknown:token' not in result

    def test_to_list_unconverted_none_with_include(self):
        eb = EntitlementBlock()
        result = eb.to_list(include_all_tokens=True)
        assert result == []


# ===========================================================================
# Entitlements
# ===========================================================================

class TestEntitlements:
    def test_init_defaults(self):
        ent = Entitlements()
        assert isinstance(ent.admin, EntitlementBlock)
        assert ent.admin.is_empty()

    def test_init_with_blocks(self):
        admin_block = EntitlementBlock(roles=['admin'])
        ent = Entitlements(admin=admin_block)
        assert ent.admin.roles == ['admin']

    def test_all_properties(self):
        ent = Entitlements()
        for prop in ['admin', 'delete', 'display', 'upload', 'edit',
                     'execute', 'plot', 'query', 'rebalance', 'trade', 'view']:
            block = getattr(ent, prop)
            assert isinstance(block, EntitlementBlock)

    def test_all_setters(self):
        ent = Entitlements()
        for prop in ['admin', 'delete', 'display', 'upload', 'edit',
                     'execute', 'plot', 'query', 'rebalance', 'trade', 'view']:
            new_block = EntitlementBlock(roles=[f'{prop}_role'])
            setattr(ent, prop, new_block)
            assert getattr(ent, prop).roles == [f'{prop}_role']

    def test_eq_true(self):
        ent1 = Entitlements()
        ent2 = Entitlements()
        assert ent1 == ent2

    def test_eq_false_different(self):
        ent1 = Entitlements(admin=EntitlementBlock(roles=['a']))
        ent2 = Entitlements(admin=EntitlementBlock(roles=['b']))
        assert ent1 != ent2

    def test_eq_false_not_entitlements(self):
        ent = Entitlements()
        assert ent != "not entitlements"

    def test_to_target_empty(self):
        ent = Entitlements()
        target = ent.to_target()
        assert isinstance(target, TargetEntitlements)

    def test_to_target_with_all_blocks(self):
        u = _fake_user('u1')
        g = _fake_group('g1')
        blocks = {}
        for prop in ['admin', 'delete', 'display', 'upload', 'edit',
                     'execute', 'plot', 'query', 'rebalance', 'trade', 'view']:
            blocks[prop] = EntitlementBlock(users=[u], groups=[g], roles=[f'{prop}_role'])
        ent = Entitlements(**blocks)
        target = ent.to_target()
        for prop in blocks:
            target_val = getattr(target, prop)
            assert target_val is not None
            assert len(target_val) > 0

    def test_to_target_include_all_tokens(self):
        eb = EntitlementBlock(roles=['r1'], unconverted_tokens=['unknown:tok'])
        ent = Entitlements(admin=eb)
        target = ent.to_target(include_all_tokens=True)
        assert 'unknown:tok' in target.admin

    def test_to_target_without_all_tokens(self):
        eb = EntitlementBlock(roles=['r1'], unconverted_tokens=['unknown:tok'])
        ent = Entitlements(admin=eb)
        target = ent.to_target(include_all_tokens=False)
        assert 'unknown:tok' not in target.admin

    def test_to_dict(self):
        ent = Entitlements(edit=EntitlementBlock(roles=['editor']))
        d = ent.to_dict()
        assert 'edit' in d

    def test_to_frame(self):
        u = _fake_user('u1', name='User1')
        g = _fake_group('g1', name='Group1')
        ent = Entitlements(
            admin=EntitlementBlock(users=[u]),
            edit=EntitlementBlock(groups=[g]),
            view=EntitlementBlock(roles=['viewer']),
        )
        df = ent.to_frame()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'action' in df.columns
        assert 'type' in df.columns

    def test_to_frame_empty(self):
        ent = Entitlements()
        df = ent.to_frame()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @patch('gs_quant.entities.entitlements.User.get_many')
    @patch('gs_quant.entities.entitlements.Group.get_many')
    def test_from_target(self, mock_get_groups, mock_get_users):
        mock_get_users.return_value = [_fake_user('u1')]
        mock_get_groups.return_value = [_fake_group('g1')]
        target = TargetEntitlements(edit=('guid:u1', 'group:g1', 'role:admin'))
        ent = Entitlements.from_target(target)
        assert len(ent.edit.users) == 1
        assert len(ent.edit.groups) == 1
        assert ent.edit.roles == ['admin']

    @patch('gs_quant.entities.entitlements.User.get_many')
    @patch('gs_quant.entities.entitlements.Group.get_many')
    def test_from_target_none(self, mock_get_groups, mock_get_users):
        mock_get_users.return_value = []
        mock_get_groups.return_value = []
        ent = Entitlements.from_target(None)
        # Should use default_instance
        assert isinstance(ent, Entitlements)

    @patch('gs_quant.entities.entitlements.User.get_many')
    @patch('gs_quant.entities.entitlements.Group.get_many')
    def test_from_dict(self, mock_get_groups, mock_get_users):
        mock_get_users.return_value = [_fake_user('u1')]
        mock_get_groups.return_value = [_fake_group('g1')]
        d = {'edit': ['guid:u1', 'group:g1', 'role:admin']}
        ent = Entitlements.from_dict(d)
        assert len(ent.edit.users) == 1
        assert len(ent.edit.groups) == 1
        assert ent.edit.roles == ['admin']

    @patch('gs_quant.entities.entitlements.User.get_many')
    @patch('gs_quant.entities.entitlements.Group.get_many')
    def test_from_dict_with_unconverted_tokens(self, mock_get_groups, mock_get_users):
        mock_get_users.return_value = []
        mock_get_groups.return_value = []
        d = {'edit': ['unknown:token1', 'another:token2']}
        ent = Entitlements.from_dict(d)
        assert ent.edit.unconverted_tokens == ['unknown:token1', 'another:token2']

    @patch('gs_quant.entities.entitlements.User.get_many')
    @patch('gs_quant.entities.entitlements.Group.get_many')
    def test_from_dict_empty(self, mock_get_groups, mock_get_users):
        mock_get_users.return_value = []
        mock_get_groups.return_value = []
        d = {'edit': []}
        ent = Entitlements.from_dict(d)
        # Empty token_set should produce empty EntitlementBlock, not added to kwargs
        assert ent.edit.is_empty()

    @patch('gs_quant.entities.entitlements.User.get_many')
    @patch('gs_quant.entities.entitlements.Group.get_many')
    def test_from_dict_multiple_actions(self, mock_get_groups, mock_get_users):
        mock_get_users.return_value = [_fake_user('u1')]
        mock_get_groups.return_value = []
        d = {
            'admin': ['guid:u1'],
            'view': ['role:viewer'],
        }
        ent = Entitlements.from_dict(d)
        assert len(ent.admin.users) == 1
        assert ent.view.roles == ['viewer']

    @patch('gs_quant.entities.entitlements.User.get_many')
    @patch('gs_quant.entities.entitlements.Group.get_many')
    def test_from_dict_user_not_resolved(self, mock_get_groups, mock_get_users):
        """guid: token that doesn't match any returned user becomes unconverted."""
        mock_get_users.return_value = []  # no users returned
        mock_get_groups.return_value = []
        d = {'edit': ['guid:missing_user']}
        ent = Entitlements.from_dict(d)
        # The token is in user_ids set but not in token_map
        assert ent.edit.unconverted_tokens == ['guid:missing_user']

    @patch('gs_quant.entities.entitlements.User.get_many')
    @patch('gs_quant.entities.entitlements.Group.get_many')
    def test_from_dict_group_not_resolved(self, mock_get_groups, mock_get_users):
        """group: token that doesn't match becomes unconverted."""
        mock_get_users.return_value = []
        mock_get_groups.return_value = []
        d = {'edit': ['group:missing_group']}
        ent = Entitlements.from_dict(d)
        assert ent.edit.unconverted_tokens == ['group:missing_group']

    @patch('gs_quant.entities.entitlements.User.get_many')
    @patch('gs_quant.entities.entitlements.Group.get_many')
    def test_from_dict_only_roles(self, mock_get_groups, mock_get_users):
        mock_get_users.return_value = []
        mock_get_groups.return_value = []
        d = {'view': ['role:viewer', 'role:editor']}
        ent = Entitlements.from_dict(d)
        assert 'viewer' in ent.view.roles
        assert 'editor' in ent.view.roles
