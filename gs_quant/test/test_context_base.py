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

import pytest

from gs_quant.context_base import ContextBase, ContextBaseWithDefault, ContextMeta, nullcontext
from gs_quant.errors import MqUninitialisedError, MqValueError


class MyContext(ContextBase):
    pass


class MyDefaultContext(ContextBaseWithDefault):
    pass


class TestContextMeta:
    def setup_method(self):
        # Clean up thread-local state between tests
        import threading
        from gs_quant.context_base import thread_local
        for attr in list(vars(thread_local)):
            if 'MyContext' in attr or 'MyDefaultContext' in attr:
                delattr(thread_local, attr)

    def test_default_value_is_none(self):
        assert ContextMeta.default_value() is None

    def test_current_raises_when_uninitialised(self):
        with pytest.raises(MqUninitialisedError):
            _ = MyContext.current

    def test_path_empty_initially(self):
        assert MyContext.path == ()

    def test_current_is_set_false_initially(self):
        assert not MyContext.current_is_set

    def test_push_and_pop(self):
        ctx = MyContext()
        MyContext.push(ctx)
        assert MyContext.current is ctx
        assert MyContext.current_is_set
        popped = MyContext.pop()
        assert popped is ctx

    def test_prior(self):
        ctx1 = MyContext()
        ctx2 = MyContext()
        MyContext.push(ctx1)
        MyContext.push(ctx2)
        assert MyContext.current is ctx2
        assert MyContext.prior is ctx1
        assert MyContext.has_prior
        MyContext.pop()
        MyContext.pop()

    def test_prior_raises_when_no_prior(self):
        ctx = MyContext()
        MyContext.push(ctx)
        with pytest.raises(MqValueError, match='has no prior'):
            _ = MyContext.prior
        MyContext.pop()

    def test_has_prior_false_with_single(self):
        ctx = MyContext()
        MyContext.push(ctx)
        assert not MyContext.has_prior
        MyContext.pop()

    def test_set_current(self):
        ctx = MyContext()
        MyContext.current = ctx
        assert MyContext.current is ctx
        # Clean up
        MyContext.pop()

    def test_set_current_raises_when_nested(self):
        ctx1 = MyContext()
        ctx2 = MyContext()
        MyContext.push(ctx1)
        MyContext.push(ctx2)
        with pytest.raises(MqValueError, match='Cannot set current while in a nested context'):
            MyContext.current = MyContext()
        MyContext.pop()
        MyContext.pop()

    def test_set_current_raises_when_entered(self):
        ctx = MyContext()
        with ctx:
            with pytest.raises(MqValueError, match='Cannot set current while in a nested context'):
                MyContext.current = MyContext()


class TestContextBase:
    def setup_method(self):
        import threading
        from gs_quant.context_base import thread_local
        for attr in list(vars(thread_local)):
            if 'MyContext' in attr or 'MyDefaultContext' in attr:
                delattr(thread_local, attr)

    def test_enter_exit(self):
        ctx = MyContext()
        assert not ctx.is_entered
        with ctx:
            assert ctx.is_entered
            assert MyContext.current is ctx
        assert not ctx.is_entered

    def test_nested_enter_exit(self):
        ctx1 = MyContext()
        ctx2 = MyContext()
        with ctx1:
            with ctx2:
                assert MyContext.current is ctx2
                assert MyContext.prior is ctx1
            assert MyContext.current is ctx1

    def test_exit_called_on_exception(self):
        ctx = MyContext()
        with pytest.raises(ValueError):
            with ctx:
                raise ValueError('test')
        assert not ctx.is_entered

    @pytest.mark.asyncio
    async def test_async_enter_exit(self):
        ctx = MyContext()
        assert not ctx.is_entered
        async with ctx:
            assert ctx.is_entered
            assert MyContext.current is ctx
        assert not ctx.is_entered

    @pytest.mark.asyncio
    async def test_async_exit_on_exception(self):
        ctx = MyContext()
        with pytest.raises(ValueError):
            async with ctx:
                raise ValueError('test')
        assert not ctx.is_entered


class TestContextBaseWithDefault:
    def setup_method(self):
        import threading
        from gs_quant.context_base import thread_local
        for attr in list(vars(thread_local)):
            if 'MyDefaultContext' in attr:
                delattr(thread_local, attr)

    def test_default_value(self):
        assert MyDefaultContext.default_value() is not None
        assert isinstance(MyDefaultContext.default_value(), MyDefaultContext)

    def test_current_returns_default(self):
        current = MyDefaultContext.current
        assert isinstance(current, MyDefaultContext)
        assert MyDefaultContext.current_is_set


class TestNullcontext:
    def test_basic(self):
        with nullcontext() as val:
            assert val is None

    def test_with_enter_result(self):
        with nullcontext(42) as val:
            assert val == 42


# ---------------------------------------------------------------------------
# Phase 6 – additional branch-coverage tests
# ---------------------------------------------------------------------------


class TestContextMetaSetCurrentWithPathLen1NotEntered:
    """Cover branch where len(path)==1 and cur.is_entered is False."""

    def setup_method(self):
        import threading
        from gs_quant.context_base import thread_local
        for attr in list(vars(thread_local)):
            if 'MyContext' in attr or 'MyDefaultContext' in attr:
                delattr(thread_local, attr)

    def test_set_current_when_existing_not_entered(self):
        """When path has exactly one element and that element is not entered,
        setting current should succeed (line 62 is_entered is False)."""
        ctx1 = MyContext()
        MyContext.current = ctx1  # Sets path to (ctx1,)
        ctx2 = MyContext()
        # ctx1 is NOT entered (no `with` block), so is_entered = False
        # This should succeed without raising
        MyContext.current = ctx2
        assert MyContext.current is ctx2
        MyContext.pop()

    def test_set_current_on_object_without_is_entered(self):
        """When current doesn't have is_entered attribute -> AttributeError caught (line 64)."""
        # Create a context whose current is a non-ContextBase object
        # by directly manipulating the thread_local path
        from gs_quant.context_base import thread_local
        key = '{}_path'.format(MyContext.__name__)
        sentinel = object()  # doesn't have is_entered
        setattr(thread_local, key, (sentinel,))

        # Setting current should catch AttributeError on sentinel.is_entered
        ctx = MyContext()
        MyContext.current = ctx
        assert MyContext.current is ctx
        MyContext.pop()


class TestContextBaseCls:
    """Cover branch where _cls returns self.__class__ (no ContextBase in bases)."""

    def setup_method(self):
        import threading
        from gs_quant.context_base import thread_local
        for attr in list(vars(thread_local)):
            if 'MyContext' in attr or 'DeepChild' in attr:
                delattr(thread_local, attr)

    def test_cls_with_deep_hierarchy(self):
        """Test _cls property with multi-level inheritance."""

        class DeepChild(MyContext):
            pass

        dc = DeepChild()
        # _cls should resolve to MyContext (which has ContextBase in its __bases__)
        assert dc._cls is MyContext
