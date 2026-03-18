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

from gs_quant.interfaces.algebra import AlgebraicType


class ConcreteAlgebraic(AlgebraicType):
    def __init__(self, value):
        self.value = value

    def __add__(self, other):
        if isinstance(other, ConcreteAlgebraic):
            return ConcreteAlgebraic(self.value + other.value)
        return ConcreteAlgebraic(self.value + other)

    def __sub__(self, other):
        return ConcreteAlgebraic(self.value - other.value)

    def __mul__(self, other):
        if isinstance(other, ConcreteAlgebraic):
            return ConcreteAlgebraic(self.value * other.value)
        return ConcreteAlgebraic(self.value * other)

    def __div__(self, other):
        return ConcreteAlgebraic(self.value / other.value)


class TestAlgebraicType:
    def test_radd(self):
        a = ConcreteAlgebraic(3)
        result = 5 + a  # triggers __radd__
        assert result.value == 8

    def test_rmul(self):
        a = ConcreteAlgebraic(3)
        result = 5 * a  # triggers __rmul__
        assert result.value == 15

    def test_add(self):
        a = ConcreteAlgebraic(3)
        b = ConcreteAlgebraic(4)
        result = a + b
        assert result.value == 7

    def test_sub(self):
        a = ConcreteAlgebraic(10)
        b = ConcreteAlgebraic(4)
        result = a - b
        assert result.value == 6

    def test_mul(self):
        a = ConcreteAlgebraic(3)
        b = ConcreteAlgebraic(4)
        result = a * b
        assert result.value == 12

    def test_div(self):
        a = ConcreteAlgebraic(12)
        b = ConcreteAlgebraic(4)
        result = a.__div__(b)
        assert result.value == 3
