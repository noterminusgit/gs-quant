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

import datetime as dt

import pandas as pd

from gs_quant.risk.scenarios import MarketDataShockBasedScenario, MarketDataVolShockScenario
from gs_quant.target.risk import MarketDataPattern, MarketDataShock, MarketDataShockType


class TestMarketDataShockBasedScenario:
    def test_construct_with_mapping(self):
        pattern1 = MarketDataPattern('IR', 'USD')
        shock1 = MarketDataShock(MarketDataShockType.Absolute, 0.01)

        pattern2 = MarketDataPattern('IR', 'EUR')
        shock2 = MarketDataShock(MarketDataShockType.Proportional, 0.05)

        shocks = {pattern1: shock1, pattern2: shock2}
        scenario = MarketDataShockBasedScenario(shocks)

        assert scenario.shocks is not None
        assert len(scenario.shocks) == 2
        assert scenario.shocks[0].pattern == pattern1
        assert scenario.shocks[0].shock == shock1
        assert scenario.shocks[1].pattern == pattern2
        assert scenario.shocks[1].shock == shock2

    def test_construct_with_name(self):
        pattern = MarketDataPattern('IR', 'USD')
        shock = MarketDataShock(MarketDataShockType.Absolute, 0.01)

        scenario = MarketDataShockBasedScenario({pattern: shock}, name='TestScenario')
        assert scenario.name == 'TestScenario'

    def test_construct_empty_mapping(self):
        scenario = MarketDataShockBasedScenario({})
        assert scenario.shocks is not None
        assert len(scenario.shocks) == 0


class TestMarketDataVolShockScenario:
    def test_from_dataframe(self):
        timestamps = [
            dt.datetime(2020, 6, 1, 10, 0, 0),
            dt.datetime(2020, 6, 1, 10, 0, 0),
            dt.datetime(2020, 6, 1, 10, 0, 0),
        ]
        data = {
            'expirationDate': [
                dt.datetime(2020, 9, 18),
                dt.datetime(2020, 9, 18),
                dt.datetime(2020, 12, 18),
            ],
            'absoluteStrike': [100.0, 110.0, 100.0],
            'impliedVolatility': [0.20, 0.18, 0.22],
        }
        df = pd.DataFrame(data, index=timestamps)

        scenario = MarketDataVolShockScenario.from_dataframe(
            asset_ric='.SPX',
            df=df,
            ref_spot=3000.0,
            name='VolShockTest',
        )

        assert scenario is not None
        assert scenario.name == 'VolShockTest'
        assert scenario.ref_spot == 3000.0
        assert scenario.pattern == MarketDataPattern('Eq Vol', '.SPX')
        assert scenario.shock_type == MarketDataShockType.Override
        assert len(scenario.vol_levels) == 2

    def test_from_dataframe_single_expiry(self):
        timestamps = [
            dt.datetime(2020, 6, 1, 10, 0, 0),
            dt.datetime(2020, 6, 1, 10, 0, 0),
        ]
        data = {
            'expirationDate': [
                dt.datetime(2020, 9, 18),
                dt.datetime(2020, 9, 18),
            ],
            'absoluteStrike': [90.0, 100.0],
            'impliedVolatility': [0.25, 0.20],
        }
        df = pd.DataFrame(data, index=timestamps)

        scenario = MarketDataVolShockScenario.from_dataframe(
            asset_ric='.STOXX50E',
            df=df,
        )

        assert len(scenario.vol_levels) == 1
        vol_slice = scenario.vol_levels[0]
        assert list(vol_slice.strikes) == [90.0, 100.0]
        assert list(vol_slice.levels) == [0.25, 0.20]

    def test_from_dataframe_filters_to_last_datetime(self):
        timestamps = [
            dt.datetime(2020, 6, 1, 9, 0, 0),
            dt.datetime(2020, 6, 1, 10, 0, 0),
            dt.datetime(2020, 6, 1, 10, 0, 0),
        ]
        data = {
            'expirationDate': [
                dt.datetime(2020, 9, 18),
                dt.datetime(2020, 9, 18),
                dt.datetime(2020, 9, 18),
            ],
            'absoluteStrike': [100.0, 100.0, 110.0],
            'impliedVolatility': [0.30, 0.20, 0.18],
        }
        df = pd.DataFrame(data, index=timestamps)

        scenario = MarketDataVolShockScenario.from_dataframe(
            asset_ric='.SPX',
            df=df,
        )

        assert len(scenario.vol_levels) == 1
        vol_slice = scenario.vol_levels[0]
        assert list(vol_slice.strikes) == [100.0, 110.0]
        assert list(vol_slice.levels) == [0.20, 0.18]
