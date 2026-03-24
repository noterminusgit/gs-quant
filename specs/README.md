# Module Specifications for Elixir Port

Comprehensive specs for all 172 production files in gs-quant. Each spec documents types, function signatures, branch logic, state mutation, error handling, and dependencies — everything needed to port to Elixir.

## Format

Specs use the [enhanced template](TEMPLATE.md) with Elixir-porting sections:
- **Summary** — module purpose
- **Dependencies** — internal module paths + external packages
- **Type Definitions** — all classes/dataclasses with typed field tables
- **Enums/Constants** — all enum values and constant definitions
- **Functions/Methods** — typed signatures, branch logic, algorithms
- **State Mutation** — mutable state and when it changes
- **Error Handling** — exception types and conditions
- **Coverage Notes** — branch count, missing branches

For the recommended reading order, see [DEPENDENCY_GRAPH.md](DEPENDENCY_GRAPH.md).

## File Inventory (172 specs)

### Root (9 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/base.py` | [root/base.md](root/base.md) | 729 |
| `gs_quant/session.py` | [root/session.md](root/session.md) | 1307 |
| `gs_quant/common.py` | [root/common.md](root/common.md) | 116 |
| `gs_quant/context_base.py` | [root/context_base.md](root/context_base.md) | 190 |
| `gs_quant/errors.py` | [root/errors.md](root/errors.md) | 89 |
| `gs_quant/priceable.py` | [root/priceable.md](root/priceable.md) | 140 |
| `gs_quant/json_convertors.py` | [root/json_convertors.md](root/json_convertors.md) | 349 |
| `gs_quant/json_convertors_common.py` | [root/json_convertors_common.md](root/json_convertors_common.md) | 88 |
| `gs_quant/json_encoder.py` | [root/json_encoder.md](root/json_encoder.md) | 44 |

### Data (8 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/data/dataset.py` | [data/dataset.md](data/dataset.md) | 933 |
| `gs_quant/data/coordinate.py` | [data/coordinate.md](data/coordinate.md) | 257 |
| `gs_quant/data/utilities.py` | [data/utilities.md](data/utilities.md) | 457 |
| `gs_quant/data/core.py` | [data/core.md](data/core.md) | 132 |
| `gs_quant/data/fields.py` | [data/fields.md](data/fields.md) | 164 |
| `gs_quant/data/query.py` | [data/query.md](data/query.md) | 63 |
| `gs_quant/data/log.py` | [data/log.md](data/log.md) | 29 |
| `gs_quant/data/stream.py` | [data/stream.md](data/stream.md) | 39 |

### DateTime (6 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/datetime/date.py` | [datetime/date.md](datetime/date.md) | 318 |
| `gs_quant/datetime/point.py` | [datetime/point.md](datetime/point.md) | 325 |
| `gs_quant/datetime/relative_date.py` | [datetime/relative_date.md](datetime/relative_date.md) | 274 |
| `gs_quant/datetime/rules.py` | [datetime/rules.md](datetime/rules.md) | 284 |
| `gs_quant/datetime/gscalendar.py` | [datetime/gscalendar.md](datetime/gscalendar.md) | 128 |
| `gs_quant/datetime/time.py` | [datetime/time.md](datetime/time.md) | 91 |

### Risk (7 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/risk/core.py` | [risk/core.md](risk/core.md) | 685 |
| `gs_quant/risk/results.py` | [risk/results.md](risk/results.md) | 972 |
| `gs_quant/risk/result_handlers.py` | [risk/result_handlers.md](risk/result_handlers.md) | 516 |
| `gs_quant/risk/measures.py` | [risk/measures.md](risk/measures.md) | 85 |
| `gs_quant/risk/scenarios.py` | [risk/scenarios.md](risk/scenarios.md) | 65 |
| `gs_quant/risk/scenario_utils.py` | [risk/scenario_utils.md](risk/scenario_utils.md) | 54 |
| `gs_quant/risk/transform.py` | [risk/transform.md](risk/transform.md) | 76 |

### Entities (4 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/entities/entity.py` | [entities/entity.md](entities/entity.md) | 1144 |
| `gs_quant/entities/entitlements.py` | [entities/entitlements.md](entities/entitlements.md) | 622 |
| `gs_quant/entities/entity_utils.py` | [entities/entity_utils.md](entities/entity_utils.md) | 56 |
| `gs_quant/entities/tree_entity.py` | [entities/tree_entity.md](entities/tree_entity.md) | 229 |

### Markets (19 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/markets/optimizer.py` | [markets/optimizer.md](markets/optimizer.md) | 2286 |
| `gs_quant/markets/securities.py` | [markets/securities.md](markets/securities.md) | 2163 |
| `gs_quant/markets/report.py` | [markets/report.md](markets/report.md) | 1866 |
| `gs_quant/markets/position_set.py` | [markets/position_set.md](markets/position_set.md) | 1618 |
| `gs_quant/markets/baskets.py` | [markets/baskets.md](markets/baskets.md) | 1267 |
| `gs_quant/markets/hedge.py` | [markets/hedge.md](markets/hedge.md) | 1041 |
| `gs_quant/markets/core.py` | [markets/core.md](markets/core.md) | 705 |
| `gs_quant/markets/portfolio.py` | [markets/portfolio.md](markets/portfolio.md) | 649 |
| `gs_quant/markets/portfolio_manager.py` | [markets/portfolio_manager.md](markets/portfolio_manager.md) | 709 |
| `gs_quant/markets/index.py` | [markets/index.md](markets/index.md) | 565 |
| `gs_quant/markets/indices_utils.py` | [markets/indices_utils.md](markets/indices_utils.md) | 610 |
| `gs_quant/markets/factor_analytics.py` | [markets/factor_analytics.md](markets/factor_analytics.md) | 522 |
| `gs_quant/markets/screens.py` | [markets/screens.md](markets/screens.md) | 495 |
| `gs_quant/markets/scenario.py` | [markets/scenario.md](markets/scenario.md) | 477 |
| `gs_quant/markets/markets.py` | [markets/markets.md](markets/markets.md) | 341 |
| `gs_quant/markets/factor.py` | [markets/factor.md](markets/factor.md) | 307 |
| `gs_quant/markets/portfolio_manager_utils.py` | [markets/portfolio_manager_utils.md](markets/portfolio_manager_utils.md) | 289 |
| `gs_quant/markets/historical.py` | [markets/historical.md](markets/historical.md) | 231 |
| `gs_quant/markets/position_set_utils.py` | [markets/position_set_utils.md](markets/position_set_utils.md) | 149 |
| `gs_quant/markets/report_utils.py` | [markets/report_utils.md](markets/report_utils.md) | 46 |

### Timeseries (21 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/timeseries/measures.py` | [timeseries/measures.md](timeseries/measures.md) | 6080 |
| `gs_quant/timeseries/measures_rates.py` | [timeseries/measures_rates.md](timeseries/measures_rates.md) | 2840 |
| `gs_quant/timeseries/measures_reports.py` | [timeseries/measures_reports.md](timeseries/measures_reports.md) | 1737 |
| `gs_quant/timeseries/statistics.py` | [timeseries/statistics.md](timeseries/statistics.md) | 1643 |
| `gs_quant/timeseries/measures_factset.py` | [timeseries/measures_factset.md](timeseries/measures_factset.md) | 1337 |
| `gs_quant/timeseries/measures_portfolios.py` | [timeseries/measures_portfolios.md](timeseries/measures_portfolios.md) | 1166 |
| `gs_quant/timeseries/econometrics.py` | [timeseries/econometrics.md](timeseries/econometrics.md) | 1093 |
| `gs_quant/timeseries/algebra.py` | [timeseries/algebra.md](timeseries/algebra.md) | 835 |
| `gs_quant/timeseries/datetime.py` | [timeseries/datetime.md](timeseries/datetime.md) | 791 |
| `gs_quant/timeseries/measures_fx_vol.py` | [timeseries/measures_fx_vol.md](timeseries/measures_fx_vol.md) | 777 |
| `gs_quant/timeseries/measures_xccy.py` | [timeseries/measures_xccy.md](timeseries/measures_xccy.md) | 611 |
| `gs_quant/timeseries/technicals.py` | [timeseries/technicals.md](timeseries/technicals.md) | 570 |
| `gs_quant/timeseries/backtesting.py` | [timeseries/backtesting.md](timeseries/backtesting.md) | 521 |
| `gs_quant/timeseries/helper.py` | [timeseries/helper.md](timeseries/helper.md) | 429 |
| `gs_quant/timeseries/measures_inflation.py` | [timeseries/measures_inflation.md](timeseries/measures_inflation.md) | 418 |
| `gs_quant/timeseries/analysis.py` | [timeseries/analysis.md](timeseries/analysis.md) | 407 |
| `gs_quant/timeseries/measures_risk_models.py` | [timeseries/measures_risk_models.md](timeseries/measures_risk_models.md) | 390 |
| `gs_quant/timeseries/measures_countries.py` | [timeseries/measures_countries.md](timeseries/measures_countries.md) | 87 |
| `gs_quant/timeseries/measure_registry.py` | [timeseries/measure_registry.md](timeseries/measure_registry.md) | 66 |
| `gs_quant/timeseries/tca.py` | [timeseries/tca.md](timeseries/tca.md) | 65 |
| `gs_quant/timeseries/measures_helper.py` | [timeseries/measures_helper.md](timeseries/measures_helper.md) | 55 |

### Models (3 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/models/risk_model.py` | [models/risk_model.md](models/risk_model.md) | 3316 |
| `gs_quant/models/epidemiology.py` | [models/epidemiology.md](models/epidemiology.md) | 652 |
| `gs_quant/models/risk_model_utils.py` | [models/risk_model_utils.md](models/risk_model_utils.md) | 517 |

### Analytics (20 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/analytics/core/processor.py` | [analytics/core/processor.md](analytics/core/processor.md) | 348 |
| `gs_quant/analytics/core/processor_result.py` | [analytics/core/processor_result.md](analytics/core/processor_result.md) | 103 |
| `gs_quant/analytics/core/query_helpers.py` | [analytics/core/query_helpers.md](analytics/core/query_helpers.md) | 143 |
| `gs_quant/analytics/common/constants.py` | [analytics/common/constants.md](analytics/common/constants.md) | 28 |
| `gs_quant/analytics/common/enumerators.py` | [analytics/common/enumerators.md](analytics/common/enumerators.md) | 35 |
| `gs_quant/analytics/common/helpers.py` | [analytics/common/helpers.md](analytics/common/helpers.md) | 148 |
| `gs_quant/analytics/datagrid/datagrid.py` | [analytics/datagrid/datagrid.md](analytics/datagrid/datagrid.md) | 757 |
| `gs_quant/analytics/datagrid/data_cell.py` | [analytics/datagrid/data_cell.md](analytics/datagrid/data_cell.md) | 105 |
| `gs_quant/analytics/datagrid/data_column.py` | [analytics/datagrid/data_column.md](analytics/datagrid/data_column.md) | 147 |
| `gs_quant/analytics/datagrid/data_row.py` | [analytics/datagrid/data_row.md](analytics/datagrid/data_row.md) | 58 |
| `gs_quant/analytics/datagrid/serializers.py` | [analytics/datagrid/serializers.md](analytics/datagrid/serializers.md) | 132 |
| `gs_quant/analytics/datagrid/utils.py` | [analytics/datagrid/utils.md](analytics/datagrid/utils.md) | 85 |
| `gs_quant/analytics/processors/analysis_processors.py` | [analytics/processors/analysis_processors.md](analytics/processors/analysis_processors.md) | 286 |
| `gs_quant/analytics/processors/econometrics_processors.py` | [analytics/processors/econometrics_processors.md](analytics/processors/econometrics_processors.md) | 90 |
| `gs_quant/analytics/processors/scale_processors.py` | [analytics/processors/scale_processors.md](analytics/processors/scale_processors.md) | 50 |
| `gs_quant/analytics/processors/special_processors.py` | [analytics/processors/special_processors.md](analytics/processors/special_processors.md) | 343 |
| `gs_quant/analytics/processors/statistics_processors.py` | [analytics/processors/statistics_processors.md](analytics/processors/statistics_processors.md) | 249 |
| `gs_quant/analytics/processors/utility_processors.py` | [analytics/processors/utility_processors.md](analytics/processors/utility_processors.md) | 316 |
| `gs_quant/analytics/workspaces/components.py` | [analytics/workspaces/components.md](analytics/workspaces/components.md) | 264 |
| `gs_quant/analytics/workspaces/workspace.py` | [analytics/workspaces/workspace.md](analytics/workspaces/workspace.md) | 113 |

### Backtests (18 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/backtests/generic_engine.py` | [backtests/generic_engine.md](backtests/generic_engine.md) | 1292 |
| `gs_quant/backtests/actions.py` | [backtests/actions.md](backtests/actions.md) | 664 |
| `gs_quant/backtests/backtest_objects.py` | [backtests/backtest_objects.md](backtests/backtest_objects.md) | 574 |
| `gs_quant/backtests/triggers.py` | [backtests/triggers.md](backtests/triggers.md) | 432 |
| `gs_quant/backtests/strategy.py` | [backtests/strategy.md](backtests/strategy.md) | 305 |
| `gs_quant/backtests/strategy_systematic.py` | [backtests/strategy_systematic.md](backtests/strategy_systematic.md) | 266 |
| `gs_quant/backtests/backtest_engine.py` | [backtests/backtest_engine.md](backtests/backtest_engine.md) | 256 |
| `gs_quant/backtests/predefined_asset_engine.py` | [backtests/predefined_asset_engine.md](backtests/predefined_asset_engine.md) | 235 |
| `gs_quant/backtests/equity_vol_engine.py` | [backtests/equity_vol_engine.md](backtests/equity_vol_engine.md) | 231 |
| `gs_quant/backtests/backtest_utils.py` | [backtests/backtest_utils.md](backtests/backtest_utils.md) | 214 |
| `gs_quant/backtests/generic_engine_action_impls.py` | [backtests/generic_engine_action_impls.md](backtests/generic_engine_action_impls.md) | 204 |
| `gs_quant/backtests/data_handler.py` | [backtests/data_handler.md](backtests/data_handler.md) | 184 |
| `gs_quant/backtests/core.py` | [backtests/core.md](backtests/core.md) | 159 |
| `gs_quant/backtests/data_sources.py` | [backtests/data_sources.md](backtests/data_sources.md) | 148 |
| `gs_quant/backtests/execution_engine.py` | [backtests/execution_engine.md](backtests/execution_engine.md) | 113 |
| `gs_quant/backtests/action_handler.py` | [backtests/action_handler.md](backtests/action_handler.md) | 83 |
| `gs_quant/backtests/decorator.py` | [backtests/decorator.md](backtests/decorator.md) | 42 |
| `gs_quant/backtests/event.py` | [backtests/event.md](backtests/event.md) | 39 |
| `gs_quant/backtests/order.py` | [backtests/order.md](backtests/order.md) | 36 |

### API (38 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/api/gs/data.py` | [api/gs/data.md](api/gs/data.md) | 1522 |
| `gs_quant/api/gs/secmaster.py` | [api/gs/secmaster.md](api/gs/secmaster.md) | 889 |
| `gs_quant/api/gs/assets.py` | [api/gs/assets.md](api/gs/assets.md) | 625 |
| `gs_quant/api/gs/risk.py` | [api/gs/risk.md](api/gs/risk.md) | 459 |
| `gs_quant/api/gs/portfolios.py` | [api/gs/portfolios.md](api/gs/portfolios.md) | 397 |
| `gs_quant/api/risk.py` | [api/risk.md](api/risk.md) | 381 |
| `gs_quant/api/gs/hedges.py` | [api/gs/hedges.md](api/gs/hedges.md) | 342 |
| `gs_quant/api/gs/reports.py` | [api/gs/reports.md](api/gs/reports.md) | 296 |
| `gs_quant/api/gs/risk_models.py` | [api/gs/risk_models.md](api/gs/risk_models.md) | 278 |
| `gs_quant/api/gs/federated_secmaster.py` | [api/gs/federated_secmaster.md](api/gs/federated_secmaster.md) | 255 |
| `gs_quant/api/gs/indices.py` | [api/gs/indices.md](api/gs/indices.md) | 230 |
| `gs_quant/api/gs/backtests.py` | [api/gs/backtests.md](api/gs/backtests.md) | 200 |
| `gs_quant/api/gs/scenarios.py` | [api/gs/scenarios.md](api/gs/scenarios.md) | 168 |
| `gs_quant/api/gs/monitors.py` | [api/gs/monitors.md](api/gs/monitors.md) | 160 |
| `gs_quant/api/gs/screens.py` | [api/gs/screens.md](api/gs/screens.md) | 153 |
| `gs_quant/api/gs/datagrid.py` | [api/gs/datagrid.md](api/gs/datagrid.md) | 142 |
| `gs_quant/api/gs/users.py` | [api/gs/users.md](api/gs/users.md) | 134 |
| `gs_quant/api/gs/content.py` | [api/gs/content.md](api/gs/content.md) | 126 |
| `gs_quant/api/gs/esg.py` | [api/gs/esg.md](api/gs/esg.md) | 102 |
| `gs_quant/api/gs/groups.py` | [api/gs/groups.md](api/gs/groups.md) | 97 |
| `gs_quant/api/gs/plots.py` | [api/gs/plots.md](api/gs/plots.md) | 93 |
| `gs_quant/api/gs/carbon.py` | [api/gs/carbon.md](api/gs/carbon.md) | 92 |
| `gs_quant/api/gs/thematics.py` | [api/gs/thematics.md](api/gs/thematics.md) | 88 |
| `gs_quant/api/gs/price.py` | [api/gs/price.md](api/gs/price.md) | 75 |
| `gs_quant/api/gs/base_screener.py` | [api/gs/base_screener.md](api/gs/base_screener.md) | 68 |
| `gs_quant/api/gs/workspaces.py` | [api/gs/workspaces.md](api/gs/workspaces.md) | 63 |
| `gs_quant/api/gs/countries.py` | [api/gs/countries.md](api/gs/countries.md) | 50 |
| `gs_quant/api/gs/parser.py` | [api/gs/parser.md](api/gs/parser.md) | 44 |
| `gs_quant/api/gs/data_screen.py` | [api/gs/data_screen.md](api/gs/data_screen.md) | 36 |
| `gs_quant/api/utils.py` | [api/utils.md](api/utils.md) | 77 |
| `gs_quant/api/api_cache.py` | [api/api_cache.md](api/api_cache.md) | 42 |
| `gs_quant/api/api_session.py` | [api/api_session.md](api/api_session.md) | 28 |
| `gs_quant/api/fred/data.py` | [api/fred/data.md](api/fred/data.md) | 169 |
| `gs_quant/api/fred/fred_query.py` | [api/fred/fred_query.md](api/fred/fred_query.md) | 104 |
| `gs_quant/api/gs/backtests_xasset/apis.py` | [api/gs/backtests_xasset/apis.md](api/gs/backtests_xasset/apis.md) | 242 |
| `gs_quant/api/gs/backtests_xasset/request.py` | [api/gs/backtests_xasset/request.md](api/gs/backtests_xasset/request.md) | 96 |
| `gs_quant/api/gs/backtests_xasset/response.py` | [api/gs/backtests_xasset/response.md](api/gs/backtests_xasset/response.md) | 47 |
| `gs_quant/api/gs/backtests_xasset/json_encoders/request_encoders.py` | [api/gs/backtests_xasset/json_encoders/request_encoders.md](api/gs/backtests_xasset/json_encoders/request_encoders.md) | 148 |
| `gs_quant/api/gs/backtests_xasset/json_encoders/response_encoders.py` | [api/gs/backtests_xasset/json_encoders/response_encoders.md](api/gs/backtests_xasset/json_encoders/response_encoders.md) | 106 |
| `gs_quant/api/gs/backtests_xasset/response_datatypes/backtest_datatypes.py` | [api/gs/backtests_xasset/response_datatypes/backtest_datatypes.md](api/gs/backtests_xasset/response_datatypes/backtest_datatypes.md) | 143 |
| `gs_quant/api/gs/backtests_xasset/response_datatypes/generic_backtest_datatypes.py` | [api/gs/backtests_xasset/response_datatypes/generic_backtest_datatypes.md](api/gs/backtests_xasset/response_datatypes/generic_backtest_datatypes.md) | 95 |
| `gs_quant/api/gs/backtests_xasset/response_datatypes/risk_result_datatypes.py` | [api/gs/backtests_xasset/response_datatypes/risk_result_datatypes.md](api/gs/backtests_xasset/response_datatypes/risk_result_datatypes.md) | 74 |
| `gs_quant/api/gs/backtests_xasset/response_datatypes/risk_result.py` | [api/gs/backtests_xasset/response_datatypes/risk_result.md](api/gs/backtests_xasset/response_datatypes/risk_result.md) | 48 |
| `gs_quant/api/gs/backtests_xasset/response_datatypes/test_backtest_datatypes.py` | [api/gs/backtests_xasset/json_encoders/response_datatypes/test_backtest_datatypes_encoders.md](api/gs/backtests_xasset/json_encoders/response_datatypes/test_backtest_datatypes_encoders.md) | 23 |

### Instrument (2 files)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/instrument/core.py` | [instrument/core.md](instrument/core.md) | 355 |
| `gs_quant/instrument/overrides.py` | [instrument/overrides.md](instrument/overrides.md) | 48 |

### Interfaces (1 file)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/interfaces/algebra.py` | [interfaces/algebra.md](interfaces/algebra.md) | 81 |

### Config (1 file)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/config/options.py` | [config/options.md](config/options.md) | 40 |

### Quote Reports (1 file)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/quote_reports/core.py` | [quote_reports/core.md](quote_reports/core.md) | 86 |

### Tracing (1 file)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/tracing/tracing.py` | [tracing/tracing.md](tracing/tracing.md) | 707 |

### Workflow (1 file)
| Source File | Spec | LOC |
|-------------|------|-----|
| `gs_quant/workflow/workflow.py` | [workflow/workflow.md](workflow/workflow.md) | 79 |

## Total: 172 spec files covering ~67,500 LOC
