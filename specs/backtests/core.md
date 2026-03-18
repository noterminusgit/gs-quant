# core.py

## Summary
Core backtest types: TradeInMethod enum, Backtest (extends target Backtest with get_results), MarketModel enum, TimeWindow NamedTuple, ValuationFixingType enum, ValuationMethod NamedTuple.

## Classes

### Backtest
- get_results(): calls GsBacktestApi.get_results(backtest_id=self.id)

### TimeWindow (NamedTuple)
- start: Union[dt.time, dt.datetime] = None
- end: Union[dt.time, dt.datetime] = None

### ValuationMethod (NamedTuple)
- data_tag: ValuationFixingType = PRICE
- window: Optional[TimeWindow] = None

## Bugs Found
None.

## Coverage Notes
- ~2 branches (get_results API call)
