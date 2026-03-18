# order.py

## Summary
Order types for backtesting: OrderBase (ABC), OrderTWAP, OrderMarketOnClose, OrderCost, OrderAtMarket, OrderTwapBTIC.

## Classes

### OrderBase (ABCMeta)
- execution_end_time(): raises RuntimeError
- _execution_price(): raises RuntimeError
- execution_price(data_handler): calls _execution_price, if NaN → RuntimeError
- execution_quantity(): raises RuntimeError (Bug 2 FIXED: error message now correct)
- execution_notional(): price * quantity
- _short_name(): raises RuntimeError
- to_dict(): uses instrument.ric

### OrderTWAP
- execution_end_time: window.end
- _execution_price: lazy mean of data_handler.get_data_range over window
- execution_quantity: self.quantity

### OrderMarketOnClose
- execution_end_time: date at 23:00
- _execution_price: lazy get_data on execution_date

### OrderCost
- Uses Cash instrument
- _execution_price: lazy, defaults to 0
- to_dict: uses instrument.currency instead of ric

### OrderAtMarket
- _execution_price: lazy get_data at execution_datetime

### OrderTwapBTIC (extends OrderTWAP)
- _execution_price: BTIC TWAP + closing price of underlying

## Edge Cases
- All _execution_price methods are lazy (cached in executed_price)
- NaN price → RuntimeError in execution_price
- OrderCost always returns 0 price

## Bugs Found
- Bug 2 (line 54): FIXED — error message changed from 'execution_price' to 'execution_quantity'

## Coverage Notes
- ~16 branches
