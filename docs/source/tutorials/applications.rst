


Applications
============

This page shows some real life examples.


Calculate Implied Volatility
----------------------------

This example calculate implied volatility for many given market price. We use `client.submit` to achieve such goal.
Alternatively, one may use `client.map` if their computation function is heavier than what's shown in the example.
Remember, `client.map` makes sense only in two cases: A. The body of the function you are submitting is quite large; 
B. The function is very computation heavy or is possible to block.

.. literalinclude:: ../../../examples/applications/implied_volatility.py
   :language: python


Get Option Closing Price
------------------------

This example gets option closing price for a specified ticket and the start date. We use `client.map` to reduce the
overhead introduced by slow IO speed.

.. literalinclude:: ../../../examples/applications/yfinance_historical_price.py
   :language: python



