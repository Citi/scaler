


Applications
============

This page shows some real life examples. Before reading this page, be sure that you have read examples in the 
Quickstart section. Those examples are better starting point than examples in this page. 


Calculate Implied Volatility
----------------------------

This example calculate implied volatility for many given market price. We use `client.map` to achieve such goal.
Notice that we provide chunk of data as input to `find_volatilities`. This is because `client.map` makes sense only 
in two cases: A. The body of the function you are submitting is quite large; B. The function is very computation 
heavy or is possible to block.

.. literalinclude:: ../../../examples/applications/implied_volatility.py
   :language: python


Get Option Closing Price
------------------------

This example gets option closing price for a specified ticker and the start date. We use `client.map` to reduce the
overhead introduced by slow IO speed.

.. literalinclude:: ../../../examples/applications/yfinance_historical_price.py
   :language: python


Timeseries Forecasting
----------------------

This example performs timeseries forecasting on the daily page views for a Wikipedia article using Meta's Prophet library.
We use Scaler to parallelize the cross-validation process and achieve a huge speedup.

.. literalinclude:: ../../../examples/applications/timeseries.py
   :language: python


Image Processing
----------------

This example uses Scaler to process a directory of images in parallel using Pillow.

.. literalinclude:: ../../../examples/applications/pillow.py
   :language: python


Web Server
----------

This example shows how Scaler can be used to parallelize a web server.
It includes a sequential version and a parallelized version using Scaler for comparison, plus an implementation of a client that rapidly sends requests to test the server.

.. literalinclude:: ../../../examples/applications/web_server.py
   :language: python

