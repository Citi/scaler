


Applications
============

This page shows some real life examples.


Calculate Implied Volatility
----------------------------

This example calculate implied volatility for many given market price. We use `client.submit` to achieve such goal.
Alternatively, one may use `client.map` if their computation function is heavier than what's shown in the example.
Remember, `client.map` makes sense only in two cases: A. The body of the function you are submitting is quite large; 
B. The function is very computation heavy.

.. literalinclude:: ../../../examples/applications/implied_volatility.py
   :language: python


