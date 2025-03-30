"""
This example shows how to use Client.map function.
Client.map allows user to map a callable to each element in an
abstract list. For more information on map functionality, check
https://en.wikipedia.org/wiki/Map_(higher-order_function)
"""

import math

from scaler import Client
from scaler.client.client import Client
from scaler.cluster.combo import SchedulerClusterCombo


def main():
    # For an explanation on how SchedulerClusterCombo and Client work, please see simple_client.py
    cluster = SchedulerClusterCombo(n_workers=10)
    client = Client(address=cluster.get_address())

    # map each integer in [0, 100) through math.sqrt()
    # the first parameter is the function to call, and the second is a list of argument tuples
    # (x,) denotes a tuple of length one
    results = client.map(math.sqrt, [(x,) for x in range(100)])

    # Collect the results and sums them
    result = sum(results)

    print(result)


if __name__ == "__main__":
    main()
