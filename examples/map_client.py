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

    # map sqrt to each element in data
    # Note 1, this results has type List[Any]. In this case, the results is a List of floats, instead of a list of
    # futures.

    # Note 2, (x,) is a tuple of one element. Client.map assumes the first element to be the callable, and the second
    # element to be a list of tuple, where each tuple represents arguments of a function call.
    results = client.map(math.sqrt, [(x,) for x in range(0, 100)])

    # Collect the results and sums them
    result = sum(results)

    print(result)


if __name__ == "__main__":
    main()
