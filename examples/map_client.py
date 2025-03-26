

import math
from scaler import Client
from scaler.client import future

data = [x for x in range(0, 100)]

with Client(address="tcp://127.0.0.1:2345") as client:
    # map sqrt to each element in data
    results = client.map(math.sqrt, [(x,) for x in data])

    # Collects the results and sums them
    result = sum(results)

    print(result)  # 661.46
