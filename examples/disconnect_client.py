import math
import random
from scaler import Client
from scaler.client import future

data = [random.randint(1, 100) for _ in range(10)]

with Client(address="tcp://127.0.0.1:2345") as client:
    results = client.map(math.sqrt, [(x,) for x in data])
    result = sum(results)
    # release resources and gracefully disconnect
    if result < 62:
        client.clear()
        client.disconnect()
    # Do other work
    else:
        print(result)
