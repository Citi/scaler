
import math
from scaler import Client

with Client(address="tcp://127.0.0.1:2345") as client:
    # Submits 100 tasks
    futures = [
        client.submit(math.sqrt, i)
        for i in range(0, 100)
    ]

    # Collects the results and sums them
    result = sum(future.result() for future in futures)

    print(result)  # 661.46
