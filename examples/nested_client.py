
from scaler import Client


def fibonacci(clnt: Client, n: int):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        a = clnt.submit(fibonacci, clnt, n - 1)
        b = clnt.submit(fibonacci, clnt, n - 2)
        return a.result() + b.result()


with Client(address="tcp://127.0.0.1:2345") as client:

    result = client.submit(fibonacci, client, 8).result()
    print(result)  # 21
