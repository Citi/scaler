
from scaler import Client

def minus(a, b):
    return a - b

def fibonacci(clnt: Client, n: int):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        # Dynamically building graph in the worker side is okay
        fib_graph = {
            "n": n,
            "one": 1,
            "two": 2,
            "n_minus_one": (minus, "n", "one"),
            "n_minus_two": (minus, "n", "two"),
        }
        res = clnt.get(fib_graph, keys = ["n_minus_one", "n_minus_two"])
        n_minus_one = res.get("n_minus_one")
        n_minus_two = res.get("n_minus_two")
        a = clnt.submit(fibonacci, clnt, n_minus_one)
        b = clnt.submit(fibonacci, clnt, n_minus_two)
        return a.result() + b.result()

with Client(address="tcp://127.0.0.1:2345") as client:
    result = client.submit(fibonacci, client, 8).result()
    print(result)  # 21

