from scaler import Client

def inc(i):
    return i + 1

def add(a, b):
    return a + b

def minus(a, b):
    return a - b

graph = {
    "a": 2,
    "b": 2,
    "c": (inc, "a"),  # c = a + 1 = 2 + 1 = 3
    "d": (add, "a", "b"),  # d = a + b = 2 + 2 = 4
    "e": (minus, "d", "c")  # e = d - c = 4 - 3 = 1
}

with Client(address="tcp://127.0.0.1:2345") as client:
    # If you are only interested in keys 'e', replace keys to ["e"]
    result = client.get(graph, keys=["a", "b", "c", "d", "e"])
    print(result.get("e"))
    print(result)  # {"e": 1}
