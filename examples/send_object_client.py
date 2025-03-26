
from scaler import Client

large_object = [1,2,3,4,5]

def query(object_reference, idx):
    return object_reference[idx]

with Client(address="tcp://127.0.0.1:2345") as client:
    # Reference behaves like the actual object.
    large_object_ref = client.send_object(large_object, "large_object")

    # Reuse through object reference
    fut1 = client.submit(query, large_object_ref, 0)
    fut2 = client.submit(query, large_object_ref, 1)

    print(fut1.result())
    print(fut2.result())


