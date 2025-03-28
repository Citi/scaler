from scaler import Client
from scaler.client.client import Client
from scaler.cluster.combo import SchedulerClusterCombo

large_object = [1, 2, 3, 4, 5]


def query(object_reference, idx):
    return object_reference[idx]


"""
This example demonstrates how to work with Client.send_object method.
Client.send_object method is used to submit large objects to the remote
end. User can then reuse this object multiple time. This saves the cost
of transmitting objects around.
"""


def main():
    # For how SchedulerClusterCombo and Client work, please read simple_client.py
    cluster = SchedulerClusterCombo(n_workers=1)
    client = Client(address=cluster.get_address())

    # send the "large_object" to the remote end for reuse.
    # the object name "name" here is optional. All operation should be based
    # on the reference to the object, which is "large_object_ref" in this case.
    large_object_ref = client.send_object(large_object, name="large_object")

    # Reuse through object reference
    # Note that this example is not very interesting, since query is essentially
    # a cheap operation that should be done in local end. We chose this operation
    # since it demonstrates that operation and operator defined on the original
    # type (list) can be applied to the reference.
    fut1 = client.submit(query, large_object_ref, 0)
    fut2 = client.submit(query, large_object_ref, 1)

    # For how return value of client.submit (which is a future) can be used, check
    # simple_client.py
    print(fut1.result())
    print(fut2.result())


if __name__ == "__main__":
    main()
