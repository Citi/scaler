from scaler import Client
from scaler.client.client import Client
from scaler.cluster.combo import SchedulerClusterCombo


# Calculate fibonacci sequence with nested client.
# Each intermediate call in the recursive process is
# submitted to the client.
def fibonacci(clnt: Client, n: int):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        a = clnt.submit(fibonacci, clnt, n - 1)
        b = clnt.submit(fibonacci, clnt, n - 2)
        return a.result() + b.result()


# This example shows how to nest Client in task.
# Please read graphtask_nested_client.py for more information.
def main():
    # For how SchedulerClusterCombo and Client work, please read simple_client.py
    cluster = SchedulerClusterCombo(n_workers=1)
    client = Client(address=cluster.get_address())
    result = client.submit(fibonacci, client, 8).result()
    print(result)  # 21


if __name__ == "__main__":
    main()
