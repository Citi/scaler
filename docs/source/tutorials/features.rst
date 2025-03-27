Additional Features
===================

Scaler comes with a number of additional features that can be used to monitor and profile tasks, and customize behavior.

Scaler Top (Monitoring)
-----------------------

Top is a monitoring tool that allows you to see the status of the Scaler.
The scheduler prints an address to the logs on startup that can be used to connect to it with the `scaler_top` CLI command:

.. code:: bash

    scaler_top ipc:///tmp/0.0.0.0_8516_monitor

Which will show an interface similar to the standard Linux `top` command:

.. code:: console

   scheduler        | task_manager        |     scheduler_sent         | scheduler_received
         cpu   0.0% |   unassigned      0 |      HeartbeatEcho 283,701 |          Heartbeat 283,701
         rss 130.1m |      running      0 |     ObjectResponse     233 |      ObjectRequest     215
                    |      success 53,704 |           TaskEcho  53,780 |               Task  53,764
                    |       failed     14 |               Task  54,660 |         TaskResult  53,794
                    |     canceled     48 |         TaskResult  53,766 |  DisconnectRequest      21
                    |    not_found     14 |      ObjectRequest     366 |         TaskCancel      60
                                          | DisconnectResponse      21 |    BalanceResponse      15
                                          |         TaskCancel      62 |          GraphTask       6
                                          |     BalanceRequest      15 |
                                          |    GraphTaskResult       6 |
   -------------------------------------------------------------------------------------------------
   Shortcuts: worker[n] agt_cpu[C] agt_rss[M] cpu[c] rss[m] free[f] sent[w] queued[d] lag[l]

   Total 7 worker(s)
                      worker agt_cpu agt_rss [cpu]    rss free sent queued   lag ITL |    client_manager
   2732890|sd-1e7d-dfba|d26+    0.5%  111.8m  0.5% 113.3m 1000    0      0 0.7ms 100 |
   2732885|sd-1e7d-dfba|56b+    0.0%  111.0m  0.5% 111.2m 1000    0      0 0.7ms 100 | func_to_num_tasks
   2732888|sd-1e7d-dfba|108+    0.0%  111.7m  0.5% 111.0m 1000    0      0 0.6ms 100 |
   2732891|sd-1e7d-dfba|149+    0.0%  113.0m  0.0% 112.2m 1000    0      0 0.9ms 100 |
   2732889|sd-1e7d-dfba|211+    0.5%  111.7m  0.0% 111.2m 1000    0      0   1ms 100 |
   2732887|sd-1e7d-dfba|e48+    0.5%  112.6m  0.0% 111.0m 1000    0      0 0.9ms 100 |
   2732886|sd-1e7d-dfba|345+    0.0%  111.5m  0.0% 112.8m 1000    0      0 0.8ms 100 |


* `scheduler` section shows the scheduler's resource usage
* `task_manager` section shows the status of tasks
* `scheduler_sent` section counts the number of each type of message sent by the scheduler
* `scheduler_received` section counts the number of each type of message received by the scheduler
* `worker` section shows worker details, you can use shortcuts to sort by columns, and the * in the column header shows which column is being used for sorting

  * `agt_cpu/agt_rss` means cpu/memory usage of the worker agent
  * `cpu/rss` means cpu/memory usage of the worker
  * `free` means number of free task slots for the worker
  * `sent` means how many tasks scheduler sent to the worker
  * `queued` means how many tasks worker received and enqueued
  * `lag` means the latency between scheduler and the worker
  * `ITL` means is debug information

    * `I` means processor initialized
    * `T` means have a task or not
    * `L` means task lock


Task Profiling
--------------

To get the execution time of a task, submit it with profiling turned on.

Futures are lazy, so ``.result()`` needs to be called on the Future first to ensure that it is executed.

.. code:: python

    from scaler import Client

    def calculate(sec: int):
        return sec * 1

    client = Client(address="tcp://127.0.0.1:2345")
    fut = client.submit(calculate, 1, profiling=True)

    # this will execute the task
    fut.result()

    # contains task run duration time in microseconds
    fut.profiling_info().duration_us

    # contains the peak memory usage in bytes for the task, this memory peak is sampled every second
    fut.profiling_info().peak_memory


Send Object
-----------

Scaler can send objects to the workers. This is useful for sending large objects that are needed for the tasks, and are reused frequently. This allows you to avoid the overhead of sending the object multiple times.

* The object is sent to the workers only once
* The Scaler API will return a special reference to the object
* Workers can use this reference to access the object, but this reference must be provided as a positional argument
  * `The reference cannot be nested in another reference or inside a list, etc.`

.. code:: python

    from scaler import Client

    def add(a, b):
        return a + b

    client = Client(address="tcp://127.0.0.1:2345")
    ref = client.send_object("large_object", [1, 2, 3, 4, 5])

    fut = client.submit(add, ref, [6])
    assert fut.result() == [1, 2, 3, 4, 5, 6]

    # this will not work, scaler doesn't do deep resolving of references
    # fut = client.submit(add, [ref], [6])


Graph Submission
----------------

Some tasks are complex and depend on the output of other tasks. Scaler supports submitting tasks as a graph and will handle executing the dependencies in the correct order and communicating between tasks.

.. code:: python

    from scaler import Client

    def inc(i):
        return i + 1

    def add(a, b):
        return a + b

    def minus(a, b):
        return a - b

    # graph tasks are specified as a dictionary of edges
    graph = {
        "a": 2,
        "b": 2,

        # this means that c depends on a,
        # and the function `inc` will be called with the result of `a`
        "c": (inc, "a"),  # c = a + 1 = 2 + 1 = 3
        "d": (add, "a", "b"),  # d = a + b = 2 + 2 = 4
        "e": (minus, "d", "c")  # e = d - c = 4 - 3 = 1
    }

    client = Client(address="tcp://127.0.0.1:2345")

    # submit the task and specify the the tasks that you want the result of
    # in this case just `e`, but it may be multiple tasks
    futures = client.submit_graph(graph, keys=["e"])

    print(futures[0].result())


Client Shutdown
---------------

By default, the Scheduler runs in protected mode. For more information, see the :ref:`protected <protected>` section.

If the Scheduler is not in protected mode, the Client can shutdown the Cluster by calling ``Client.shutdown()``.

.. code:: python

    from scaler import Client

    client = Client(address="tcp://127.0.0.1:2345")

    # shuts down the scheduler and all workers
    # assuming that the scheduler is not in 'protected' mode
    # otherwise does nothing
    client.shutdown()

Custom Serialization
--------------------

Scaler uses ``cloudpickle`` by default for serialization. You can use a custom serializer by passing it to the Client.

The serializer API has only two methods: ``serialize`` and ``deserialize``, and these are responsible for serializing and deserializing functions, function arguments, and function results.

.. note::
    All libraries used for serialization must be installed on workers.


.. py:function:: serialize(obj: Any) -> bytes

   :param obj: the object to be serialized, can be function object, argument object, or function result object
   :return: serialized bytes of the object

Serialize the object to bytes. This serialization method is called for the function object each argument, and function's result, for example:


.. code:: python

    def add(a, b):
        return a + b

    client.submit(add, 1, 2)


``serialize`` will be called four times:

* Once for the ``add`` function
* Once for the argument ``1``
* Once for ``2``
* Once for the result of the task

The client will then ``deserialize`` the result.


.. py:function:: deserialize(payload: bytes) -> Any

   :param payload: the serialized bytes of the object, can be function object, argument object, or function result object
   :return: any deserialized object

Deserialize the bytes into the original object, this deserialize method is used to deserialize the function and each argument as received by the workers, and the result of the task as received by the client.

Below is an example implementation of a custom serializer that uses a different serialization/deserialization method for different types of objects. It uses a simple tagging system to indicate the type of object being serialized/deserialized.

* Dataframes are serialized into the parquet format
* Integers are serialized as 4-byte integers
* All other objects are serialized using cloudpickle


.. code:: python

    import enum
    import pickle
    import struct
    from io import BytesIO
    from typing import Any

    import pandas as pd
    from cloudpickle import cloudpickle

    from scaler import Serializer


    class ObjType(enum.Enum):
        General = b"G"
        Integer = b"I"
        DataFrame = b"D"


    class CustomSerializer(Serializer):
        @staticmethod
        def serialize(obj: Any) -> bytes:
            if isinstance(obj, pd.DataFrame):
                buf = BytesIO()
                obj.to_parquet(buf)
                return ObjType.DataFrame.value + buf.getvalue()

            if isinstance(obj, int):
                return ObjType.Integer.value + struct.pack("I", obj)

            return ObjType.General.value + cloudpickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

        @staticmethod
        def deserialize(payload: bytes) -> Any:
            obj_type = ObjType(payload[0])
            payload = payload[1:]

            match obj_type:
                case ObjType.DataFrame:
                    buf = BytesIO(payload)
                    return pd.read_parquet(buf)

                case ObjType.Integer:
                    return struct.unpack("I", payload)[0]

                case _:
                    return cloudpickle.loads(payload)
