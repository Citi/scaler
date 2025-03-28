
from scaler import Cluster
from scaler.utility.logging.utility import LoggingLevel
from scaler.utility.network_util import get_available_tcp_port

from scaler import SchedulerClusterCombo
from scaler import Scheduler
from scaler.scheduler.config import SchedulerConfig
from scaler.utility.zmq_config import ZMQConfig
from scaler.utility.network_util import get_available_tcp_port

from scaler.io.config import (
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
)

# This example shows how to instantiate a Cluster programmatically.
# For how to instantiate a Scheduler, read simple_scheduler.py
def main():
    N_WORKERS = 8
    # Initialize a Cluster as following.
    # You may find more information in the file where all these GLOBALs
    # are defined.
    cluster = Cluster(
        worker_io_threads=1,
        address=ZMQConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}"),
        worker_names=[str(i) for i in range(0, N_WORKERS - 1)],
        heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
        death_timeout_seconds=DEFAULT_WORKER_DEATH_TIMEOUT,
        garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        hard_processor_suspend=DEFAULT_HARD_PROCESSOR_SUSPEND,
        event_loop="builtin", # Or "uvloop"
        logging_paths=("/dev/stdout",),
        logging_level="DEBUG", # other choices are "INFO", "WARNING", "ERROR", "CRITICAL"
        logging_config_file=None,
    )

    # Calling Cluster.start starts a cluster.
    # cluster will start to accept tasks from scheduler after this call.
    cluster.start()

    # Cluster are long running until Cluster.terminate is called.
    # Should you wish to shutdown a cluster, call this method first.
    # Another choice is to kill cluster, which can be done by calling
    # Cluter.kill, which is a more "brutal" way to stop cluster.
    cluster.terminate()

    # Wait for the subprocess (initiated by Cluster.start) to join.
    # This method must be called before Cluster.close is called.
    # This method will not return unless Cluster.terminate is called.
    cluster.join()

    # Final step of closing a Cluster. The cluster must be joined first.
    cluster.close()


if __name__ == "__main__":
    main()
