# how long SchedulerProcess.wait_until_ready waits for the scheduler to listen
SCHEDULER_READINESS_TIMEOUT_SECONDS = 30

# vanilla scaling adds a worker above VANILLA_UPPER_TASK_RATIO tasks per worker, shrinks below VANILLA_LOWER_TASK_RATIO
VANILLA_LOWER_TASK_RATIO = 1
VANILLA_UPPER_TASK_RATIO = 10
