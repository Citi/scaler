from scaler.scheduler.scheduler import Scheduler
from .about import __version__
from .client.client import Client, ScalerFuture
from .client.serializer.mixins import Serializer
from .cluster.cluster import Cluster
from .cluster.combo import SchedulerClusterCombo

assert isinstance(__version__, str)
assert isinstance(Client, type)
assert isinstance(ScalerFuture, type)
assert isinstance(Scheduler, type)
assert isinstance(Cluster, type)
assert isinstance(SchedulerClusterCombo, type)
assert isinstance(Serializer, type)
