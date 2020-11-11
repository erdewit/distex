"""Execute tasks in a pool of local or remote worker processes."""

from .pool import HostSpec, LoopType, PickleType, Pool, RemoteException
from .poolmap import PoolMap
from .version import __version__, __version_info__  # noqa

__all__ = [
    'Pool', 'RemoteException', 'HostSpec', 'PickleType', 'LoopType', 'PoolMap']
