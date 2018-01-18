import os
from .pool import *

path = os.path.join(__path__[0], 'version.py')
__version__ = eval(open(path).read())

__all__ = pool.__all__
