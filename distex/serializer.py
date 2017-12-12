import struct
import pickle
import dill
import cloudpickle
from enum import IntEnum

REQ_HEADER_FMT = '!IQIBBBB'
REQ_HEADER_SIZE = struct.calcsize(REQ_HEADER_FMT)
RESP_HEADER_FMT = '!QB'
RESP_HEADER_SIZE = struct.calcsize(RESP_HEADER_FMT)
PICKLE_MODULES = [pickle, cloudpickle, dill]


class SerializeError(Exception): pass


class PickleType(IntEnum):
    pickle = 0
    cloudpickle = 1
    dill = 2


class Serializer:
    """
    Serialize tasks and results as requests and responses.
    """

    __slots__ = ('data', 'last_func', 'func_pickle', 'data_pickle',
            'ddumps', 'dloads')

    Func_cache = [None, None, None]

    def __init__(self, func_pickle=None, data_pickle=None):
        self.data = bytearray()
        self.last_func = None
        self.func_pickle = func_pickle
        self.data_pickle = data_pickle
        if data_pickle is not None:
            self.ddumps = PICKLE_MODULES[self.data_pickle].dumps
            self.dloads = PICKLE_MODULES[self.data_pickle].loads

    def write_request(self, write, task):
        func, args, kwargs, no_star, do_map = task
        if func is self.last_func:
            f = b''
        else:
            cache = Serializer.Func_cache
            if self.func_pickle == cache[0] and func is cache[1]:
                f = cache[2]
            else:
                f = PICKLE_MODULES[self.func_pickle].dumps(func, -1)
                cache[:] = [self.func_pickle, func, f]
            self.last_func = func
        ar = self.ddumps(args, -1) if args != () else b''
        kw = self.ddumps(kwargs, -1) if kwargs else b''

        header = struct.pack(REQ_HEADER_FMT,
                len(f), len(ar), len(kw), self.func_pickle, self.data_pickle,
                no_star, do_map)
        write(header)
        if f: write(f)
        if ar: write(ar)
        if kw: write(kw)
        del ar, kw

    def get_requests(self, data):
        """
        Yield request tuples as long as there is data.
        """
        self.data += data
        data = self.data
        while data:
            l = len(data)
            if l < REQ_HEADER_SIZE:
                return
            header = data[:REQ_HEADER_SIZE]
            (func_size, args_size, kwargs_size,
                    self.func_pickle, self.data_pickle,
                    no_star, do_map) = struct.unpack(REQ_HEADER_FMT, header)
            func_end = REQ_HEADER_SIZE + func_size
            args_end = func_end + args_size
            end = args_end + kwargs_size
            if l < end:
                return
            f = data[REQ_HEADER_SIZE: func_end]
            a = data[func_end:args_end]
            k = data[args_end:end]
            del self.data[:end]
            if f:
                func = PICKLE_MODULES[self.func_pickle].loads(f)
                self.last_func = func
            else:
                func = self.last_func
            del f
            args = PICKLE_MODULES[self.data_pickle].loads(a) if a else ()
            del a
            kwargs = PICKLE_MODULES[self.data_pickle].loads(k) if k else {}
            del k
            yield func, args, kwargs, no_star, do_map

    def write_response(self, write, success, result):
        if result is None:
            s = b''
        else:
            try:
                dumps = PICKLE_MODULES[self.data_pickle].dumps
                s = dumps(result, -1)
            except Exception as e:
                s = dumps(SerializeError(str(e)), -1)
                success = 0
        header = struct.pack(RESP_HEADER_FMT, len(s), success)
        write(header)
        write(s)

    def get_responses(self, data):
        """
        Yield (success, result) tuples as long as there is data.
        """
        self.data += data
        data = self.data
        while data:
            l = len(data)
            if l < RESP_HEADER_SIZE:
                return
            header = data[:RESP_HEADER_SIZE]
            size, success = struct.unpack(RESP_HEADER_FMT, header)
            end = RESP_HEADER_SIZE + size
            if l < end:
                return
            if size:
                payload = (data if end < 4096 else
                        memoryview(data)) [RESP_HEADER_SIZE:end]
                result = self.dloads(payload)
                del payload
            else:
                result = None
            del self.data[:end]
            yield (success, result)
