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


class SerializeError(Exception):
    pass


class PickleType(IntEnum):
    pickle = 0
    cloudpickle = 1
    dill = 2


class ClientSerializer:
    """
    Client-side serialization of requests and responses.
    """

    __slots__ = ('data', 'last_func', 'data_pickle', 'func_pickle')

    Func_cache = [None, None, None]

    def __init__(self, data_pickle, func_pickle):
        self.data_pickle = data_pickle
        self.func_pickle = func_pickle
        self.last_func = None
        self.data = bytearray()

    def add_data(self, data):
        self.data.extend(data)

    def write_request(self, write, task):
        func, args, kwargs, no_star, do_map = task
        if func is self.last_func:
            f = b''
        else:
            cache = ClientSerializer.Func_cache
            if self.func_pickle == cache[0] and func is cache[1]:
                f = cache[2]
            else:
                f = PICKLE_MODULES[self.func_pickle].dumps(func, -1)
                cache[:] = [self.func_pickle, func, f]
            self.last_func = func
        ddumps = PICKLE_MODULES[self.data_pickle].dumps
        ar = ddumps(args, -1) if args != () else b''
        kw = ddumps(kwargs, -1) if kwargs else b''

        header = struct.pack(
            REQ_HEADER_FMT,
            len(f), len(ar), len(kw), self.func_pickle, self.data_pickle,
            no_star, do_map)
        write(header)
        if f:
            write(f)
        if ar:
            write(ar)
        if kw:
            write(kw)
        del ar, kw

    def get_responses(self):
        """
        Yield (success, result) tuples as long as there is data.
        """
        data = self.data
        while data:
            sz = len(data)
            if sz < RESP_HEADER_SIZE:
                return
            header = data[:RESP_HEADER_SIZE]
            size, success = struct.unpack(RESP_HEADER_FMT, header)
            end = RESP_HEADER_SIZE + size
            if sz < end:
                return
            if size:
                payload = (
                    data if end < 4096 else
                    memoryview(data))[RESP_HEADER_SIZE:end]
                result = PICKLE_MODULES[self.data_pickle].loads(payload)
                del payload
            else:
                result = None
            del self.data[:end]
            yield (success, result)


class ServerSerializer:
    """
    Server-side serialization of requests and responses.
    """

    __slots__ = ('data', 'data_pickle', 'last_func')

    def __init__(self):
        self.data_pickle = None
        self.last_func = None
        self.data = bytearray()

    def add_data(self, data):
        self.data.extend(data)

    def get_request(self):
        """
        Return the next request, or None is there is none.
        """
        data = self.data
        sz = len(data)
        if sz < REQ_HEADER_SIZE:
            return None
        header = data[:REQ_HEADER_SIZE]
        (func_size, args_size, kwargs_size, func_pickle, self.data_pickle,
            no_star, do_map) = struct.unpack(REQ_HEADER_FMT, header)
        func_end = REQ_HEADER_SIZE + func_size
        args_end = func_end + args_size
        end = args_end + kwargs_size
        if sz < end:
            return None
        f = data[REQ_HEADER_SIZE: func_end]
        a = data[func_end:args_end]
        k = data[args_end:end]
        del self.data[:end]
        if f:
            func = PICKLE_MODULES[func_pickle].loads(f)
            self.last_func = func
        else:
            func = self.last_func
        del f
        loads = PICKLE_MODULES[self.data_pickle].loads
        args = loads(a) if a else ()
        del a
        kwargs = loads(k) if k else {}
        del k
        return func, args, kwargs, no_star, do_map

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
