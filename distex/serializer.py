"""Serialization of tasks and data."""

import pickle
import struct
from enum import IntEnum

import cloudpickle
import dill

REQ_HEADER_FMT = '!IQIBB'
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
    """Client-side serialization of requests and responses."""

    __slots__ = (
        'data', 'func_pickle', 'data_pickle',
        'data_dumps', 'data_loads', 'func_dumps', 'last_func')

    Func_cache = [None, None]

    def __init__(self, func_pickle, data_pickle):
        self.func_pickle = func_pickle
        self.data_pickle = data_pickle
        self.data_dumps = PICKLE_MODULES[data_pickle].dumps
        self.data_loads = PICKLE_MODULES[data_pickle].loads
        self.func_dumps = PICKLE_MODULES[func_pickle].dumps
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
            if func is cache[0]:
                f = cache[1]
            else:
                f = self.func_dumps(func, -1)
                cache[:] = [func, f]
            self.last_func = func
        ar = self.data_dumps(args, -1) if args != () else b''
        kw = self.data_dumps(kwargs, -1) if kwargs else b''

        header = struct.pack(
            REQ_HEADER_FMT, len(f), len(ar), len(kw), no_star, do_map)
        write(header)
        if f:
            write(f)
        if ar:
            write(ar)
        if kw:
            write(kw)
        del ar, kw

    def get_responses(self):
        """Yield (success, result) tuples as long as there is data."""
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
                result = self.data_loads(payload)
                del payload
            else:
                result = None
            del self.data[:end]
            yield (success, result)


class ServerSerializer:
    """Server-side serialization of requests and responses."""

    __slots__ = ('data', 'func_loads', 'data_loads', 'data_dumps', 'last_func')

    def __init__(self, func_pickle, data_pickle):
        self.func_loads = PICKLE_MODULES[func_pickle].loads
        self.data_loads = PICKLE_MODULES[data_pickle].loads
        self.data_dumps = PICKLE_MODULES[data_pickle].dumps
        self.last_func = None
        self.data = bytearray()

    def add_data(self, data):
        self.data.extend(data)

    def get_request(self):
        """Return the next request, or None is there is none"""
        data = self.data
        sz = len(data)
        if sz < REQ_HEADER_SIZE:
            return None
        header = data[:REQ_HEADER_SIZE]
        func_size, args_size, kwargs_size, no_star, do_map = \
            struct.unpack(REQ_HEADER_FMT, header)
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
            func = self.func_loads(f)
            self.last_func = func
        else:
            func = self.last_func
        args = self.data_loads(a) if a else ()
        kwargs = self.data_loads(k) if k else {}
        del f, a, k
        return func, args, kwargs, no_star, do_map

    def write_response(self, write, success, result):
        if result is None:
            s = b''
        else:
            try:
                s = self.data_dumps(result, -1)
            except Exception as e:
                s = self.data_dumps(SerializeError(str(e)), -1)
                success = 0
        header = struct.pack(RESP_HEADER_FMT, len(s), success)
        write(header)
        write(s)
