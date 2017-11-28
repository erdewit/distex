|Build| |PyVersion| |Status| |PyPiVersion| |License|

Introduction
============

The distex package provides a distributed process pool for Python that uses
`asyncio <https://docs.python.org/3.6/library/asyncio.html>`_
to efficiently manage the local and remote worker processes.

Features:

* Scales to 1000's of processors;
* Can handle in the order of 50.000 small tasks per second;
* Easy to use with ssh (secure shell);
* Full asynchronous support;
* Maps over unbounded iterables;
* Choice of ``pickle``, ``dill`` or ``cloudpickle`` serialization for
  functions and data;
* Backward compatible with ``concurrent.futures.ProcessPool`` (PEP3148_).
 

Installation
------------

::

    pip3 install -U distex

Dependencies:

* Python_ version 3.6 or higher;
* On Unix the ``uvloop`` package is recommended: ``pip3 install uvloop``
* SSH client and server (optional). 

Examples
--------

A process pool can have local and remote workers in any combination.
Here is a pool that uses 4 local workers:

.. code-block:: python

    from distex import Pool

    def f(x):
        return x*x

    pool = Pool(4)
    for y in pool.map(f, range(100)):
        print(y)
        
To create a pool that also uses 8 workers on host ``maxi``, using ssh:

.. code-block:: python

    pool = Pool(4, ['ssh://maxi/8'])

There is full support for every asynchronous construct imaginable:

.. code-block:: python

    import asyncio
    from distex import Pool

    def init():
        # pool initializer: set the start time for every worker
        import time
        __builtins__.t0 = time.time()
    
    async def timer(i=0):
        # async code running in the pool
        import time
        await asyncio.sleep(1)
        return time.time() - t0
    
    async def ait():
        # async iterator running on the user side
        for i in range(20):
            await asyncio.sleep(0.1)
            yield i
    
    async def main():
        async with Pool(4, initializer=init, qsize=1) as pool:
            async for t in pool.map_async(timer, ait()):
                print(t)
            print(await pool.run_on_all_async(timer))
    
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

High level architecture
-----------------------

Distex does not use remote 'task servers'.
Instead it is done the other way around: A local
server is started first; Then the local and remote workers are started
and each of them will connect on its own back to the server. When all
workers have connected then the pool is ready for duty.

Each worker consists of a single-threaded process that is running
an asyncio event loop. This loop is used both for communication and for
running asynchronous tasks. Synchronous tasks are run in a blocking fashion.

When using ssh, a remote (or 'reverse') tunnel is created from a remote Unix socket
to the local Unix socket that the local server is listening on.
Multiple workers on a remote machine will use the same Unix socket and
share the same ssh tunnel.
    
Documentation
-------------

`Distex documentation <http://rawgit.com/erdewit/distex/master/docs/html/api.html>`_


Changelog
---------

Version 0.5.0
^^^^^^^^^^^^^

* Initial release


:author: Ewald de Wit <ewald.de.wit@gmail.com>

.. _Python: http://www.python.org

.. _ssh-keygen: https://linux.die.net/man/1/ssh-keygen

.. _ssh-copy-id: https://linux.die.net/man/1/ssh-copy-id

.. _PEP3148: https://www.python.org/dev/peps/pep-3148

.. |PyPiVersion| image:: https://img.shields.io/pypi/v/distex.svg
   :alt: PyPi
   :target: https://pypi.python.org/pypi/distex

.. |Build| image:: https://travis-ci.org/erdewit/distex.svg?branch=master
   :alt: Build
   :target: https://travis-ci.org/erdewit/distex

.. |PyVersion| image:: https://img.shields.io/badge/python-3.6+-blue.svg
   :alt:

.. |Status| image:: https://img.shields.io/badge/status-beta-green.svg
   :alt:

.. |License| image:: https://img.shields.io/badge/license-BSD-blue.svg
   :alt:
   
