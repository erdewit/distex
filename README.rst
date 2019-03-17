|Build| |PyVersion| |Status| |PyPiVersion| |License| |Docs|

Introduction
============

Distex offers a distributed process pool to utilize multiple CPUs or machines.
It uses
`asyncio <https://docs.python.org/3.6/library/asyncio.html>`_
to efficiently manage the worker processes.

Features:

* Scales from 1 to 1000's of processors;
* Can handle in the order of 50.000 small tasks per second;
* Easy to use with SSH (secure shell) hosts;
* Full async support;
* Maps over unbounded iterables;
* Compatible with
  `concurrent.futures.ProcessPool <https://docs.python.org/3/library/concurrent.futures.html>`_
  (or PEP3148_).


Installation
------------

::

    pip3 install -U distex

When using remote hosts then distex must be installed on those too.
Make sure that the ``distex_proc`` script can be found in the path.

For SSH hosts: Authentication should be done with SSH keys since there is
no support for passwords. The remote installation  can be tested with::

    ssh <host> distex_proc

Dependencies:

* Python_ version 3.6 or higher;
* On Unix the ``uvloop`` package is recommended: ``pip3 install uvloop``
* SSH client and server (optional).

Examples
--------

A process pool can have local and remote workers.
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

    pool = Pool(4, 'ssh://maxi/8')

To use a pool in combination with
`eventkit <https://github.com/erdewit/eventkit>`_:

.. code-block:: python

    from distex import Pool
    import eventkit as ev
    import bz2

    pool = Pool()
    # await pool  # un-comment in Jupyter
    data = [b'A' * 1000000] * 1000

    pipe = ev.Sequence(data).poolmap(pool, bz2.compress).map(len).mean().last()

    print(pipe.run())  # in Jupyter: print(await pipe)
    pool.shutdown()

There is full support for every asynchronous construct imaginable:

.. code-block:: python

    import asyncio
    from distex import Pool

    def init():
        # pool initializer: set the start time for every worker
        import time
        import builtins
        builtins.t0 = time.time()

    async def timer(i=0):
        # async code running in the pool
        import time
        import asyncio
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

The plain ``ssh`` executable is used instead of much nicer solutions such
as `AsyncSSH <https://github.com/ronf/asyncssh>`_. This is to keep the
CPU usage of encrypting/decrypting outside of the event loop and offload
it to the ``ssh`` process(es).

Documentation
-------------

`Distex documentation <http://rawgit.com/erdewit/distex/master/docs/html/api.html>`_


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

.. |Docs| image:: https://readthedocs.org/projects/distex/badge/?version=latest
   :alt: Documentation
   :target: https://distex.readthedocs.io/
