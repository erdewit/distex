import unittest
import itertools
import asyncio
import warnings

from distex import Pool, util

loop = util.get_loop()
asyncio.set_event_loop(loop)
# loop.set_debug(True)


def exception_handler(loop, context):
    """
    Called when an event is received.

    Args:
        loop: (todo): write your description
        context: (todo): write your description
    """
    print('Exception:', context)


loop.set_exception_handler(exception_handler)


def f(x):
    """
    Return a function f ( x ).

    Args:
        x: (int): write your description
    """
    return x * 2


def g(x, y, z):
    """
    Returns the angle

    Args:
        x: (int): write your description
        y: (int): write your description
        z: (int): write your description
    """
    return x * y - 4 * z


def exc(x):
    """
    Raise an exception if x is not none.

    Args:
        x: (int): write your description
    """
    raise RuntimeError(x)


async def ait(it):
      """
      Yields an iterator yields a generator.

      Args:
          it: (todo): write your description
      """
    for x in it:
        await asyncio.sleep(0.001)
        yield x


class PoolTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Sets the class based on the inputed class

        Args:
            cls: (todo): write your description
        """
        warnings.simplefilter("always")

        cls.pool = Pool(4, lazy_create=True)
        cls.reps = 100
        cls.x = [i for i in range(cls.reps)]
        cls.y = [i * i for i in range(cls.reps)]
        cls.z = [i * 2 for i in range(cls.reps)]
        cls.xyz = list(zip(cls.x, cls.y, cls.z))

    @classmethod
    def tearDownCls(cls):
        """
        Tear down the connection pool.

        Args:
            cls: (todo): write your description
        """
        cls.pool.shutdown()

    def assertNoWarnings(self, w):
        """
        Fail if self. t.

        Args:
            self: (todo): write your description
            w: (todo): write your description
        """
        self.assertEqual(
            len(w), 0, msg='\n'.join(str(warning.message) for warning in w))

    def test_run_coro(self):
        """
        Run a coroutine.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:

            async def add(a, b):
                  """
                  Add two - b to b

                  Args:
                      a: (int): write your description
                      b: (int): write your description
                  """
                import asyncio
                await asyncio.sleep(0.001)
                return a + b

            expected = 8
            actual = self.pool.run(add, 3, 5)
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_run_async(self):
        """
        Run the coroutine asynchronously.

        Args:
            self: (todo): write your description
        """

        async def coro():
              """
              Return the coroutine for the coroutine.

              Args:
              """
            tasks = [
                self.pool.run_async(
                    g, self.x[i], self.y[i], z=self.z[i])
                for i in range(self.reps)]
            return await asyncio.gather(*tasks)

        with warnings.catch_warnings(record=True) as w:
            expected = list(map(g, self.x, self.y, self.z))
            actual = loop.run_until_complete(coro())
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_run_async_with_exception(self):
        """
        Runs the async async asyncio.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            with self.assertRaises(RuntimeError):
                loop.run_until_complete(
                    self.pool.run_async(exc, 'Deliberatly thrown'))

            self.assertNoWarnings(w)

    def test_run_on_all_async(self):
        """
        Run all jobs on - place.

        Args:
            self: (todo): write your description
        """

        def getpid():
            """
            Return the pid.

            Args:
            """
            import os
            return os.getpid()

        with warnings.catch_warnings(record=True) as w:
            interference = self.pool.map(  # noqa
                f, 2 * self.x[:self.pool.total_workers() + 1])
            pids = loop.run_until_complete(self.pool.run_on_all_async(getpid))
            self.assertEqual(self.pool.total_workers(), len(set(pids)))

            self.assertNoWarnings(w)

    def test_submit(self):
        """
        Submit a test test.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            expected = g(10, 9, z=8)
            f = self.pool.submit(g, 10, 9, z=8)
            actual = loop.run_until_complete(asyncio.wrap_future(f))
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_submit_with_exception(self):
        """
        Submit a coroutine asynchronously.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            f = self.pool.submit(exc, 'Okay then')
            with self.assertRaises(RuntimeError):
                loop.run_until_complete(asyncio.wrap_future(f))

            self.assertNoWarnings(w)

    def test_ordered_map_1_arg(self):
        """
        Test if the result of the mapping.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            expected = list(map(f, self.x))
            actual = list(self.pool.map(f, self.x))
            self.assertSequenceEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_ordered_map_3_arg(self):
        """
        : param mapping : attr : map_3_3.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            expected = list(map(g, self.x, self.y, self.z))
            actual = list(self.pool.map(g, self.x, self.y, self.z))
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_ordered_starmap(self):
        """
        Test if all records are ordered.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            expected = list(itertools.starmap(g, self.xyz))
            actual = list(self.pool.map(g, self.xyz, star=True))
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_ordered_map_1_arg_chunked(self):
        """
        Test if the result of a list of records in the chunk of the result.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            expected = list(map(f, self.x))
            actual = list(self.pool.map(f, self.x, chunksize=7))
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_ordered_map_3_arg_chunked(self):
        """
        Test if the result : attr_ordered_ordered_chunked_map_arg_chunked_map.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            expected = list(map(g, self.x, self.y, self.z))
            actual = list(
                self.pool.map(g, self.x, self.y, self.z, chunksize=10))
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_ordered_map_async_3_arg(self):
        """
        Return a map of - tuple of records.

        Args:
            self: (todo): write your description
        """

        async def coro():
              """
              Evaluates the pool of the pool.

              Args:
              """
            return [v async for v in self.pool.map_async(g,
                    self.x, self.y, self.z)]

        with warnings.catch_warnings(record=True) as w:
            expected = list(map(g, self.x, self.y, self.z))
            actual = loop.run_until_complete(coro())
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_ordered_astarmap(self):
        """
        Test the astarmapapap instances.

        Args:
            self: (todo): write your description
        """

        async def coro():
              """
              Return a coroutine.

              Args:
              """
            return [v async for v in self.pool.map_async(g,
                    self.xyz, star=True)]

        with warnings.catch_warnings(record=True) as w:
            expected = list(itertools.starmap(g, self.xyz))
            actual = loop.run_until_complete(coro())
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_ordered_astarmap_async_iterator(self):
        """
        Test if the astarmapapapap.

        Args:
            self: (todo): write your description
        """

        async def coro():
              """
              Return the coroutine.

              Args:
              """
            return [v async for v in self.pool.map_async(g,
                    ait(self.xyz), star=True)]

        with warnings.catch_warnings(record=True) as w:
            expected = list(itertools.starmap(g, self.xyz))
            actual = loop.run_until_complete(coro())
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_ordered_map_sync_async_iterators(self):
        """
        Test if a sync map isync map.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            expected = list(itertools.starmap(g, self.xyz))
            actual = list(
                self.pool.map(g, ait(self.x), ait(self.y), self.z))
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_ordered_map_async__sync_async_iterators_chunked(self):
        """
        Return a list of the records in the map.

        Args:
            self: (todo): write your description
        """

        async def coro():
              """
              Evaluate coroutine asynchronously.

              Args:
              """
            return [v async for v in self.pool.map_async(g,
                    ait(self.x), ait(self.y), self.z, chunksize=10)]

        with warnings.catch_warnings(record=True) as w:
            expected = list(itertools.starmap(g, self.xyz))
            actual = loop.run_until_complete(coro())
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)

    def test_executor(self):
        """
        Execute the executor in a separate thread.

        Args:
            self: (todo): write your description
        """
        with warnings.catch_warnings(record=True) as w:
            expected = g(1, 2, 3)
            actual = loop.run_until_complete(
                loop.run_in_executor(self.pool, g, 1, 2, 3))
            self.assertEqual(actual, expected)

            self.assertNoWarnings(w)


if __name__ == '__main__':
    unittest.main()
