import sys
import time
import cProfile
import pstats
import asyncio
import traceback

# import uvloop
# uvloop.install()

from distex import Pool, util, PickleType

sys.excepthook = traceback.print_exception
loop = util.get_loop()
asyncio.set_event_loop(loop)
# util.logToConsole(logging.DEBUG)
# loop.set_debug(True)

loop = asyncio.get_event_loop()
REPS = 100000


def f(x):
    import math
    for _ in range(10000):
        x += math.sin(x)
    return x


def g(x):
    return x + 2


def main():

    async def run():
        result = await asyncio.gather(
            *[pool.run_async(g, i) for i in range(REPS)])
        print(result)

    async def map_async():
        async for result in pool.map_async(
                g, range(REPS),
                chunksize=1, timeout=None):
            pass
        print(result)
        # pool.shutdown()

    pool = Pool()
    if 1:
        loop.run_until_complete(map_async())
    elif 1:
        loop.run_until_complete(run())
    else:
        for r in pool.map(g, range(REPS)):
            pass
        print(r)
    # pool.shutdown()


if 1:
    t0 = time.time()
    main()
    print(REPS / (time.time() - t0))
else:
    profPath = '.distex.prof'
    cProfile.run('main()', profPath)
    stats = pstats.Stats(profPath)
    stats.strip_dirs()
    stats.sort_stats('time')
    stats.print_stats()
#     profile.print_stats()
