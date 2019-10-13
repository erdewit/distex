import sys
import time
import cProfile
import pstats
import asyncio
import traceback

from distex import Pool, util

loop = util.get_loop()
asyncio.set_event_loop(loop)
# util.logToConsole(logging.DEBUG)

# import quamash
# import PyQt5.Qt as qt
# qApp = qt.QApplication([])
# loop = quamash.QEventLoop()
# asyncio.set_event_loop(loop)

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
                g, range(REPS), star=False,
                chunksize=16, ordered=True, timeout=None):
            pass
#             print(result)
        print(result)

#     pool = Pool(0, ['ssh://cup/4'], qsize=2)
#     pool = Pool(0, ['maxi/4'], qsize=2)
#     pool = Pool(4, ['localhost:10000/2', 'localhost:10001/4'])
    pool = Pool(qsize=4)
#     pool = Pool(4, ['ssh://maxi/2'], qsize=2)
    # pool = Pool(4, ['localhost:8899/4'], qsize=4)
    if 1:
        loop.run_until_complete(map_async())
    elif 1:
        loop.run_until_complete(run())
    else:
        for r in pool.map(g, range(REPS)):
            pass
        print(r)
    pool.shutdown()


sys.excepthook = traceback.print_exception

if 1:
    t0 = time.time()
    main()
    print(REPS / (time.time() - t0))
#     profile.print_stats()
else:
    profPath = '.distex.prof'
    cProfile.run('main()', profPath)
    stats = pstats.Stats(profPath)
    stats.strip_dirs()
    stats.sort_stats('time')
    stats.print_stats()
