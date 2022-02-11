import asyncio
from ta.scanner import Scanner
import ta.myhelper as mh
from config import intervals, PATHS
import os
from ta.scanner import test

paths = [os.path.join('data', interval + '.pkl') for interval in intervals]


if __name__ == "__main__":
    test(PATHS)

    # scanner = Scanner(intervals)
    # scanner.save_df(paths)
    # scanner.print_df()
    #
    # loop = asyncio.get_event_loop()
    # loop.create_task(scanner.streaming())
    # loop.create_task(scanner.resave_df(paths))
    # loop.run_forever()
