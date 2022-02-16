import asyncio
from ta.scanner import Scanner
import ta.myhelper as mh
from config import intervals, Paths
import os
from ta.scanner import test

sum_paths = [os.path.join(Paths.sum_dir, interval + '.h5') for interval in intervals]


if __name__ == "__main__":
    # test()

    scanner = Scanner(intervals)
    scanner.save_df(sum_paths)
    scanner.print_df()

    # loop = asyncio.get_event_loop()
    # loop.create_task(scanner.streaming())
    # loop.create_task(scanner.resave_df(paths))
    # loop.run_forever()
