import asyncio
from ta.scanner import Scanner
import ta.myhelper as mh
from config import intervals, Paths
import os
from ta.scanner import test, test_overview

sum_paths = [os.path.join(Paths.sum_dir, interval + '.h5') for interval in intervals]


if __name__ == "__main__":
    scanner = Scanner(intervals)
    scanner.save_df(sum_paths)
    # scanner.print_df()
    scanner.fill_overview()

    # loop = asyncio.get_event_loop()
    # loop.create_task(scanner.streaming())
    # loop.create_task(scanner.resave_df(paths))
    loop.create_task(scanner.save_overview(os.path.join(Paths.overview, 'summery' + '.h5')))
    # loop.run_forever()
