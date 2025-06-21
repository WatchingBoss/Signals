import logging
from time import time
import asyncio
from ta.scanner import Scanner
# import ta.myhelper as mh # Not directly used here, scanner handles client
from config import intervals, Paths
import os
# from ta.scanner import test, test_overview, test_tin # Test functions usually not run in main script

# Configure basic logging for the main script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sum_paths = [os.path.join(Paths.sum_dir, interval + '.h5') for interval in intervals]
overview_path = os.path.join(Paths.overview, 'summery.h5') # Define overview_path

async def main():
    main_start_time = time()
    logger.info("Starting main_ta.py script.")

    # --- Initial Scan ---
    logger.info("Performing initial data scan and save...")
    initial_scan_start_time = time()
    try:
        # Ensure data directories exist
        for path_obj in [Paths.data_dir, Paths.candles_dir, Paths.sum_dir, Paths.overview]:
            if not os.path.exists(path_obj):
                logger.info(f"Creating directory: {path_obj}")
                os.makedirs(path_obj, exist_ok=True)

        # The Scanner class __init__ does a lot of the initial data fetching and processing.
        scanner = Scanner(intervals)

        # Saving candles and initial summaries is part of the Scanner's __init__ or subsequent calls.
        # Let's assume Scanner's __init__ prepares data, and then we explicitly save.
        # Based on current Scanner code:
        # __init__ calls: fill_dfs -> fill_indicators -> summeries = [self.sum_df()]
        # So, after __init__, summeries are ready.

        logger.info("Saving initial candle data (if not already done by Scanner init)...")
        scanner.save_candles() # This saves raw candle data to disk.

        logger.info("Saving initial summary DataFrames...")
        scanner.save_df(sum_paths) # This saves the processed summaries.

        # Option to fill and save overview data initially
        logger.info("Filling and saving initial overview data...")
        scanner.fill_overview() # Fetches data from Finviz
        # The save_overview is an async task, so initial save might be different.
        # For an initial save, we might need a synchronous version or call it within the async part.
        # For simplicity, let's assume fill_overview populates the data,
        # and the async save_overview task will handle periodic saving.
        # Or, we can do a one-off save here if needed:
        # if scanner.usd_stocks: # Check if there's data to save
        #     overview_data_list = [list(s.overview.values()) for s in scanner.usd_stocks.values() if s.overview]
        #     if overview_data_list:
        #         overview_df = pd.DataFrame(overview_data_list, columns=OVERVIEW_COLUMNS).sort_values(by='Ticker', ascending=True, ignore_index=True)
        #         overview_df.to_hdf(overview_path, key='df', mode='w')
        #         logger.info(f"Initial overview data saved to {overview_path}")

        logger.info(f"Initial scan and save completed in {time() - initial_scan_start_time:.2f} sec.")
    except Exception as e:
        logger.error(f"Error during initial scan: {e}", exc_info=True)
        return # Exit if initial scan fails

    # --- Start Asynchronous Tasks ---
    logger.info("Starting long-running asynchronous tasks (streaming, resave_df, save_overview)...")
    try:
        # loop = asyncio.get_event_loop() # Not needed with asyncio.run or asyncio.gather

        # Create tasks
        streaming_task = asyncio.create_task(scanner.streaming(), name="TinkoffStreaming")
        resave_df_task = asyncio.create_task(scanner.resave_df(sum_paths), name="ResaveSummaries")
        save_overview_task = asyncio.create_task(scanner.save_overview(overview_path), name="SaveOverview")

        # Keep main running to allow tasks to execute, or await them if they are meant to complete
        # For services that run "forever", we'd typically await them.
        # If run_forever() was intended, we can simulate by awaiting a future that never completes,
        # or by awaiting the tasks themselves if they have their own infinite loops.
        logger.info("Async tasks created. Running indefinitely (or until tasks complete/fail)...")

        # Gather tasks to await their completion or capture exceptions
        # This will keep the main function alive until all these tasks finish (or one errors out if not handled in task)
        await asyncio.gather(
            streaming_task,
            resave_df_task,
            save_overview_task
            # return_exceptions=True # Could be used to handle task exceptions here
        )

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down async tasks...")
        # Add cancellation logic if tasks support it
        # streaming_task.cancel()
        # resave_df_task.cancel()
        # save_overview_task.cancel()
        # await asyncio.gather(streaming_task, resave_df_task, save_overview_task, return_exceptions=True)
        logger.info("Async tasks shutdown.")
    except Exception as e:
        logger.error(f"Error in main async execution: {e}", exc_info=True)
    finally:
        logger.info(f"Main_ta.py script finished in {time() - main_start_time:.2f} sec.")


if __name__ == "__main__":
    # To run test functions, execute them directly if needed, e.g.:
    # from ta.scanner import test
    # test()
    # exit()

    # Python 3.7+
    asyncio.run(main())
