import concurrent.futures
import os
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiohttp
from datetime import timedelta, datetime, timezone
from time import time

import tinvest as ti
import pandas as pd
import cloudscraper

from ta import myhelper as mh
from ta.stock import Stock
from ta.variables import DELTAS, SUM_COLUMNS, OVERVIEW_COLUMNS
from config import Interval
import ta.scraper as scr

# Basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# TODO: Streaming for 1min-day timeframes
# TODO: Update week and month by candle_market each week
# TODO: Signal for previous candle in every timeframe


def update_raw(df: pd.DataFrame, last_row: int, payload: ti.CandleStreaming) -> None:
    df.loc[last_row, 'Time'] = payload.time
    df.loc[last_row, 'Open'] = payload.o
    df.loc[last_row, 'High'] = payload.h
    df.loc[last_row, 'Low'] = payload.l
    df.loc[last_row, 'Close'] = payload.c
    df.loc[last_row, 'Volume'] = payload.v


async def handle_candle(payload: ti.CandleStreaming, stock: Stock):
    tf = stock.timeframes[payload.interval]
    last_row = tf.df.index[-1]
    if (payload.time - tf.df['Time'].iat[last_row]) >= DELTAS[tf.interval]:
        pd.concat([tf.df, pd.DataFrame(pd.Series(dtype=int))], axis=0)
        last_row += 1
    update_raw(tf.df, last_row, payload)


class Scanner:
    def __init__(self, intervals):
        logger.info("Initializing Scanner...")
        start_init = time()
        self.intervals = intervals
        self.client = mh.get_client()
        logger.info("Fetching market data for USD stocks...")
        self.usd_stocks = mh.get_market_data(self.client, 'USD', developing=True)
        self.ticker_figi = {s.ticker: s.figi for s in self.usd_stocks.values()}
        logger.info(f"Fetched {len(self.usd_stocks)} USD stocks. Time taken: {time() - start_init:.2f} sec (before filling DFs).")

        self.fill_dfs()

        start_indicators = time()
        self.fill_indicators()
        logger.info(f"Indicators filled in {time() - start_indicators:.2f} sec.")

        logger.info("Generating summaries...")
        start_summaries = time()
        self.summeries = [self.sum_df(interval) for interval in intervals]
        logger.info(f"Summaries generated in {time() - start_summaries:.2f} sec.")
        logger.info(f"Scanner initialization complete. Total time: {time() - start_init:.2f} sec.")

    def fill_dfs(self) -> None:
        logger.info("Starting to fill DataFrames for all stocks...")
        start_read = time()
        logger.info(f"Reading candle data for {len(self.usd_stocks)} stocks using ThreadPoolExecutor (max_workers=8)...")
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(s.read_candles, self.intervals) for s in self.usd_stocks.values()]
            # Wait for all futures to complete if needed, or handle results/exceptions
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result() # If read_candles returns something or to raise exceptions
                except Exception as e:
                    logger.error(f"Error reading candles for a stock: {e}", exc_info=True)
        logger.info(f"Candle reading process completed in {time() - start_read:.2f} sec.")

        total_fill_df_time = 0
        for stock_obj in self.usd_stocks.values():
            start_fill_stock = time()
            logger.debug(f"Filling DataFrame for stock: {stock_obj.ticker}")
            try:
                for interval in self.intervals:
                    logger.debug(f"Calling fill_df for {stock_obj.ticker}, interval: {interval}")
                    stock_obj.fill_df(self.client, interval)
                current_fill_time = time() - start_fill_stock
                total_fill_df_time += current_fill_time
                logger.info(f"fill_df completed for {stock_obj.ticker} in {current_fill_time:.2f} sec.")
            except Exception as e:
                logger.error(f"Error in fill_df for stock {stock_obj.ticker}: {e}", exc_info=True)
        logger.info(f"All DataFrames filled. Total time for fill_df operations: {total_fill_df_time:.2f} sec.")


    def save_candles(self):
        logger.info("Saving candle data for all stocks...")
        start_save = time()
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(s.save_candles, self.intervals) for s in self.usd_stocks.values()]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result() # To raise exceptions if any occurred in save_candles
                except Exception as e:
                    logger.error(f"Error saving candles for a stock: {e}", exc_info=True)
        logger.info(f"Candle data saving process completed in {time() - start_save:.2f} sec.")

    def fill_indicators(self) -> None:
        logger.info("Filling indicators for all stocks and intervals...")
        start_fill_indicators = time()
        with ThreadPoolExecutor(max_workers=6) as executor:
            for interval in self.intervals:
                logger.debug(f"Submitting fill_indicators tasks for interval: {interval}")
                futures = [executor.submit(s.fill_indicators, interval) for s in self.usd_stocks.values()]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result() # To raise exceptions
                    except Exception as e:
                        # It would be good to know which stock caused the error if s was available here
                        logger.error(f"Error filling indicators for interval {interval} for a stock: {e}", exc_info=True)
        logger.info(f"Indicators filling process completed in {time() - start_fill_indicators:.2f} sec.")

    def sum_df(self, interval: ti.CandleResolution) -> pd.DataFrame:
        logger.info(f"Generating summary DataFrame for interval: {interval}")
        start_sum_df = time()
        summery_data = []
        for s_obj in self.usd_stocks.values():
            last_values = []
            for column in SUM_COLUMNS[1:]: # First column is 'Ticker'
                try:
                    value = s_obj.timeframes[interval].df[column].iat[-1]
                    last_values.append(value)
                except KeyError:
                    logger.warning(f"KeyError for column '{column}' in {s_obj.ticker} for interval {interval}. Appending None.")
                    last_values.append(None)
                except IndexError:
                    logger.warning(f"IndexError for column '{column}' in {s_obj.ticker} for interval {interval} (likely empty DataFrame). Appending None.")
                    last_values.append(None)
            summery_data.append([s_obj.ticker] + last_values)

        df = pd.DataFrame(summery_data, columns=SUM_COLUMNS).sort_values(
            by='Ticker', ascending=True, ignore_index=True
        )
        logger.info(f"Summary DataFrame for interval {interval} generated in {time() - start_sum_df:.2f} sec. Shape: {df.shape}")
        return df

    def fill_overview(self):
        logger.info("Starting to fill overview data from Finviz...")
        start_fill_overview = time()
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )
        stock_list_for_finviz = list(self.usd_stocks.values())
        urls = scr.finviz_urls(stock_list_for_finviz)
        logger.info(f"Generated {len(urls)} Finviz URLs for {len(stock_list_for_finviz)} stocks.")

        htmls = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            logger.info("Downloading Finviz pages...")
            futures_to_url = {executor.submit(scr.downlaod_page_cloudflare, url, scraper): url for url in urls}
            for future in concurrent.futures.as_completed(futures_to_url):
                url = futures_to_url[future]
                try:
                    html_content = future.result()
                    htmls.append(html_content)
                    logger.debug(f"Successfully downloaded page for {url}")
                except Exception as e:
                    logger.error(f"Error downloading Finviz page {url}: {e}", exc_info=True)
        logger.info(f"Downloaded {len(htmls)} Finviz pages.")

        successful_parses = 0
        with ThreadPoolExecutor(max_workers=8) as executor:
            logger.info("Parsing Finviz HTML pages...")
            futures_to_html = {executor.submit(scr.finviz, html): html for html in htmls}
            for future in concurrent.futures.as_completed(futures_to_html):
                try:
                    parsed_data = future.result()
                    if parsed_data and 'ticker' in parsed_data and parsed_data['ticker'] in self.ticker_figi:
                        figi = self.ticker_figi[parsed_data['ticker']]
                        self.usd_stocks[figi].overview = parsed_data
                        successful_parses +=1
                        logger.debug(f"Successfully parsed Finviz data for ticker: {parsed_data['ticker']}")
                    else:
                        logger.warning(f"Could not map parsed Finviz data or 'ticker' field missing: {parsed_data.get('ticker', 'N/A')}")
                except Exception as e:
                    logger.error(f"Error parsing Finviz HTML: {e}", exc_info=True)
        logger.info(f"Successfully parsed and assigned overview data for {successful_parses} stocks.")
        logger.info(f"Finviz overview data filling completed in {time() - start_fill_overview:.2f} sec.")


    async def save_overview(self, path: str):
        logger.info(f"Starting save_overview loop. Target path: {path}")
        while True:
            try:
                file_mod_time = await asyncio.to_thread(os.path.getmtime, path) # Use getmtime for modification time
                if (time() - file_mod_time) < timedelta(days=1).total_seconds() : # Check if file is fresh enough
                    logger.info(f"Overview file {path} is recent. Sleeping for 6 hours.")
                    await asyncio.sleep(timedelta(hours=6).total_seconds())
                    continue
            except FileNotFoundError:
                logger.info(f"Overview file {path} not found. Proceeding to create it.")
            except Exception as e:
                logger.error(f"Error checking file modification time for {path}: {e}. Proceeding to save.", exc_info=True)

            logger.info("Generating overview DataFrame to save...")
            overviews_data = []
            for s_obj in self.usd_stocks.values():
                if s_obj.overview: # Ensure overview data exists
                    overviews_data.append(list(s_obj.overview.values()))
                else:
                    logger.warning(f"No overview data for stock {s_obj.ticker} during save_overview.")

            if not overviews_data:
                logger.warning("No overview data collected for any stock. Skipping save.")
                await asyncio.sleep(timedelta(hours=1).total_seconds()) # Wait before retrying
                continue

            df = pd.DataFrame(overviews_data, columns=OVERVIEW_COLUMNS).sort_values(
                by='Ticker', ascending=True, ignore_index=True
            )
            try:
                await asyncio.to_thread(df.to_hdf, path, key='df', mode='w')
                logger.info(f"Overview data successfully saved to {path}. Shape: {df.shape}")
            except Exception as e:
                logger.error(f"Error saving overview data to HDF5 file {path}: {e}", exc_info=True)

            # Wait before next check/save cycle
            await asyncio.sleep(timedelta(hours=6).total_seconds())


    async def streaming(self) -> None:
        logger.info("Initializing Tinkoff streaming...")
        try:
            async with ti.Streaming(mh.get_token()) as streaming:
                for interval in self.intervals:
                    logger.info(f"Subscribing to candles for interval: {interval}")
                    await asyncio.gather(*(streaming.candle.subscribe(s.figi, interval)
                                           for s in self.usd_stocks.values()), return_exceptions=True)
                logger.info("Subscriptions complete. Listening for streaming events...")
                async for event in streaming:
                    if event.payload and hasattr(event.payload, 'figi'):
                        logger.debug(f"Received streaming event: {event.payload}")
                        stock_obj = self.usd_stocks.get(event.payload.figi)
                        if stock_obj:
                            await handle_candle(event.payload, stock_obj)
                        else:
                            logger.warning(f"Received candle for unknown FIGI: {event.payload.figi}")
                    else:
                        logger.warning(f"Received an empty or malformed streaming event: {event}")
        except Exception as e:
            logger.error(f"Error during streaming: {e}", exc_info=True)
            logger.info("Attempting to reconnect streaming after 60 seconds...")
            await asyncio.sleep(60)
            # Consider a recursive call or a loop for robust reconnection,
            # possibly with backoff strategy. For now, just log and exit the method.
            # await self.streaming() # Example of a recursive call (needs careful handling)


    def print_df(self) -> None:
        logger.info("Printing summary DataFrames:")
        for i, df_summary in enumerate(self.summeries):
            interval = self.intervals[i] # Assuming summeries are in the same order as intervals
            logger.info(f"\nSummary for Interval: {interval}\n{df_summary.to_string()}")

    def save_df(self, paths: list) -> None:
        if len(paths) != len(self.summeries):
            logger.error(f"Mismatch between number of paths ({len(paths)}) and summaries ({len(self.summeries)}). Skipping save.")
            return

        logger.info("Saving summary DataFrames to HDF5 files...")
        for i in range(len(self.intervals)):
            df = self.summeries[i]
            path = paths[i]
            interval = self.intervals[i]
            logger.info(f"Processing summary for interval {interval} to be saved at {path}")
            try:
                df['Ticker'] = df['Ticker'].astype(str)
                # Ensure 'Time' column exists and handle potential errors if it's missing after sum_df
                if 'Time' in df.columns:
                    df['Time'] = pd.to_datetime(df['Time'], errors='raise', utc=True)
                else:
                    logger.warning(f"'Time' column not found in summary for interval {interval}. Cannot convert to datetime.")

                # Convert other relevant columns to numeric, handling potential errors
                numeric_cols = [col for col in SUM_COLUMNS[2:] if col in df.columns] # Process only existing columns
                if numeric_cols:
                    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce') # Coerce errors to NaT/NaN

                df.to_hdf(path, key='df', mode='w')
                logger.info(f"Successfully saved summary for interval {interval} to {path}. Shape: {df.shape}")
            except Exception as e:
                logger.error(f"Error saving summary for interval {interval} to {path}: {e}", exc_info=True)

    async def resave_df(self, paths: list) -> None:
        logger.info("Starting resave_df loop...")
        while True:
            logger.info('Resave_df: Periodic update cycle initiated.')
            await asyncio.sleep(60) # Initial short sleep or make it configurable

            logger.info("Resave_df: Re-filling indicators...")
            start_refill_indicators = time()
            # fill_indicators is synchronous, run in thread to not block asyncio loop
            await asyncio.to_thread(self.fill_indicators)
            logger.info(f"Resave_df: Indicators re-filled in {time() - start_refill_indicators:.2f} sec.")

            logger.info("Resave_df: Re-generating summaries...")
            start_resummeries = time()
            # sum_df is synchronous, run in thread
            new_summeries = []
            for interval in self.intervals:
                try:
                    summary_df = await asyncio.to_thread(self.sum_df, interval)
                    new_summeries.append(summary_df)
                except Exception as e:
                    logger.error(f"Resave_df: Error generating summary for interval {interval}: {e}", exc_info=True)
            self.summeries = new_summeries # Update with successfully generated summaries
            logger.info(f"Resave_df: Summaries re-generated in {time() - start_resummeries:.2f} sec.")

            logger.info("Resave_df: Re-saving DataFrames...")
            start_resave = time()
            # save_df is synchronous
            await asyncio.to_thread(self.save_df, paths)
            logger.info(f"Resave_df: DataFrames re-saved in {time() - start_resave:.2f} sec.")

            # self.print_df() # printing can be verbose for a loop, consider if needed or log level
            logger.info("Resave_df: Cycle complete. Waiting for next iteration.")
            # Configurable sleep time for how often to resave
            await asyncio.sleep(3600) # e.g., sleep for 1 hour


def test_overview():
    logger.info("Starting test_overview()...")
    client = mh.get_client()
    logger.info("Fetching stocks for test_overview...")
    stocks = list(mh.get_market_data(client, 'USD', developing=True).values())
    urls = scr.finviz_urls(stocks)
    logger.info(f"Generated {len(urls)} Finviz URLs.")

    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
    )

    htmls = []
    finviz_data = []
    logger.info("Downloading Finviz pages in test_overview...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures_to_url = {executor.submit(scr.downlaod_page_cloudflare, url, scraper): url for url in urls}
        for future in concurrent.futures.as_completed(futures_to_url):
            url = futures_to_url[future]
            try:
                html_content = future.result()
                htmls.append(html_content)
                logger.debug(f"test_overview: Downloaded {url}")
            except Exception as e:
                logger.error(f"test_overview: Error downloading {url}: {e}", exc_info=True)

    logger.info(f"test_overview: Downloaded {len(htmls)} HTML pages. Parsing...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(scr.finviz, html) for html in htmls}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                finviz_data.append(data)
            except Exception as e:
                logger.error(f"test_overview: Error parsing Finviz HTML: {e}", exc_info=True)

    logger.info(f"test_overview: Parsed {len(finviz_data)} items. Displaying data:")
    for d_item in finviz_data:
        if d_item:
            logger.info(f"--- Ticker: {d_item.get('ticker', 'N/A')} ---")
            for k, v_item in d_item.items():
                logger.info(f"{k}: {v_item}")
        else:
            logger.warning("test_overview: Encountered an empty parsed item.")
        logger.info("-" * 20)
    logger.info("test_overview() finished.")


def test():
    logger.info("Starting test()...")
    intervals_test = [
        Interval.min1, Interval.min5, Interval.min15, Interval.min30,
        Interval.hour, Interval.day, Interval.week, Interval.month,
    ]

    client = mh.get_client()
    logger.info("test: Fetching market data...")
    stocks_test = mh.get_market_data(client, 'USD', developing=True)
    logger.info(f"test: Fetched {len(stocks_test)} stocks.")

    logger.info("test: Reading candles for all stocks...")
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(s.read_candles, intervals_test) for s in stocks_test.values()]
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                future.result()
                logger.debug(f"test: read_candles completed for stock {i+1}/{len(stocks_test)}")
            except Exception as e:
                logger.error(f"test: Error in read_candles for a stock: {e}", exc_info=True)
    logger.info("test: Candle reading finished.")

    # Example: Fill DFs (Optional, as it's part of Scanner init)
    # logger.info("test: Filling DataFrames (example)...")
    # for s_obj_test in stocks_test.values():
    #     for interval_test in intervals_test:
    #         try:
    #             s_obj_test.fill_df(client, interval_test)
    #             logger.debug(f"test: fill_df for {s_obj_test.ticker}, {interval_test} done.")
    #         except Exception as e:
    #             logger.error(f"test: Error in fill_df for {s_obj_test.ticker}, {interval_test}: {e}", exc_info=True)

    # Example: Fill Indicators (Optional)
    # logger.info("test: Filling indicators (example)...")
    # for s_obj_test in stocks_test.values():
    #     for interval_test in intervals_test:
    #         try:
    #             s_obj_test.fill_indicators(interval_test)
    #             logger.debug(f"test: fill_indicators for {s_obj_test.ticker}, {interval_test} done.")
    #         except Exception as e:
    #             logger.error(f"test: Error in fill_indicators for {s_obj_test.ticker}, {interval_test}: {e}", exc_info=True)


    logger.info("test: Displaying tail of DataFrames for each stock and interval...")
    for s_obj_test in stocks_test.values():
        logger.info(f"--- Stock: {s_obj_test.ticker} ---")
        for interval_test in intervals_test:
            if interval_test in s_obj_test.timeframes and not s_obj_test.timeframes[interval_test].df.empty:
                logger.info(f"Interval: {interval_test}\n{s_obj_test.timeframes[interval_test].df.tail(3).to_string()}")
            else:
                logger.info(f"Interval: {interval_test} - No data or DataFrame is empty.")

    logger.info("test() finished.")


def test_tin():
    logger.info("Starting test_tin()...")
    interval_tin_test = Interval.min1
    client = mh.get_client()
    logger.info("test_tin: Fetching market data...")
    stocks_tin_test = mh.get_market_data(client, 'USD', developing=True)

    if not stocks_tin_test:
        logger.warning("test_tin: No stocks found. Exiting.")
        return

    s_tin_test = list(stocks_tin_test.values())[0]
    logger.info(f"test_tin: Using stock {s_tin_test.ticker} (FIGI: {s_tin_test.figi}) for test.")

    from ta.variables import PERIODS # Local import as it's specific to this test
    now = datetime.now(timezone.utc)
    from_time = now - PERIODS[interval_tin_test]

    logger.info(f"test_tin: Fetching market candles for {s_tin_test.ticker} from {from_time} to {now} with interval {interval_tin_test}.")
    try:
        candles_response = client.get_market_candles(
            s_tin_test.figi,
            from_=from_time,
            to=now,
            interval=ti.CandleResolution(interval_tin_test)
        )
        candles = candles_response.payload.candles
        logger.info(f"test_tin: Fetched {len(candles)} candles.")

        if not candles:
            logger.warning("test_tin: No candles received from API.")
            return

        candle_list = [[c.time, float(c.o), float(c.h), float(c.l), float(c.c), int(c.v)] for c in candles]
        df_tin_test = pd.DataFrame(
            candle_list,
            columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        ).sort_values(by='Time', ascending=True, ignore_index=True)

        logger.info(f"test_tin: DataFrame created. Shape: {df_tin_test.shape}\n{df_tin_test.to_string()}")
    except Exception as e:
        logger.error(f"test_tin: Error during API call or DataFrame creation: {e}", exc_info=True)

    logger.info("test_tin() finished.")
