import logging
from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import cloudscraper
from aiohttp import ClientSession, ClientError

logger = logging.getLogger(__name__)

tinkoff_table_check_for_short_access = "https://www.tinkoff.ru/invest/margin/equities/"
finviz_stock_page = "https://finviz.com/quote.ashx?t="

# Initialize UserAgent only once
try:
    _user_agent_string = str(UserAgent().chrome)
except Exception: # Handle cases where UserAgent might fail (e.g. network issues, outdated db)
    logger.warning("Failed to initialize UserAgent. Using a generic User-Agent string.")
    _user_agent_string = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

HEADER = {'User-Agent': _user_agent_string}


def to_number(string_val: str):
    if not isinstance(string_val, str): # Handle non-string inputs gracefully
        logger.debug(f"to_number received non-string input: {string_val}. Returning 0.")
        return 0

    string_val = string_val.strip()
    if not string_val:  # Handle empty string after strip
        logger.debug("to_number received an empty or whitespace-only string. Returning 0.")
        return 0
    if string_val == '-':
         return 0

    d = {
        'k': 1000,
        'm': 1000000,
        'b': 1000000000
    }

    char = string_val[-1].lower()
    if char in d:
        try:
            return int(float(string_val[:-1]) * d[char])
        except ValueError:
            logger.warning(f"ValueError converting string with multiplier '{string_val}' to number. Returning 0.")
            return 0
    elif char == '%':
        try:
            return float(string_val[:-1])
        except ValueError:
            logger.warning(f"ValueError converting percentage string '{string_val}' to float. Returning 0.")
            return 0
    else:
        try:
            return float(string_val)
        except ValueError:
            logger.debug(f"ValueError converting string '{string_val}' to float directly. Returning 0.")
            return 0


def finviz_urls(stock_list: list) -> list:
    logger.debug(f"Generating Finviz URLs for {len(stock_list)} stocks.")
    return [finviz_stock_page + s.ticker for s in stock_list if hasattr(s, 'ticker')]


async def async_downlaod_page(url: str, session: ClientSession) -> str | None:
    logger.debug(f"Asynchronously downloading page: {url}")
    try:
        async with session.get(url, headers=HEADER, timeout=15) as response: # Added timeout and headers
            response.raise_for_status() # Raise an exception for bad status codes
            text_content = await response.text()
            logger.debug(f"Successfully downloaded {url}, length {len(text_content)}")
            return text_content
    except ClientError as e:
        logger.error(f"aiohttp ClientError while downloading {url}: {e}")
        return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout while downloading {url}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error downloading {url}: {e}", exc_info=True)
        return None


def downlaod_page_cloudflare(url: str, scraper: cloudscraper.CloudScraper) -> str | None:
    logger.debug(f"Downloading page with cloudscraper: {url}")
    try:
        response = scraper.get(url, timeout=15) # Added timeout
        response.raise_for_status()
        logger.debug(f"Successfully downloaded {url} via cloudscraper, length {len(response.text)}")
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Cloudscraper RequestException while downloading {url}: {e}")
        return None
    except Exception as e: # Catch other potential errors from cloudscraper
        logger.error(f"Unexpected error with cloudscraper for {url}: {e}", exc_info=True)
        return None


def finviz(html: str) -> dict | None:
    if not html:
        logger.warning("Finviz parser received empty HTML content.")
        return None
    logger.debug("Parsing Finviz HTML content.")
    try:
        soup = BeautifulSoup(html, 'lxml')

        # Check for a key element to ensure the page is as expected
        ticker_element = soup.find(id='ticker')
        if not ticker_element:
            # Try to get some context if it's not a standard page
            title_tag = soup.find('title')
            page_title = title_tag.text if title_tag else "No title"
            logger.warning(f"Finviz HTML page does not seem to contain stock data. Page title: '{page_title}'. URL might be wrong or page structure changed.")
            # Example: log part of the HTML for debugging if it's small enough
            # logger.debug(f"Problematic HTML (first 500 chars): {html[:500]}")
            return None # Return None instead of raising ValueError to allow batch processing to continue

        table_title_trs = soup.find('table', class_='fullview-title').find_all('tr')
        if len(table_title_trs) < 3:
            logger.warning(f"Finviz title table structure unexpected. Found {len(table_title_trs)} rows.")
            return None

        table_mult = soup.find('table', class_='snapshot-table2')
        if not table_mult:
            logger.warning("Finviz snapshot table (snapshot-table2) not found.")
            return None

        snapshot_td2_cp = table_mult.find_all('td', class_='snapshot-td2-cp')
        snapshot_td2 = table_mult.find_all('td', class_='snapshot-td2')

        if len(snapshot_td2_cp) != len(snapshot_td2):
            logger.warning(f"Finviz snapshot table data mismatch: {len(snapshot_td2_cp)} keys vs {len(snapshot_td2)} values.")
            return None

        mult = {snapshot_td2_cp[i].text.lower().strip(): snapshot_td2[i].text.strip()
                for i in range(len(snapshot_td2))}

        # Helper to safely extract text from elements
        def safe_extract_text(elements, index, default="N/A"):
            try:
                return elements[index].text.strip()
            except IndexError:
                logger.warning(f"IndexError extracting text at index {index} from Finviz data.")
                return default

        def safe_find_all_and_extract(element, tag, index, default="N/A"):
            try:
                return element.find_all(tag)[index].text.strip()
            except IndexError:
                logger.warning(f"IndexError finding '{tag}' at index {index} in Finviz data.")
                return default

        data = {
            'ticker': table_title_trs[0].text.split(' ')[0].strip(), # Ensure ticker is stripped
            'name': safe_extract_text(table_title_trs, 1),
            'sector': safe_find_all_and_extract(table_title_trs[2], 'a', 0),
            'industry': safe_find_all_and_extract(table_title_trs[2], 'a', 1),
            'country': safe_find_all_and_extract(table_title_trs[2], 'a', 2),
            'market_cap': to_number(mult.get('market cap', '-')),
            'dividend': to_number(mult.get('dividend', '-')),
            'dividend_%': to_number(mult.get('dividend %', '-')),
            'employees': to_number(mult.get('employees', '-')),
            'recomendation': to_number(mult.get('recom', '-')),
            'p_e': to_number(mult.get('p/e', '-')),
            'p_s': to_number(mult.get('p/s', '-')),
            'debt_eq': to_number(mult.get('debt/eq', '-')),
            'short_float_%': to_number(mult.get('short float', '-')) # Finviz calls it 'Short Float'
        }
        logger.debug(f"Successfully parsed Finviz data for ticker: {data['ticker']}")
        return data
    except Exception as e:
        logger.error(f"Error parsing Finviz HTML: {e}", exc_info=True)
        # logger.debug(f"Problematic HTML (first 500 chars): {html[:500]}") # Optionally log HTML on error
        return None


# Global cache for Tinkoff short availability - consider alternatives for long-running/concurrent apps
isin_able: dict[str, bool] = {}
# TODO: This cache (isin_able) might need a TTL or a more robust update mechanism if app runs continuously.

def check_tinkoff_short_table(stock_isin: str) -> bool:
    if not stock_isin:
        logger.warning("check_tinkoff_short_table received empty stock_isin.")
        return False

    # Consider if this "less than 2" logic is still appropriate or if it should always refresh if not found.
    # For now, keeping existing logic but adding logging.
    if not isin_able or stock_isin not in isin_able: # Simplified condition: refresh if empty or ISIN not found
        logger.info(f"Tinkoff short availability for {stock_isin} not in cache or cache empty. Fetching from URL.")
        url = "https://www.tinkoff.ru/invest/margin/equities/"
        try:
            r = requests.get(url, headers=HEADER, timeout=15)
            r.raise_for_status()
            logger.debug(f"Successfully fetched Tinkoff margin equities page. Status: {r.status_code}")

            soup = BeautifulSoup(r.content, 'lxml')
            # Clear the cache before repopulating to ensure fresh data
            # isin_able.clear() # If we want full refresh each time this block is hit

            all_tr = soup.find_all('tr', class_=lambda x: x and 'Table__row_' in x and 'Table__row_clickable_' in x) # More flexible class search
            if not all_tr:
                logger.warning(f"No rows found in Tinkoff margin table. Page structure might have changed. URL: {url}")
                # Return False early if table structure seems off, to prevent incorrect cache state.
                # If we expect the ISIN might still be in a partially filled cache from a previous run,
                # we might not return here, but this indicates a problem.
                return False

            newly_fetched_isins = 0
            for tr in all_tr:
                all_td = tr.find_all('td')
                if len(all_td) > 2: # Ensure enough cells exist
                    isin = all_td[1].text.strip()
                    ability_text = all_td[2].text.strip()
                    is_available = (ability_text == "Доступен")
                    isin_able[isin] = is_available
                    newly_fetched_isins +=1
                    logger.debug(f"Cached Tinkoff short availability for ISIN {isin}: {is_available}")
                else:
                    logger.warning("Found a table row in Tinkoff margin data with less than 3 cells.")
            logger.info(f"Fetched and cached Tinkoff short availability for {newly_fetched_isins} ISINs.")
            if newly_fetched_isins == 0:
                 logger.warning(f"No ISINs were parsed from Tinkoff margin page. URL: {url}")


        except requests.exceptions.RequestException as e:
            logger.error(f"RequestException while fetching Tinkoff margin data: {e}", exc_info=True)
            # In case of network error, don't trust the cache for this specific ISIN if it wasn't found.
            # If the ISIN was already in cache, it will use the old value.
            # If we want to invalidate on error, we could do: `return isin_able.get(stock_isin, False)`
            # or even `raise` to signal a failure to update. For now, it will try to return from cache.
            pass # Fall through to try returning from cache or False
        except Exception as e:
            logger.error(f"Unexpected error processing Tinkoff margin data: {e}", exc_info=True)
            pass

    # Return from cache (either newly populated or existing)
    is_shortable = isin_able.get(stock_isin, False)
    logger.debug(f"Tinkoff short availability for {stock_isin}: {is_shortable} (from cache)")
    return is_shortable

# Need to import asyncio for ClientError and TimeoutError if not already present
import asyncio