from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
import cloudscraper
from aiohttp import ClientSession


tinkoff_table_check_for_short_access = "https://www.tinkoff.ru/invest/margin/equities/"
finviz_stock_page = "https://finviz.com/quote.ashx?t="

HEADER = {"User-Agent": str(UserAgent().chrome)}


def to_number(string: str):
    if string == "-":
        return 0
    d = {"k": 1000, "m": 1000000, "b": 1000000000}
    if string[-1] in d:
        return int(float(string[:-1]) * d[string[-1]])
    elif string[-1] == "%":
        return float(string[:-1])
    else:
        try:
            return float(string)
        except ValueError:
            return 0


def finviz_urls(stock_list: list) -> list:
    return [finviz_stock_page + s.ticker for s in stock_list]


async def async_downlaod_page(url: str, session: ClientSession) -> str:
    async with session.get(url) as response:
        return await response.text()


def downlaod_page_cloudflare(url: str, scraper: cloudscraper) -> str:
    return scraper.get(url).text


def finviz(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    if not soup.find(id="ticker"):
        raise ValueError("Html page does not have stock data")

    table_title_trs = soup.find("table", class_="fullview-title").find_all("tr")

    table_mult = soup.find("table", class_="snapshot-table2")
    snapshot_td2_cp = table_mult.find_all("td", class_="snapshot-td2-cp")
    snapshot_td2 = table_mult.find_all("td", class_="snapshot-td2")
    mult = {
        snapshot_td2_cp[i].text.lower(): snapshot_td2[i].text.lower()
        for i in range(len(snapshot_td2))
    }

    return {
        "ticker": table_title_trs[0].text.split(" ")[0],
        "name": table_title_trs[1].text,
        "sector": table_title_trs[2].find_all("a")[0].text,
        "industry": table_title_trs[2].find_all("a")[1].text,
        "country": table_title_trs[2].find_all("a")[2].text,
        "market_cap": to_number(mult["market cap"]),
        "dividend": to_number(mult["dividend"]),
        "dividend_%": to_number(mult["dividend %"]),
        "employees": to_number(mult["employees"]),
        "recomendation": to_number(mult["recom"]),
        "p_e": to_number(mult["p/e"]),
        "p_s": to_number(mult["p/s"]),
        "debt_eq": to_number(mult["debt/eq"]),
        "short_float_%": to_number(mult["short float"]),
    }


isin_able = {}


def check_tinkoff_short_table(stock_isin):
    if len(isin_able.values()) < 2:
        url = "https://www.tinkoff.ru/invest/margin/equities/"
        r = requests.get(url, headers=HEADER)
        soup = BeautifulSoup(r.content, "lxml")

        all_tr = soup.find_all(
            "tr", class_="Table__row_3Unlc Table__row_clickable_3EeUg"
        )
        for tr in all_tr:
            all_td = tr.find_all("td")
            isin = all_td[1].text
            ability = all_td[2].text
            if ability == "Доступен":
                isin_able[isin] = True
            else:
                isin_able[isin] = False

    try:
        return isin_able[stock_isin]
    except KeyError:
        return False