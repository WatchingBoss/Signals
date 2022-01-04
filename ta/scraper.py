from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent

tinkoff_table_check_for_short_access = "https://www.tinkoff.ru/invest/margin/equities/"

HEADER = {'User-Agent': str(UserAgent().chrome)}


def check_for_hash(string):
    if len(string) < 2:
        try:
            return float(string)
        except ValueError:
            return 0
    else:
        return float(string)


def check_finviz(ticker):
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    r = requests.get(url, headers=HEADER)
    soup = BeautifulSoup(r.content, 'lxml')

    if not soup.find(id='ticker'):
        return False

    table_title = soup.find('table', class_='fullview-title')

    table_title_trs = table_title.find_all('tr')
    this_ticker = table_title.find(id='ticker').text

    exchange = table_title_trs[0].text
    exchange = exchange[exchange.index('[')+1:-1]
    if exchange == 'NASD':
        exchange = 'NASDAQ'
    name = table_title_trs[1].text
    sector = table_title_trs[2].find_all('a')[0].text
    industry = table_title_trs[2].find_all('a')[1].text

    table_mult = soup.find('table', class_='snapshot-table2')
    snapshot_td2_cp = table_mult.find_all('td', class_='snapshot-td2-cp')
    snapshot_td2 = table_mult.find_all('td', class_='snapshot-td2')
    mult = {snapshot_td2_cp[i].text.lower(): snapshot_td2[i].text.lower()
            for i in range(len(snapshot_td2))}

    p_e = check_for_hash(mult['p/e'])
    p_s = check_for_hash(mult['p/s'])
    debt_eq = check_for_hash(mult['debt/eq'])
    short_float = check_for_hash(mult['short float'][0:-1]) / 100
    price = mult['price']
    avg_v = mult['avg volume']
    atr = mult['atr']

    data = {
        'name': name,
        'sector': sector,
        'industry': industry,
        'exchange': exchange,
        'p_e': p_e,
        'p_s': p_s,
        'debt_eq': debt_eq,
        'short_float': short_float,
        'price': price,
        'avg_v': avg_v,
        'atr': atr
    }

    if this_ticker == ticker:
        return data


isin_able = {}


def check_tinkoff_short_table(stock_isin):
    if len(isin_able.values()) < 2:
        url = "https://www.tinkoff.ru/invest/margin/equities/"
        r = requests.get(url, headers=HEADER)
        soup = BeautifulSoup(r.content, 'lxml')

        all_tr = soup.find_all('tr', class_='Table__row_3Unlc Table__row_clickable_3EeUg')
        for tr in all_tr:
            all_td = tr.find_all('td')
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