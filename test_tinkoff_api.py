import os
import json
from datetime import datetime
import pytz

from tinkoff.invest.sandbox.client import SandboxClient
from tinkoff.invest import CandleInterval
from tinkoff.invest.utils import now


DATA_DIR_PATH = os.path.join(os.curdir, "data")
SHARES_FILE_PATH = os.path.join(DATA_DIR_PATH, "shares.json")

TARGET_TZ = pytz.timezone("Europe/Moscow")
NANO_DIVISOR = 1_000_000_000


def get_token() -> str:
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    return data['token_tinkoff_sandbox']


def get_rub_shares(client) -> dict:
    if not os.path.isdir(DATA_DIR_PATH):
        os.mkdir(DATA_DIR_PATH)
    if not os.path.isfile(SHARES_FILE_PATH):
        rub_shares = {x.ticker: (x.figi, x.class_code, x.name)
                      for x in client.instruments.shares().instruments if x.currency == "rub"}
        with open(SHARES_FILE_PATH, 'w') as f:
            json.dump(rub_shares, f)
    with open(SHARES_FILE_PATH, 'r') as f:
        return json.load(f)


def get_candles_with_limit(client, figi: str, to_time: datetime, limit: int, interval: CandleInterval) -> list:
    candles = client.market_data.get_candles(
        instrument_id=figi,
        to=to_time,
        limit=limit,
        interval=interval,
    ).candles
    # lambda get_price(x): x.units + (x.nano / NANO_DIVISOR)
    out_candles = []
    for candle in candles:
        moscow_time = candle.time.astimezone(TARGET_TZ).strftime("%H:%M:%S")
        high_price = candle.high.units + (candle.high.nano / NANO_DIVISOR)
        low_price = candle.low.units + (candle.low.nano / NANO_DIVISOR)
        close_price = candle.close.units + (candle.close.nano / NANO_DIVISOR)
        open_price = candle.open.units + (candle.open.nano / NANO_DIVISOR)

        out_candles.append((high_price, low_price, open_price, close_price, moscow_time))
    return out_candles


def main():
    api_token = get_token()
    with SandboxClient(api_token) as client:
        shares = get_rub_shares(client)
        sber_candles = get_candles_with_limit(client, shares['SBER'][0], now(), 25,
                                              CandleInterval.CANDLE_INTERVAL_5_SEC)
        print(f"{'High': 6}{'Low': 6}{'Open': 6}{'Close': 6}{'Time': 10}")
        for candle in sber_candles:
            print(f"{candle[0]: 6}{candle[1]: 6}{candle[2]: 6}{candle[3]: 6}{candle[4]: 10}")



if __name__ == "__main__":
    main()
