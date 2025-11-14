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


def main():
    api_token = get_token()
    with SandboxClient(api_token) as client:
        shares = get_rub_shares(client)
        sber_candles = client.market_data.get_candles(
            instrument_id=shares['SBER'][0],
            to=now(),
            limit=21,
            interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
        ).candles
        for candle in sber_candles:
            moscow_time = candle.time.astimezone(TARGET_TZ).strftime("%H:%M")
            close_price = candle.close.units + (candle.close.nano / NANO_DIVISOR)
            print(f"{moscow_time} | {close_price}")



if __name__ == "__main__":
    main()
