import os
import json

from tinkoff.invest.sandbox.client import SandboxClient
from tinkoff.invest import InstrumentIdType, InstrumentType


DATA_DIR_PATH = os.path.join(os.curdir, "data")
SHARES_FILE_PATH = os.path.join(DATA_DIR_PATH, "shares.json")


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
        print(f"{shares['SBER']}")


if __name__ == "__main__":
    main()
