import os
import json

from tinkoff.invest.sandbox.client import SandboxClient
from tinkoff.invest import InstrumentIdType, InstrumentType


def get_token() -> str:
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    return data['token_tinkoff_sandbox']


def main():
    api_token = get_token()
    with SandboxClient(api_token) as client:
        instrument_gazp = client.instruments.get_instrument_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                                                 class_code="TQBR",
                                                 id="GAZP")
        all_shares = client.instruments.shares()

        rub_shares = [x for x in all_shares.instruments if x.currency == "rub"]
        i = 1
        for share in rub_shares:
            print(f"{i:3}: | {share.figi:12} | {share.ticker:6} | {share.class_code:6} | {share.name}")
            i += 1



if __name__ == "__main__":
    main()
