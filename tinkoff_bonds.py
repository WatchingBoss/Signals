import os
import json
from zoneinfo import ZoneInfo
import polars as pl

from tinkoff.invest import Client

DATA_DIR_PATH = os.path.join(os.curdir, "data")
SHARES_FILE_PATH = os.path.join(DATA_DIR_PATH, "bonds.json")

TARGET_TZ = ZoneInfo("Europe/Moscow")


def get_price(x):
    return x.units + (x.nano / 1e9)


def get_token() -> str:
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    return data['token_tinkoff_sandbox']


def get_bonds(client) -> pl.DataFrame:
    if not os.path.isdir(DATA_DIR_PATH):
        os.mkdir(DATA_DIR_PATH)
    if not os.path.isfile(SHARES_FILE_PATH):
        bonds = [
            {
                'figi': x.figi,
                'ticker': x.ticker,
                'uid': x.uid,
                'class_code': x.class_code,
                'name': x.name,
                'isin': x.isin,
                'currency': x.currency,
                'exchange': x.exchange,
                'real_exchange': x.real_exchange,
                'coupon_quantity_per_year': x.coupon_quantity_per_year,
                'maturity_date': x.maturity_date.astimezone(TARGET_TZ),
                'nominal': get_price(x.nominal),
                'initial_nominal': get_price(x.initial_nominal),
                'state_reg_date': x.state_reg_date.astimezone(TARGET_TZ),
                'placement_date': x.placement_date.astimezone(TARGET_TZ),
                'first_1day_candle_date': x.first_1day_candle_date.astimezone(TARGET_TZ),
                'placement_price': get_price(x.placement_price),
                'current_NKD': get_price(x.aci_value),
                'country_of_risk_name': x.country_of_risk_name,
                'sector': x.sector,
                'issue_size': x.issue_size,
                'trading_status': x.trading_status,
                'otc_flag': x.otc_flag,
                'floating_coupon_flag': x.floating_coupon_flag,
                'perpetual_flag': x.perpetual_flag,
                'amortization_flag': x.amortization_flag,
                'for_qual_investor_flag': x.for_qual_investor_flag,
                'blocked_tca_flag': x.blocked_tca_flag,
                'subordinated_flag': x.subordinated_flag,
                'bond_type': x.bond_type,
                'call_date': x.call_date.astimezone(TARGET_TZ),
            }
            for x in client.instruments.bonds().instruments
        ]
        df = pl.DataFrame(bonds)
        df.write_json(SHARES_FILE_PATH)
    return pl.read_json(SHARES_FILE_PATH)


def main():
    api_token = get_token()
    with Client(api_token) as client:
        df = get_bonds(client)

        df.write_excel("temp.xlsx")


if __name__ == "__main__":
    main()
