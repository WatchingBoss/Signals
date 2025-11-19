import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from tinkoff.invest import Client


TARGET_TZ = ZoneInfo("Europe/Moscow")
NANO_DIVISOR = 1_000_000_000


def get_price(x):
    return x.units + (x.nano / NANO_DIVISOR)


def get_token() -> str:
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    return data['token_tinkoff_view_only']


def main():
    with Client(get_token()) as client:
        accounts = client.users.get_accounts().accounts
        for account in accounts:
            print(f"Account ID: {account.id}")
            print(f"Account Type: {account.type}")
            print(f"Account Name: {account.name}")
            print(f"Account Access Level: {account.access_level}")
            print("\n-----------------------------------------------\n")

        for account in accounts:
            portfolio = client.operations.get_portfolio(account_id=account.id)
            print(f"{account.name}:")
            print(f"Total amount: {get_price(portfolio.total_amount_portfolio)}")
            print(f"{'Ticker':<18} {'Type':<8} {'Quantity':<8} {'NKD':<6} {'Yield':<8}")
            for p in portfolio.positions:
                print(
                    f"{p.ticker:<18} {p.instrument_type:<8} {get_price(p.quantity):<8} "
                    f"{get_price(p.current_nkd):<6} {get_price(p.expected_yield):<8}"
                )
            print("\n-----------------------------------------------\n")


if __name__ == "__main__":
    main()