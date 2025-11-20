import os
import json
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


class Bond:
    def __init__(
            self, ticker: str, uid: str, quantity: float, average_position_price: float,
            expected_yield: float, nkd: float, current_price: float,
    ):
        self.ticker = ticker
        self.uid = uid
        self.quantity = quantity
        self.average_position_price = average_position_price
        self.expected_yield = expected_yield
        self.nkd = nkd
        self.current_price = current_price
        self.total = quantity * current_price
        self.total_nkd = quantity * nkd

    def out_string(self) -> str:
        return str(f"{self.ticker:<18} {self.quantity:<8} {self.average_position_price:<14.2f} "
                   f"{self.expected_yield:<12} {self.nkd:<7} {self.current_price:<12.2f} "
                   f"{self.total:<12.2f} {self.total_nkd:<12.2f}")
        


def main():
    with Client(get_token()) as client:
        accounts = client.users.get_accounts().accounts

        account_total = {}
        bonds_total = {}
        bonds = []

        for account in accounts:
            portfolio = client.operations.get_portfolio(account_id=account.id)
            account_total[account.name] = get_price(portfolio.total_amount_portfolio)
            bonds_total[account.name] = get_price(portfolio.total_amount_bonds)
            for p in portfolio.positions:
                if p.instrument_type == "bond":
                    bonds.append(Bond(
                        p.ticker, p.instrument_uid, get_price(p.quantity), get_price(p.average_position_price),
                        get_price(p.expected_yield), get_price(p.current_nkd), get_price(p.current_price)
                    ))

        for key, value in account_total.items():
            print(f"{key:<18} : {value}\n"
                  f"{'Bonds':>19}: {bonds_total[key]}")
        print(f"All accounts total: {sum(account_total.values())}")
        print(f"{'Bonds total':>18}: {sum(bonds_total.values())}")

        print("Bonds:")
        print(f"{'Ticker':<18} {'Quantity':<8} {'Average Price':<14} {'Yield':<12} {'NKD':<7} {'Price':<12}"
              f"{'Total':<12} {'Total NKD':<12}")
        for bond in bonds:
            print(bond.out_string())



if __name__ == "__main__":
    main()