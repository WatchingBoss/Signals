import os
import json

from tinkoff.invest import Client


def get_price(x):
    return x.units + (x.nano / 1e9)


def get_token() -> str:
    with open(os.path.join(os.path.expanduser("~"), "no_commit", "info.json")) as f:
        data = json.load(f)
    return data["token_tinkoff_view_only"]


class Bond:
    def __init__(
        self,
        ticker: str,
        uid: str,
        quantity: float,
        avg_price: float,
        nkd: float,
        current_price: float,
    ):
        self.ticker = ticker
        self.uid = uid
        self.quantity = quantity
        self.avg_price = avg_price
        self.nkd = nkd
        self.current_price = current_price

    @property
    def total(self):
        return self.quantity * self.avg_price

    @property
    def total_nkd(self):
        return self.quantity * self.nkd

    @property
    def expected_yield(self):
        return (self.current_price - self.avg_price) * self.quantity

    def merge(self, new_quantity: float, new_avg_price: float):
        total = self.quantity * self.avg_price
        total_new = new_quantity * new_avg_price
        self.quantity += new_quantity
        self.avg_price = (total + total_new) / self.quantity

    def out_string(self) -> str:
        return str(
            f"{self.ticker:<18} {self.quantity:<8.0f} {self.avg_price:<14.2f} "
            f"{self.expected_yield:<12.2f} {self.nkd:<7} {self.current_price:<12.2f} "
            f"{self.total:<12.2f} {self.total_nkd:<12.2f}"
        )


def get_account_data(client, total: dict, bonds: dict):
    accounts = client.users.get_accounts().accounts

    total_amount_accounts = 0
    total_amount_bonds = 0

    for account in accounts:
        portfolio = client.operations.get_portfolio(account_id=account.id)
        total_amount_accounts += get_price(portfolio.total_amount_portfolio)
        total_amount_bonds += get_price(portfolio.total_amount_bonds)

        for p in portfolio.positions:
            if p.instrument_type == "bond":
                uid = p.instrument_uid
                quantity = get_price(p.quantity)
                avg_price = get_price(p.average_position_price)

                if uid in bonds:
                    bonds[uid].merge(quantity, avg_price)
                else:
                    bonds[uid] = Bond(
                        p.ticker,
                        uid,
                        quantity,
                        avg_price,
                        get_price(p.current_nkd),
                        get_price(p.current_price),
                    )

    total["account"] = total_amount_accounts
    total["bonds"] = total_amount_bonds


def print_portfolio(total: dict, bonds: dict):
    print(f"{'All accounts total':>20}: {total['account']:,.0f}")
    print(f"{'Bonds total':>20}: {total['bonds']:,.0f}")

    print("Bonds:")
    print(
        f"{'Ticker':<18} {'Quantity':<8} {'Average Price':<14} {'Yield':<12} {'NKD':<7} {'Price':<12}"
        f"{'Total':<12} {'Total NKD':<12}"
    )
    for bond in list(bonds.values()):
        print(bond.out_string())


def main():
    with Client(get_token()) as client:
        total = {}
        bonds = {}

        get_account_data(client, total, bonds)
        print_portfolio(total, bonds)


if __name__ == "__main__":
    main()