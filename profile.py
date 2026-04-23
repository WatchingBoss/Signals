from t_tech.invest import Client

from config import INVEST_TOKEN


def get_price(x):
    return x.units + (x.nano / 1e9)


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

    pos_dict = {
        "bond": {},
        "share": {},
        "etf": {},
    }
    total_rub = 0

    for account in accounts:
        portfolio = client.operations.get_portfolio(account_id=account.id)
        total["portfolio"] += get_price(portfolio.total_amount_portfolio)
        total["bonds"] += get_price(portfolio.total_amount_bonds)
        total["shares"] += get_price(portfolio.total_amount_shares)
        total["etfs"] += get_price(portfolio.total_amount_etf)
        total["currencies"] += get_price(portfolio.total_amount_currencies)

        positions = client.operations.get_positions(account_id=account.id)
        print(f"Positions of {account.name}:")
        for i in positions.money:
            if i.currency == "rub":
                total_rub += get_price(i)
        securities = positions.securities
        for i in securities:
            if i.instrument_uid in pos_dict[i.instrument_type]:
                pos_dict[i.instrument_type][i.instrument_uid]["balance"] += i.balance
            else:
                pos_dict[i.instrument_type][i.instrument_uid] = {
                        "figi": i.figi,
                        "balance": i.balance,
                        "uid": i.instrument_uid,
                        "ticker": i.ticker,
                    }

    print(f"bonds: {len(pos_dict["bond"])}\n"
          f"shares: {len(pos_dict["share"])}\n"
          f"etfs: {len(pos_dict["etf"])}\n")

    print(f"\nTotal rub: {total_rub}\n")

    print("Bonds:")
    print(pos_dict["bond"])
    print("\n===================\n")
    print("Shares:")
    print(pos_dict["share"])
    print("\n===================\n")
    print("ETFs:")
    print(pos_dict["etf"])
    print("\n===================\n")


        # for p in portfolio.positions:
        #     if p.instrument_type == "bond":
        #         uid = p.instrument_uid
        #         quantity = get_price(p.quantity)
        #         avg_price = get_price(p.average_position_price)
        #
        #         if uid in bonds:
        #             bonds[uid].merge(quantity, avg_price)
        #         else:
        #             bonds[uid] = Bond(
        #                 p.ticker,
        #                 uid,
        #                 quantity,
        #                 avg_price,
        #                 get_price(p.current_nkd),
        #                 get_price(p.current_price),
        #             )
    pass


# def print_portfolio(total: dict, bonds: dict):
#     print(f"{'All accounts total':>20}: {total['account']:,.0f}")
#     print(f"{'Bonds total':>20}: {total['bonds']:,.0f}")
#
#     print("Bonds:")
#     print(
#         f"{'Ticker':<18} {'Quantity':<8} {'Average Price':<14} {'Yield':<12} {'NKD':<7} {'Price':<12}"
#         f"{'Total':<12} {'Total NKD':<12}"
#     )
#     for bond in list(bonds.values()):
#         print(bond.out_string())


def play_portfolio_response(client):
    accounts = client.users.get_accounts().accounts
    for acc in accounts:
        p = client.operations.get_portfolio(account_id=acc.id)
        positions = p.positions
        id = p.account_id
        expected_yield = get_price(p.expected_yield)

        print(f"ID: {id}\n"
              f"expected_yield: {expected_yield}")
        for position in positions:
            print(position)


def main():
    with Client(INVEST_TOKEN) as client:
        total = {
            "portfolio": 0.0,
            "bonds": 0.0,
            "shares": 0.0,
            "etfs": 0.0,
            "currencies": 0.0,
        }
        bonds = {}

        get_account_data(client, total, bonds)
        # print_portfolio(total, bonds)
        for k, v in total.items():
            print(f"{k}: {v:,.0f}")


if __name__ == "__main__":
    main()
