import os
import json
from datetime import datetime

from dataclasses import dataclass, asdict
from typing import Optional, List, Tuple

from zoneinfo import ZoneInfo
from pympler import asizeof
import polars as pl

from tinkoff.invest import (
    Client,
    Quotation,
    MoneyValue,
)

DATA_DIR_PATH = os.path.join(os.curdir, "data")
BONDS_FILE_PATH = os.path.join(DATA_DIR_PATH, "bonds.parquet")
BONDS_EXCEL_PATH = os.path.join(DATA_DIR_PATH, "bonds.xlsx")

TARGET_TZ = ZoneInfo("Europe/Moscow")


def get_token() -> str:
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    return data['token_tinkoff_sandbox']


@dataclass
class BondData:
    figi: str
    ticker: str
    uid: str
    name: str
    isin: str
    class_code: str
    currency: str
    exchange: str

    coupon_quantity_per_year: int
    issue_size: int

    maturity_date: datetime
    placement_date: datetime
    call_date: datetime

    current_nominal: float
    initial_nominal: float
    placement_price: float
    current_nkd: float

    sector: Optional[str] = None
    country_of_risk: Optional[str] = None
    country_of_risk_name: Optional[str] = None

    floating_coupon_flag: bool = False
    amortization_flag: bool = False
    perpetual_flag: bool = False
    for_qual_investor_flag: bool = False
    subordinated_flag: bool = False
    replaced_flag: bool = False

    @classmethod
    def from_tinkoff_object(cls, x) -> 'BondData':
        return cls(
            figi=x.figi,
            ticker=x.ticker,
            uid=x.uid,
            name=x.name,
            isin=x.isin,
            class_code=x.class_code,
            currency=x.currency,
            exchange=x.exchange,
            coupon_quantity_per_year=x.coupon_quantity_per_year,
            issue_size=x.issue_size,
            maturity_date=x.maturity_date,
            placement_date=x.placement_date,
            call_date=x.call_date,
            current_nominal=cls._convert_money_value(x.nominal),
            initial_nominal=cls._convert_money_value(x.initial_nominal),
            placement_price=cls._convert_money_value(x.placement_price),
            current_nkd=cls._convert_money_value(x.aci_value),
            sector=x.sector,
            country_of_risk=x.country_of_risk,
            country_of_risk_name=x.country_of_risk_name,
            floating_coupon_flag=x.floating_coupon_flag,
            amortization_flag=x.amortization_flag,
            perpetual_flag=x.perpetual_flag,
            for_qual_investor_flag=x.for_qual_investor_flag,
            subordinated_flag=x.subordinated_flag,
            replaced_flag=x.bond_type,
        )

    @classmethod
    def from_row(cls, row_dict: dict) -> "BondData":
        valid_keys = cls.__annotations__.keys()
        clean_dict = {k: v for k, v in row_dict.items() if k in valid_keys}
        return cls(**clean_dict)

    def to_dict(self) -> dict:
        data = asdict(self)
        data['call_flag'] = self.call_flag
        return data

    @property
    def call_flag(self) -> bool:
        """
        If there is no offer date then call_date year is 1970
        """
        if self.call_date < datetime.now(self.call_date.tzinfo):
            return False
        return True

    @staticmethod
    def _convert_quotation(x: Quotation) -> float:
        return x.units + (x.nano / 1e9)

    @staticmethod
    def _convert_money_value(x: MoneyValue) -> float:
        return x.units + (x.nano / 1e9)


def get_bonds(client) -> Tuple[List[BondData], pl.DataFrame]:
    if not os.path.isdir(DATA_DIR_PATH):
        os.mkdir(DATA_DIR_PATH)
    if not os.path.isfile(BONDS_FILE_PATH):
        bond_objects = [
            BondData.from_tinkoff_object(x)
            for x in client.instruments.bonds().instruments
        ]
        df = pl.DataFrame([x.to_dict() for x in bond_objects])
        df.write_parquet(BONDS_FILE_PATH)
        df.with_columns(
            pl.col(pl.Datetime).dt.to_string("%Y-%m-%d")
        ).write_excel(BONDS_EXCEL_PATH)
        return bond_objects, df
    df = pl.read_parquet(BONDS_FILE_PATH)
    bond_objects = [BondData.from_row(row) for row in df.to_dicts()]
    return bond_objects, df


def main():
    api_token = get_token()
    with Client(api_token) as client:
        bonds_list, bonds_df = get_bonds(client)

        print(f"Size of bonds list: {asizeof.asizeof(bonds_list) / 1024:.2f} KB")
        print(f"Size of bonds dataframe: {bonds_df.estimated_size() / 1024:.2f} KB")


if __name__ == "__main__":
    main()
