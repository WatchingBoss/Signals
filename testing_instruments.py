import polars as pl

from t_tech.invest import Client, CandleInterval
from t_tech.invest.utils import now

from config import INVEST_TOKEN


def get_shares(client):
    shares = client.instruments.shares()

    all_currencies = set()
    all_class_codes = set()
    all_exchanges = set()
    count_otc_flag_true = 0
    count_otc_flag_false = 0
    count_weekend_flag_true = 0
    all_instrument_exchanges = set()

    for share in shares.instruments:
        all_currencies.add(share.currency)
        if share.currency == "rub":
            all_class_codes.add(share.class_code)
            all_exchanges.add(share.exchange)
            if share.otc_flag:
                count_otc_flag_true += 1
            else:
                count_otc_flag_false += 1
            if share.weekend_flag:
                count_weekend_flag_true += 1
            all_instrument_exchanges.add(share.instrument_exchange)

    print(f"All currencies: {all_currencies}\n"
          f"All class codes: {all_class_codes}\n"
          f"All exchanges: {all_exchanges}\n"
          f"OTC flags true: {count_otc_flag_true}\n"
          f"OTC flags false: {count_otc_flag_false}\n"
          f"Weekend flags true: {count_weekend_flag_true}\n"
          f"All instrument exchanges: {all_instrument_exchanges}\n")


def check_volumes(client):
    shares = client.instruments.shares()
    shares = {
        x.uid: {
            'ticker': x.ticker,
            'name': x.name,
        } for x in shares.instruments if x.currency == "rub"
    }
    candles: list[dict] = []
    for uid in shares.keys():
        raw_candles = client.market_data.get_candles(
            instrument_id=uid,
            to=now(),
            limit=50,
            interval=CandleInterval.CANDLE_INTERVAL_HOUR
        )
        for candle in raw_candles.candles:
            candles.append(
                {
                    "ticker": shares[uid]["ticker"],
                    "name": shares[uid]["name"],
                    "volume": candle.volume,
                }
            )
    df = pl.DataFrame(candles)
    print(f"Length: {df.shape[0]}\n")
    avg_volume = df.group_by("ticker").agg(
        pl.col("volume").mean().alias("avg_volume")
    ).sort("avg_volume")
    with pl.Config(tbl_rows=250):
        print(avg_volume)


def main():
    with Client(INVEST_TOKEN) as client:
        check_volumes(client)


if __name__ == "__main__":
    main()