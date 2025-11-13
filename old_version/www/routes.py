from flask import render_template, g
from flask_babel import get_locale

from www import app
from www.forms import ChooseInterval

import pandas as pd
import os
from datetime import datetime, timedelta


@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html", title="Signals")


@app.route("/overview")
def overview():
    df = pd.read_hdf(os.path.join("data", "overview", "summery" + ".h5"), key="df")
    return render_template("overview.html", title="Overview", df=df)


@app.route("/indicators/<interval>", methods=["GET"])
def indicators(interval):
    form = ChooseInterval(interval=interval)
    df = pd.read_hdf(os.path.join(app.config["SUM_DIR"], interval + ".h5"), key="df")
    return render_template(
        "indicators.html", title="Indicators", df=df, interval=interval, form=form
    )


@app.route("/stock/<ticker>/<interval>", methods=["GET"])
def stock(ticker, interval):
    form = ChooseInterval(interval=interval)
    df = pd.read_hdf(
        os.path.join(app.config["CANDLES_DIR"], ticker + "_" + interval + ".h5"),
        key="df",
    )
    df = df.sort_values(by="Time", ascending=False, ignore_index=True).iloc[:50]
    return render_template(
        "stock.html",
        title="Stock" + ticker,
        df=df,
        interval=interval,
        form=form,
        ticker=ticker,
    )


@app.before_request
def before_request():
    g.locale = str(get_locale())


@app.template_filter("time_format")
def time_format(value) -> str:
    if isinstance(value, datetime):
        date = value + timedelta(hours=3)
        return date.strftime("%y-%m-%d %H:%M:%S")
    return value
