from flask import render_template, url_for, g
from flask_babel import get_locale

from www import app

import pandas as pd
import os


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html', title='Signals')


@app.route('/overview')
def overview():
    df = pd.read_pickle(os.path.join('data', 'overview.pkl'))
    return render_template('overview.html', title='Overview',
                           table=df.to_html(classes='table table-striped table-bordered table-sm'))

@app.route('/indicators')
def indicators():
    df_min1 = pd.read_pickle(os.path.join('data', '1min.pkl'))
    df_min5 = pd.read_pickle(os.path.join('data', '5min.pkl'))
    df_min15 = pd.read_pickle(os.path.join('data', '15min.pkl'))
    df_min30 = pd.read_pickle(os.path.join('data', '30min.pkl'))
    df_hour = pd.read_pickle(os.path.join('data', 'hour.pkl'))
    df_day = pd.read_pickle(os.path.join('data', 'day.pkl'))
    df_week = pd.read_pickle(os.path.join('data', 'week.pkl'))
    df_month = pd.read_pickle(os.path.join('data', 'month.pkl'))
    classes = 'table table-striped table-bordered table-hover table-sm'
    return render_template('indicators.html', title='Indicators',
                           table_min1=df_min1.to_html(classes=classes),
                           table_min5=df_min5.to_html(classes=classes),
                           table_min15=df_min15.to_html(classes=classes),
                           table_min30=df_min30.to_html(classes=classes),
                           table_hour=df_hour.to_html(classes=classes),
                           table_day=df_day.to_html(classes=classes),
                           table_week=df_week.to_html(classes=classes),
                           table_month=df_month.to_html(classes=classes)
                           )


@app.before_request
def before_request():
    g.locale = str(get_locale())