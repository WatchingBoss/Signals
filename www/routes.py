from flask import render_template, url_for, g
from flask_babel import get_locale

from www import app

import pandas as pd
import os
from datetime import datetime, timedelta


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html', title='Signals')


@app.route('/overview')
def overview():
    df = pd.read_pickle(os.path.join('data', 'overview.pkl'))
    return render_template('overview.html', title='Overview',
                           table=df.to_html(classes='table table-striped table-bordered table-sm'))


@app.route('/indicators/<interval>')
def indicators(interval):
    df = pd.read_pickle(os.path.join('data', interval + '.pkl'))
    return render_template('indicators.html', title='Indicators',
                           df=df, interval=interval
                           )


@app.before_request
def before_request():
    g.locale = str(get_locale())


@app.template_filter('check_value')
def check_value(value) -> str:
    if isinstance(value, datetime):
        date = value + timedelta(hours=3)
        return date.strftime("%y-%m-%d %H:%M:%S")
    return value
