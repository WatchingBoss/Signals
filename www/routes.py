from flask import render_template, url_for
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
    df = pd.read_pickle(os.path.join('data', 'indicators.pkl'))
    return render_template('indicators.html', title='Indicators',
                           table=df.to_html(classes='table table-striped table-bordered table-sm'))
