from flask import Flask, render_template
import scrinner
from stock import Stock
import pandas as pd

app = Flask(__name__)


DATA_STOCKS = scrinner.scrinner()


@app.route('/')
def hello_world():
    return home_page()


@app.route('/home')
def home_page():
    return render_template('home.html', table=DATA_STOCKS.to_html(classes='stocks_talbe'))


if __name__ == '__main__':
    app.run()
