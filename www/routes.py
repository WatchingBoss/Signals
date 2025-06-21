import logging
from flask import render_template, redirect, url_for, g, request, current_app
from flask_babel import get_locale

from www import app # Assuming 'app' is created in www.__init__.py
from www.forms import ChooseInterval

import pandas as pd
import os
from datetime import datetime, timedelta

# Setup logger for this module
logger = logging.getLogger(__name__)
# Basic logging configuration should be done in app creation (e.g. main_www.py or www.__init__.py)
# For example: logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s : %(message)s')
# If not configured at app level, Flask's default logger or root logger might be used.


@app.before_request
def log_request_info():
    """Log information about each incoming request."""
    # Using current_app.logger if Flask's app-specific logger is configured and preferred.
    # Otherwise, using the module-level logger.
    effective_logger = current_app.logger if hasattr(current_app, 'logger') else logger

    effective_logger.debug(f"Request: {request.method} {request.path}")
    effective_logger.debug(f"Request Headers: {request.headers}")
    effective_logger.debug(f"Request Args: {request.args}")
    effective_logger.debug(f"Request Form Data: {request.form}")
    # Log g.locale setting from the original before_request
    g.locale = str(get_locale())
    effective_logger.debug(f"Set g.locale: {g.locale}")


@app.route('/')
@app.route('/index')
def index():
    logger.info("Serving index page.")
    try:
        return render_template('index.html', title='Signals')
    except Exception as e:
        logger.error(f"Error rendering index page: {e}", exc_info=True)
        # Basic error response for now, will be improved with error pages
        return "An internal error occurred.", 500


@app.route('/overview')
def overview():
    logger.info("Serving overview page.")
    try:
        overview_file_path = os.path.join(current_app.config.get('DATA_DIR', 'data'), 'overview', 'summery.h5')
        if not os.path.exists(overview_file_path):
            logger.error(f"Overview file not found: {overview_file_path}")
            return "Overview data is not available.", 404
        df = pd.read_hdf(overview_file_path, key='df')
        return render_template('overview.html', title='Overview', df=df)
    except FileNotFoundError:
        logger.error(f"Overview file not found during read: {overview_file_path}", exc_info=True)
        return "Overview data is not available (file not found).", 404
    except Exception as e:
        logger.error(f"Error serving overview page: {e}", exc_info=True)
        return "An internal error occurred while loading overview data.", 500


@app.route('/indicators/<interval>', methods=['GET'])
def indicators(interval):
    logger.info(f"Serving indicators page for interval: {interval}")
    try:
        form = ChooseInterval(interval=interval) # Assuming ChooseInterval is a FlaskForm
        indicators_file_path = os.path.join(current_app.config['SUM_DIR'], f"{interval}.h5")
        if not os.path.exists(indicators_file_path):
            logger.error(f"Indicators file not found: {indicators_file_path}")
            return f"Indicator data for interval '{interval}' is not available.", 404

        df = pd.read_hdf(indicators_file_path, key='df')
        return render_template('indicators.html', title='Indicators',
                               df=df, interval=interval, form=form)
    except FileNotFoundError:
        logger.error(f"Indicators file not found during read: {indicators_file_path}", exc_info=True)
        return f"Indicator data for interval '{interval}' is not available (file not found).", 404
    except Exception as e:
        logger.error(f"Error serving indicators page for interval {interval}: {e}", exc_info=True)
        return "An internal error occurred while loading indicator data.", 500


@app.route('/stock/<ticker>/<interval>', methods=['GET'])
def stock(ticker, interval):
    logger.info(f"Serving stock page for ticker: {ticker}, interval: {interval}")
    try:
        form = ChooseInterval(interval=interval)
        stock_file_path = os.path.join(current_app.config['CANDLES_DIR'], f"{ticker}_{interval}.h5")

        if not os.path.exists(stock_file_path):
            logger.error(f"Stock data file not found: {stock_file_path}")
            return f"Data for stock '{ticker}' at interval '{interval}' is not available.", 404

        df = pd.read_hdf(stock_file_path, key='df')
        df = df.sort_values(by='Time', ascending=False, ignore_index=True).iloc[:50]
        return render_template('stock.html', title=f'Stock {ticker}',
                               df=df, interval=interval, form=form, ticker=ticker)
    except FileNotFoundError:
        logger.error(f"Stock data file not found during read: {stock_file_path}", exc_info=True)
        return f"Data for stock '{ticker}' at interval '{interval}' is not available (file not found).", 404
    except Exception as e:
        logger.error(f"Error serving stock page for {ticker}/{interval}: {e}", exc_info=True)
        return "An internal error occurred while loading stock data.", 500


# The original before_request for g.locale is now part of log_request_info
# @app.before_request
# def before_request():
#     g.locale = str(get_locale())


@app.template_filter('time_format')
def time_format(value) -> str:
    if isinstance(value, datetime):
        # Assuming the datetime object from HDF5 is timezone-aware (e.g., UTC)
        # If it's naive, you might need to localize it first or assume a specific timezone.
        # The original code added 3 hours, implying it might be converting from UTC to Moscow Time (MSK)
        # For robustness, it's better to handle timezones explicitly.
        # If 'value' is UTC:
        # from dateutil import tz
        # msk_tz = tz.gettz('Europe/Moscow')
        # local_time = value.astimezone(msk_tz)
        # return local_time.strftime("%y-%m-%d %H:%M:%S")
        # For now, retaining original behavior with a comment:
        date_adjusted = value + timedelta(hours=3) # Assuming this is for a specific timezone adjustment (e.g. UTC to MSK)
        return date_adjusted.strftime("%y-%m-%d %H:%M:%S")
    return str(value) # Ensure non-datetime values are also stringified


# Error Handlers
@app.errorhandler(404)
def not_found_error(error):
    logger.info(f"Handling 404 error for path: {request.path}. Error: {error}")
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    # For 500 errors, the actual exception might not be passed directly to 'error' argument
    # by default, but Flask logs it. We've already added logging in route handlers.
    # If using something like Flask-SQLAlchemy, db.session.rollback() might be needed here.
    logger.error(f"Handling 500 error for path: {request.path}. Error details should be in preceding logs. Error: {error}")
    return render_template('500.html'), 500

# It's also good practice to handle other common errors, e.g., 403, 400, etc.
# For now, 404 and 500 are the most critical.
