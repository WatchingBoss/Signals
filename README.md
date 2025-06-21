# Stock Technical Analysis and Web Viewer

This project fetches stock market data using the Tinkoff Invest API and Finviz, performs technical analysis, and provides a Flask web interface to view the data and indicators.

## Project Structure

- `main_ta.py`: Main script for running the technical analysis data processing.
- `main_www.py`: Main script for running the Flask web application.
- `config.py`: Project configuration settings.
- `ta/`: Contains modules related to technical analysis, data scraping, and processing.
    - `scanner.py`: Core class for scanning stocks, fetching data, and calculating indicators.
    - `scraper.py`: Functions for scraping data from web sources like Finviz.
    - `stock.py`: Represents a stock and its associated data.
    - `myhelper.py`: Utility functions, likely for API client initialization.
- `www/`: Contains modules for the Flask web application.
    - `routes.py`: Defines the web application routes.
    - `forms.py`: Defines web forms.
    - `templates/`: HTML templates for the web interface.
    - `static/`: Static assets (CSS, JavaScript).
- `data/`: Directory for storing data files (e.g., HDF5 candles, summaries).
- `requirements.txt`: Python dependencies.
- `.flake8`: Configuration for the Flake8 linter.

## Setup

1.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\\Scripts\\activate
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set up environment variables:**
    Create a `.env` file in the project root or set the following environment variables:
    - `SECRET_KEY`: A secret key for Flask session management.
    - `TINKOFF_INVEST_TOKEN`: Your Tinkoff Invest API token (if `myhelper.py` or `tinvest` requires it directly from env).
    - `DATABASE_URI` (Optional): To override the default SQLite database path.

    *Note: Review `config.py` and `ta/myhelper.py` to confirm all required environment variables.*

## Running the Application

### Data Processing

To fetch and process the latest stock data:
```bash
python main_ta.py
```
*(This may need to be run regularly or as a scheduled task to keep data up-to-date.)*

### Web Application

To start the Flask web server:
```bash
flask run
# or
python main_www.py
```
*(Ensure that `main_www.py` is configured to run the Flask development server or a production server like Gunicorn.)*

## TODO

- Further refine setup instructions based on API key requirements.
- Add more detailed usage instructions.
- Document the data processing pipeline.
