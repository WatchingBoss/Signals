from flask import Flask
from config import Config
from flask_moment import Moment
from flask_babel import Babel
import logging


app = Flask(__name__)
app.config.from_object(Config)
moment = Moment(app)
babel = Babel(app)

if not app.debug: # Only configure logging if not in debug mode (debug often has its own)
    # Or, more generally, always configure it if you want consistent logging
    # For simplicity, let's configure a basic handler if no handlers are present.
    if not app.logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        # You can set the level for the app.logger or for the handler
        app.logger.addHandler(stream_handler)
        app.logger.setLevel(logging.INFO) # Set default level for app logger
        # You might also want to set level for werkzeug logger if needed
        # logging.getLogger('werkzeug').setLevel(logging.INFO)
        app.logger.info("Flask app logger configured.")


from www import routes