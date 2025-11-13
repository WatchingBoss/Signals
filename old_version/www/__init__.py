from flask import Flask
from config import Config
from flask_moment import Moment
from flask_babel import Babel


app = Flask(__name__)
app.config.from_object(Config)
moment = Moment(app)
babel = Babel(app)


from www import routes