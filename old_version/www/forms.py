from flask_wtf import FlaskForm
from wtforms import SelectField
from wtforms.validators import InputRequired

from config import intervals


class ChooseInterval(FlaskForm):
    interval = SelectField('Intervals', choices=[i.value for i in intervals], validators=[InputRequired()])
