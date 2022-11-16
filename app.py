import flask
from flask_cors import CORS

# create flask app
app = flask.Flask(__name__)
CORS(app)
