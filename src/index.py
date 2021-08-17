from flask import Flask, request, Response
import pandas as pd
from src.util.test_controller import controller
from src.projects.talltables_handler import model_handler, ingesterv2


app = Flask(__name__)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route('/', methods=['POST'])
def json_example():
    """ UNDER DEV """
    request_data = request.get_json()
    
    tmp = controller(request_data)
    print(tmp.checked.info(), flush=True)

    return "ok"

app.run(debug=True)