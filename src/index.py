from flask import Flask, request, Response
import pandas as pd
from src.util.test_controller import controller
from src.projects.talltables_handler import model_handler, ingesterv2


app = Flask(__name__)

def which_table():
    pass

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route('/<tableid>', methods=['POST'])
def json_example(tableid):
    """ UNDER DEV """
    print("starting...")
    request_data = request.get_json()
    
    # tmp = controller(request_data)
    print(tableid, flush=True)
    

    return "ok"

app.run(debug=True)