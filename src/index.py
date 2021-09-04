from flask import Flask, request, Response
import pandas as pd
from src.util.test_controller import controller
from src.projects.talltables_handler import model_handler, ingesterv2
import logging
logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', level=logging.NOTSET)

app = Flask(__name__)

def which_table():
    pass

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"



@app.route('/<tableid>', methods=['POST'])
def json_example(tableid):
    """ UNDER DEV """
    logging.info("starting..")
    request_data = request.get_json()
    try:
        logging.info("data goes into the controller...")
        tmp = controller(request_data)
        logging.info(tmp)
    except Exception as e:
        logging.error(e, flush=True)

    
    # tmp = controller(request_data)
    # print(request_data, flush=True)
    

    return "ok"

app.run(debug=True)