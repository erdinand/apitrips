from flask import Flask, request, jsonify
from database_handler import DatabaseHandler
from flask_sock import Sock
import time
import json


app = Flask(__name__)
sock = Sock(app)
with open("config.json", "r") as f:
    config = json.load(f)
db = DatabaseHandler('trips', config)

@app.route('/load', methods=["GET", "POST"])
def load():
    """
    This function is responsible for instanciating the class that will load the CSV file, 
    transform some columns, detect some groups and then load a SQLite database.

    DatabaseHandler was transformed in an extension of Thread because the idea was start
    all loading process without waiting the answer. This way, the API would be able to 
    just return a confirmation about the beginning of the process and the client would be
    able to just receive notifications about the loading status. 
    """
    global db, config
    db = DatabaseHandler('trips', config)
    db.start()
    status = "Loading process has been started! Subscribe to /load-status and follow the loading status."
    return jsonify({"status": status})


@sock.route('/load-status')
def status(sock):
    """
    Using a web socket was the only way I found to inform the client about 
    the loading status without using polling. Although I ended up doing a kind of
    polling here to check loading_status property of the class, this approach 
    allows the server to send messages to the client, like a subscription,
    and the client does not need to poll this information from this endpoint. 
    """
    global db
    while True:
        msg = ""
        if len(db.loading_status) > 0:
            msg = db.loading_status.pop(0)
            sock.send(msg)
        time.sleep(1)


@app.route('/weekly-trips-average/by-region', methods=["POST"])
def trips_average_by_region():
    """
    This function just takes the parameter region received in the JSON body and sends 
    it to the function that does the real work. The idea here was to separate the
    endpoints related to the weekly average number of trips. I think it was simpler
    and clearer creating two functions and two endpoints, instead of only
    one endpoint handling more complex parameters to realize which type was requested.
    """
    params = request.json
    db = DatabaseHandler('trips', config)
    result = db.get_weekly_avg_qt_trips_by_region(params["region"])
    return jsonify({"result": result})


@app.route('/weekly-trips-average/by-bounding-box', methods=["POST"])
def trips_average_by_bounding_box():
    """
    This function just takes the JSON body that contains the bounding box coordinates  
    and sends it to the function that does the real work. The idea here was to separate 
    the endpoints related to the weekly average number of trips. I think it was simpler
    and clearer creating two functions and two endpoints, instead of only
    one endpoint handling more complex parameters to realize which type was requested.
    """
    params = request.json
    db = DatabaseHandler('trips', config)
    result = db.get_weekly_avg_qt_trips_by_bounding_box(params)
    return jsonify({"result": result})


if __name__ == '__main__':
    app.run('')
