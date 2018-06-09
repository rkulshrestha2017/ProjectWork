from flask import Flask
from flask import render_template
from pymongo import MongoClient
from flask import jsonify
import json
from bson import json_util
from bson.json_util import dumps
from flask_cors import CORS, cross_origin

app = Flask(__name__)

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DBS_NAME = 'diabetesdb'
COLLECTION_NAME = 'things'
FIELDS = {'Pregnancies': True, 'Glucose': True, 'BloodPressure': True, 'SkinThickness': True, 'Insulin': True, 'BMI': True, 'DiabetesPedigreeFunction': True, 'Age': True, 'Outcome': True, '_id': False}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/diabetesdb/things")
@cross_origin(origin='localhost',headers=['Content- Type','Authorization'])
def diabetesdb_things():
    connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
    collection = connection[DBS_NAME][COLLECTION_NAME]
    things = collection.find(projection=FIELDS)
    json_things = []
    for thing in things:
        json_things.append(thing)
    # json_registrar = json.dumps(json_registrar, default=json_util.default)
    connection.close()
    return jsonify(data = json_things)

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)