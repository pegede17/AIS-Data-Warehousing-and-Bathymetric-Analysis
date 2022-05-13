from flask import Flask
from flask_restful import Resource, Api
from resources.view import View
from resources.boxes import Boxes
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app, support_credentials=True)

api = Api(app)

api.add_resource(View, '/view')
api.add_resource(Boxes, '/boxes')

# 'flask run' to run api
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)


@app.route("/boxes")
def fetch_boxes():
    return "lets get them boxes from the server"
