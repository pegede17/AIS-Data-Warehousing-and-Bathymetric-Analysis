from flask import Flask
from flask_restful import Resource, Api
from resources.view import View

app = Flask(__name__)
api = Api(app)

api.add_resource(View, '/view')

if __name__ == '__main__':
    app.run(debug=True)