from flask_restful import Resource, reqparse, fields, marshal_with
import pandas as pd
import psycopg2
from models.viewDTO import ViewDTO
from util.queries import testSelect, testSelect2

class View(Resource):
    @marshal_with(ViewDTO.getDTO())
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('bbox', required=True, type=list)
        parser.add_argument('zoom', required=True, type=int)

        args = parser.parse_args()

        with psycopg2.connect(database="ais_db", user='postgres', password='admin') as connection:
            df = pd.read_sql_query(testSelect, connection)

        if (args['zoom'] > 5):
            print("Initialize 50m call")
        else:
            print("Initialize 1000m cells")
        
        print(df)

        return ViewDTO(data=[{'totalTrajectories': 50, 'maxDraught': 50.25, 'geometry': 'testing'}, {'totalTrajectories': 502, 'maxDraught': 50.252, 'geometry': 'testing2'}])