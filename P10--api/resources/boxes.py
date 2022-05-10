from flask_restful import Resource, reqparse, fields, marshal_with
import pandas as pd
import psycopg2
from models.viewDTO import ViewDTO
from util.queries import testSelect, testSelect2, boxRaster

class Boxes(Resource):
    def get(self):
        with psycopg2.connect(database="ais_db", user='postgres', password='admin') as connection:
            df = pd.read_sql_query(boxRaster, connection)

        print(df)

        # Return boxDTO, not made yet
        return