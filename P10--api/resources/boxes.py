from flask_restful import Resource, reqparse, fields, marshal_with
import pandas as pd
import psycopg2
from models.viewDTO import ViewDTO
from util.queries import testSelect, testSelect2, boxRaster
from util.database import connect_via_ssh


class Boxes(Resource):
    def get(self):

        with connect_via_ssh() as connection:
            df = pd.read_sql_query(boxRaster, connection)

        # with psycopg2.connect(database="speciale", user='<postgres', password='admin') as connection:

        # Return boxDTO, not made yet
        return df["json_build_object"].iloc[0]
