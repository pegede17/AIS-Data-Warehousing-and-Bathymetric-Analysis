from flask_restful import Resource, reqparse, request, fields, marshal_with
import pandas as pd
import psycopg2
from models.viewDTO import ViewDTO
from util.queries import testSelect, testSelect2
from util.database import connect_via_ssh, connect_locally
import configparser
import re


class Histogram(Resource):
    def get(self):

        ship_types = "'" + request.args['shipTypes'] + "'"
        ship_types = re.sub("\,", "\',\'", ship_types)
        mobile_types = "'" + request.args['mobileTypes'] + "'"
        mobile_types = re.sub("\,", "\',\'", mobile_types)
        only_trusted_draught = request.args['onlyTrustedDraught']

        only_trusted_draught_query = ""
        if(only_trusted_draught == "true"):
            only_trusted_draught_query = "AND is_draught_trusted"

        zoom_level = int(request.args['zoomLevel'])

        if(zoom_level <= 13):
            fact_cell = "fact_cell_3034_1000m"
        else:
            fact_cell = "fact_cell_3034"

        histogram_query = f"""
              WITH histos as (
        SELECT
          histogram_draught,
          histogram_traj_speed
        from
          {fact_cell}
        where
          cell_id = {request.args['cellId']}
          AND date_id BETWEEN {request.args['fromDate']} AND {request.args['toDate']}
          AND ship_type_id IN (SELECT ship_type_id from dim_ship_type WHERE ship_type IN ({ship_types}))
          AND type_of_mobile_id IN (SELECT type_of_mobile_id from dim_type_of_mobile WHERE mobile_type IN ({mobile_types}))
          {only_trusted_draught_query}
        )
        SELECT
        ARRAY (
          SELECT
            sum(elem)
          FROM
            histos t,
            unnest(t.histogram_draught) WITH ORDINALITY x(elem, rn)
          GROUP BY
            rn
          ORDER BY
            rn
        ) hist_draught,
        ARRAY (
          SELECT
            sum(elem)
          FROM
            histos t,
            unnest(t.histogram_traj_speed) WITH ORDINALITY x(elem, rn)
          GROUP BY
            rn
          ORDER BY
            rn
        ) hist_speed;"""

        config = configparser.ConfigParser()
        config.read('application.properties')

        if (config["Environment"]["development"] == "False"):
            with connect_via_ssh() as connection:
                df = pd.read_sql_query(histogram_query, connection)
        else:
            with connect_locally() as connection:
                df = pd.read_sql_query(histogram_query, connection)

        # with psycopg2.connect(database="speciale", user='<postgres', password='admin') as connection:

        # Return boxDTO, not made yet
        return df["hist_draught"].iloc[0]
