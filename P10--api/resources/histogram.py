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
    date_id BETWEEN {request.args['fromDate']} AND {request.args['toDate']}
    and cell_id = {request.args['cellId']}
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
