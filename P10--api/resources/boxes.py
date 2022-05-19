from flask_restful import Resource, reqparse, request, fields, marshal_with
import pandas as pd
import psycopg2
from models.viewDTO import ViewDTO
from util.queries import testSelect, testSelect2
from util.database import connect_via_ssh, connect_locally
import configparser
import re


class Boxes(Resource):
    def get(self):
        # Put quotes around ship types
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
            boundary_to_show = "boundary"
            dim_cell = "dim_cell_3034_1000m"
            boundary_to_intersect = "boundary"
            fact_cell = "fact_cell_3034_1000m"
        else:
            boundary_to_show = "boundary_50m"
            dim_cell = "dim_cell_3034"
            boundary_to_intersect = "boundary_1000m"
            fact_cell = "fact_cell_3034_50m"

        boxRaster = f"""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(t.*)::json)
            )
        FROM ( SELECT ST_Transform({boundary_to_show},4326), max_draught, min_draught, count, foo.cell_id, avg_draught
        FROM {dim_cell} d inner join
            (SELECT max(max_draught) max_draught,
                MIN(min_draught) min_draught,
                cell_id,
                SUM(trajectory_count) count,
                SUM(avg_draught * trajectory_count)/
                    CASE WHEN SUM(CASE WHEN avg_draught IS NOT NULL THEN trajectory_count ELSE 0 END) > 0
                        THEN SUM(CASE WHEN avg_draught IS NOT NULL THEN trajectory_count ELSE 0 END)
                        ELSE 1 END avg_draught
            from {fact_cell}
            WHERE cell_id in (SELECT cell_id
                            FROM   {dim_cell}
                            WHERE  ST_Intersects({boundary_to_intersect},
                            ST_Transform(ST_MakeEnvelope (
                            {request.args['southWestLong']}, {request.args['southWestLat']}, -- bounding
                            {request.args['northEastLong']}, {request.args['northEastLat']}, -- box limits
                            4326), 3034)))
            AND date_id BETWEEN {request.args['fromDate']} AND {request.args['toDate']}
            AND ship_type_id IN (SELECT ship_type_id from dim_ship_type WHERE ship_type IN ({ship_types}))
            AND type_of_mobile_id IN (SELECT type_of_mobile_id from dim_type_of_mobile WHERE mobile_type IN ({mobile_types}))
            {only_trusted_draught_query}
            GROUP BY cell_id) foo on foo.cell_id = d.cell_id
        ) as t(geom, maxDraught, minDraught, count, cellId, avgDraught);"""

        config = configparser.ConfigParser()
        config.read('../application.properties')

        if (config["Environment"]["development"] == "False"):
            with connect_via_ssh() as connection:
                df = pd.read_sql_query(boxRaster, connection)
        else:
            with connect_locally() as connection:
                df = pd.read_sql_query(boxRaster, connection)

        # with psycopg2.connect(database="speciale", user='<postgres', password='admin') as connection:

        # Return boxDTO, not made yet
        return df["json_build_object"].iloc[0]
