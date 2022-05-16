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
            only_trusted_draught_query = "AND is_trusted_draught"

        zoom_level = int(request.args['zoomLevel'])

        conditions = f"""
        ST_Transform(ST_MakeEnvelope (
                            {request.args['southWestLong']}, {request.args['southWestLat']}, -- bounding
                            {request.args['northEastLong']}, {request.args['northEastLat']}, -- box limits
                            4326), 3034)))
        AND date_id BETWEEN {request.args['fromDate']} AND {request.args['toDate']}
        AND ship_type_id IN (SELECT ship_type_id from dim_ship_type WHERE ship_type IN ({ship_types}))
        AND type_of_mobile_id IN (SELECT type_of_mobile_id from dim_type_of_mobile WHERE mobile_type IN ({mobile_types}))
        {only_trusted_draught_query}"""

        if(zoom_level <= 13):
            boxRaster = f"""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(t.*)::json)
            )
        FROM ( SELECT ST_Transform(boundary,4326), max_draught from dim_cell_3034_1000m d inner join
        (SELECT max(max_draught) max_draught, cell_id
        from fact_cell_3034_1000m
        WHERE cell_id in (SELECT cell_id
                    FROM   dim_cell_3034_1000m
                    WHERE  ST_Intersects(boundary,{conditions}
        GROUP BY cell_id) foo on foo.cell_id = d.cell_id
             ) as t(geom, draught);
        """
        else:
            boxRaster = f"""SELECT json_build_object(
                            'type', 'FeatureCollection',
                            'features', json_agg(ST_AsGeoJSON(t.*)::json)
                            )
                        FROM ( SELECT ST_Transform(boundary_50m,4326), max_draught from dim_cell_3034 d inner join
                        (SELECT max(max_draught) max_draught, cell_id
                        from fact_cell_3034
                        WHERE cell_id in (SELECT cell_id
                                    FROM   dim_cell_3034
                                    WHERE  ST_Intersects(boundary_1000m, {conditions}
                        GROUP BY cell_id) foo on foo.cell_id = d.cell_id
                            ) as t(geom, draught);"""

        config = configparser.ConfigParser()
        config.read('application.properties')

        if (config["Environment"]["development"] == "False"):
            with connect_via_ssh() as connection:
                df = pd.read_sql_query(boxRaster, connection)
        else:
            with connect_locally() as connection:
                df = pd.read_sql_query(boxRaster, connection)

        # with psycopg2.connect(database="speciale", user='<postgres', password='admin') as connection:

        # Return boxDTO, not made yet
        return df["json_build_object"].iloc[0]
