from flask_restful import Resource, reqparse, request, fields, marshal_with
import pandas as pd
import psycopg2
from models.viewDTO import ViewDTO
from util.queries import testSelect, testSelect2
from util.database import connect_via_ssh


class Boxes(Resource):
    def get(self):

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
                    WHERE  ST_Intersects(boundary,

                        ST_Transform(ST_MakeEnvelope (
                            {request.args['southWestLong']}, {request.args['southWestLat']}, -- bounding
                            {request.args['northEastLong']}, {request.args['northEastLat']}, -- box limits
                            4326), 3034)))
        AND date_id BETWEEN {request.args['startDate']} AND {request.args['endDate']}
        GROUP BY cell_id) foo on foo.cell_id = d.cell_id
             ) as t(geom, draught);
        """

        with connect_via_ssh() as connection:
            df = pd.read_sql_query(boxRaster, connection)

        # with psycopg2.connect(database="speciale", user='<postgres', password='admin') as connection:

        # Return boxDTO, not made yet
        return df["json_build_object"].iloc[0]
