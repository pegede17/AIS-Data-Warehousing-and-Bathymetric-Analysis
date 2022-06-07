from math import ceil
from utils.database_connection import connect_to_db
import configparser


def fill_bridge_table(date):

    config = configparser.ConfigParser()
    config.read('../application.properties')

    connection = connect_to_db(config)

    cur = connection.cursor()

    FILL_BRIDGE_TABLE_QUERY = f"""
    WITH raster as (
    SELECT ST_AddBand(ST_MakeEmptyRaster({ceil(int(config["Map"]["columns"]))},{ceil(int(config["Map"]["rows"]))},{int(config["Map"]["southwestx"])}::float,{int(config["Map"]["southwesty"])}::float,50::float,50::float,0::float,0::float,3034),
                    '8BUI'::text, 0, null) ras
    ),
    trajectory as(
    SELECT trajectory_id, coordinates
        FROM public.fact_trajectory_sailing
        WHERE date_start_id = {date}
    ), cells as (
    SELECT (((rowy - 1) * {ceil(int(config["Map"]["columns"]))}) + columnx), trajectory_id as cell_id from(
    SELECT trajectory_id, (ST_WorldToRasterCoord(ras,(
                ST_PixelAsPoints(
                    ST_AsRaster(
                        ST_Transform(
                            ST_SetSRID(coordinates, 4326),3034), ras, '8BUI'::text,1,0,true))).geom)).*
    FROM raster, trajectory) foo)
    INSERT INTO bridge_traj_sailing_cell_3034
    SELECT * from cells;"""

    print("Filling bridge table")
    cur.execute("ALTER TABLE bridge_traj_sailing_cell_3034 DISABLE TRIGGER ALL;")
    connection.commit()
    cur.execute(FILL_BRIDGE_TABLE_QUERY)
    connection.commit()
    cur.execute("ALTER TABLE bridge_traj_sailing_cell_3034 ENABLE TRIGGER ALL;")

    connection.commit()
    cur.close()
    connection.close()
