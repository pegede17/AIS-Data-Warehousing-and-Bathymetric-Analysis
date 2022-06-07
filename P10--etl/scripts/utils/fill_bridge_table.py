from math import ceil
from utils.database_connection import connect_to_db
import configparser
from pygrametl.datasources import SQLSource
import pandas as pd
from sqlalchemy import create_engine


def fill_bridge_table(date):

    config = configparser.ConfigParser()
    config.read('../application.properties')

    connection = connect_to_db(config)

    cur = connection.cursor()

    print("Getting rows to insert")

    BRIDGE_TABLE_QUERY = f"""
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
    SELECT * from cells;"""

    bridge_data = SQLSource(connection=connection, query=BRIDGE_TABLE_QUERY)

    bridge_df = pd.DataFrame(bridge_data)

    engineString = f"""postgresql://{config["Database"]["dbuser"]}:{config["Database"]["dbpass"]}@{config["Database"]["hostname"]}:5432/{config["Database"]["dbname"]}"""
    engine = create_engine(engineString, executemany_mode='values_plus_batch')

    print("Filling bridge table")
    cur.execute("ALTER TABLE bridge_traj_sailing_cell_3034 DISABLE TRIGGER ALL;")
    connection.commit()

    bridge_df.to_sql('bridge_traj_sailing_cell_3034', index=False, con=engine,
                     if_exists='append', chunksize=10000)

    cur.execute("ALTER TABLE bridge_traj_sailing_cell_3034 ENABLE TRIGGER ALL;")

    connection.commit()
    cur.close()
    connection.close()
