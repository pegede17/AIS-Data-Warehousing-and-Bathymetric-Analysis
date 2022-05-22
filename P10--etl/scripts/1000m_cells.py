from math import ceil
from utils.database_connection import connect_to_db
import configparser


def create_3034_1000m_cells():
    config = configparser.ConfigParser()
    config.read('../application.properties')

    connection = connect_to_db()

    cur = connection.cursor()
    cur.execute(f"""
    CREATE TABLE dim_cell_3034_1000m(
        cell_id SERIAL PRIMARY KEY,
        columnx INTEGER,
        rowy INTEGER,
        boundary GEOMETRY
    );

    CREATE TABLE bridge_traj_sailing_cell_3034_1000m(
        cell_id INTEGER NOT NULL,
        trajectory_id BIGINT NOT NULL,
            
        FOREIGN KEY (cell_id)
            REFERENCES dim_cell_3034_1000m (cell_id)
            ON UPDATE CASCADE,
        FOREIGN KEY (trajectory_id)
            REFERENCES fact_trajectory_sailing (trajectory_id)
            ON UPDATE CASCADE,
        PRIMARY KEY (cell_id, trajectory_id)
    );

    With start_point as (
        SELECT {int(config["Map"]["southwestx"])} x_start, {int(config["Map"]["southwesty"])} y_start
    )
    INSERT INTO dim_cell_3034_1000m (columnx, rowy, boundary) 
        SELECT columnx, rowy,
        ST_MakeEnvelope(x_start + ((columnx - 1) * 1000),y_start + ((rowy - 1) * 1000),x_start + (columnx * 1000),y_start + (rowy * 1000),3034)
        FROM generate_series(1, {ceil(int(config["Map"]["columns"])/20)}) columnx, generate_series(1, {ceil(int(config["Map"]["rows"])/20)}) rowy, start_point;
    """)


def fill_3034_1000m_bridge_and_fact():
    config = configparser.ConfigParser()
    config.read('../application.properties')

    connection = connect_to_db()

    cur = connection.cursor()

    cur.execute(f"""
    ALTER TABLE bridge_traj_sailing_cell_3034_1000m DISABLE TRIGGER ALL;
    WITH raster as (
    SELECT ST_AddBand(ST_MakeEmptyRaster({ceil(int(config["Map"]["columns"])/20)},{ceil(int(config["Map"]["rows"])/20)},{int(config["Map"]["southwestx"])}::float,{int(config["Map"]["southwesty"])}::float,1000::float,1000::float,0::float,0::float,3034),
                    '8BUI'::text, 0, null) ras
    ),
    trajectory as(
    SELECT trajectory_id, coordinates
        FROM public.fact_trajectory_sailing
    ), cells as (
    SELECT (((rowy - 1) * {ceil(int(config["Map"]["columns"])/20)}) + columnx), trajectory_id as cell_id from(
    SELECT trajectory_id, (ST_WorldToRasterCoord(ras,(
                ST_PixelAsPoints(
                    ST_AsRaster(
                        ST_Transform(
                            ST_SetSRID(coordinates, 4326),3034), ras, '8BUI'::text,1,0,true))).geom)).*
    FROM raster, trajectory) foo)
    INSERT INTO bridge_traj_sailing_cell_3034_1000m
    SELECT * from cells;
    ALTER TABLE bridge_traj_sailing_cell_3034_1000m ENABLE TRIGGER ALL;
    """)
