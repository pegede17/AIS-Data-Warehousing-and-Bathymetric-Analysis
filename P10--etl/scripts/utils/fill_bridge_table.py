from math import ceil
from utils.database_connection import connect_to_db
import configparser
from pygrametl.datasources import SQLSource
import pandas as pd
from sqlalchemy import create_engine
from pygrametl.tables import BulkFactTable
import pygrametl


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
    SELECT (((rowy - 1) * {ceil(int(config["Map"]["columns"]))}) + columnx) as cell_id, trajectory_id as trajectory_id from(
    SELECT trajectory_id, (ST_WorldToRasterCoord(ras,(
                ST_PixelAsPoints(
                    ST_AsRaster(
                        ST_Transform(
                            ST_SetSRID(coordinates, 4326),3034), ras, '8BUI'::text,1,0,true))).geom)).*
    FROM raster, trajectory) foo)
    SELECT * from cells;"""

    bridge_data = SQLSource(connection=connection, query=BRIDGE_TABLE_QUERY)

    # bridge_df = pd.DataFrame(bridge_data)

    # engineString = f"""postgresql://{config["Database"]["dbuser"]}:{config["Database"]["dbpass"]}@{config["Database"]["hostname"]}:5432/{config["Database"]["dbname"]}"""
    # engine = create_engine(engineString, executemany_mode='values_plus_batch')

    print("Filling bridge table")
    cur.execute("""ALTER TABLE bridge_traj_sailing_cell_3034 DISABLE TRIGGER ALL;
                    ALTER TABLE bridge_traj_sailing_cell_3034 DROP CONSTRAINT bridge_traj_sailing_cell_3034_geo_id_fkey;
                    ALTER TABLE bridge_traj_sailing_cell_3034 DROP CONSTRAINT bridge_traj_sailing_cell_3034_pkey;
                    ALTER TABLE bridge_traj_sailing_cell_3034 DROP CONSTRAINT bridge_traj_sailing_cell_3034_trajectory_id_fkey;
                """)
    connection.commit()

    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        cursor = connection.cursor()
        cursor.copy_from(file=filehandle, table=name, sep=fieldsep, null=str(nullval),
                         columns=attributes)

    dw_conn_wrapper = pygrametl.ConnectionWrapper(
        connection=connection)

    bridge_table = BulkFactTable(
        name='bridge_traj_sailing_cell_3034',
        keyrefs=['trajectory_id', 'cell_id'],
        measures=[],
        bulkloader=pgbulkloader,
        fieldsep=',',
        rowsep='\\r\n',
        nullsubst=str(None),
        bulksize=100000,
        usefilename=False
    )

    i = 0
    for row in bridge_data:
        i += 1
        if (i % 100000 == 0):
            print("Commiting:", end="")
            dw_conn_wrapper.commit()
            print(i)
        bridge_table.insert(row)

    # bridge_df.to_sql('bridge_traj_sailing_cell_3034', index=False, con=engine,
    #                 #  if_exists='append', chunksize=100000)

    cur.execute("""ALTER TABLE bridge_traj_sailing_cell_3034 ENABLE TRIGGER ALL;
                    ALTER TABLE bridge_traj_sailing_cell_3034 ADD CONSTRAINT bridge_traj_sailing_cell_3034_geo_id_fkey
                        FOREIGN KEY (cell_id)
                        REFERENCES dim_cell_3034_1000m (cell_id)
                        ON UPDATE CASCADE;
                    ALTER TABLE bridge_traj_sailing_cell_3034 ADD CONSTRAINT bridge_traj_sailing_cell_3034_trajectory_id_fkey
                        FOREIGN KEY (trajectory_id)
                        REFERENCES fact_trajectory_sailing (trajectory_id)
                        ON UPDATE CASCADE;
                    ALTER TABLE bridge_traj_sailing_cell_3034 ADD CONTRAINT bridge_traj_sailing_cell_3034_pkey
                        PRIMARY KEY (cell_id, trajectory_id);
                    """)

    connection.commit()
    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()
    cur.close()
    connection.close()
