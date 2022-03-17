from datetime import datetime, timedelta
from math import ceil

import pandas as pd
import pygrametl
from pygrametl.datasources import SQLSource
from sqlalchemy import create_engine
from pyproj import Transformer


from database_connection import connect_to_local, connect_via_ssh
from helper_functions import create_audit_dimension

MAX_COLUMNS = 22000
MAX_ROWS = 16000


def clean_data(config, date_id):
    if (config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()

    engineString = f"""postgresql://{config["Database"]["dbuser"]}:{config["Database"]["dbpass"]}@{config["Database"]["hostname"]}:5432/{config["Database"]["dbname"]}"""
    engine = create_engine(engineString)

    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    START_TIME = datetime.today()

    audit_dimension = create_audit_dimension()

    audit_obj = {'timestamp': datetime.now(),
                 'processed_records': 0,
                 'inserted_records': 0,
                 'etl_duration': timedelta(seconds=1),
                 'source_system': config["Audit"]["source_system"],
                 'etl_version': config["Audit"]["elt_version"],
                 'table_name': f"fact_ais_clean",
                 'description': f"Date: {date_id}"}

    audit_id = audit_dimension.insert(audit_obj)

    cur = connection.cursor()

    DISABLE_TRIGGERS = f"""
        ALTER TABLE fact_ais_clean DISABLE TRIGGER ALL;
    """

    ENABLE_TRIGGERS = f"""
        ALTER TABLE fact_ais_clean ENABLE TRIGGER ALL;	
    """

    INITIAL_CLEAN_QUERY = f"""
    SELECT DISTINCT ON (eta_date_id, eta_time_id, ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, 
                    navigational_status_id, cargo_type_id, coordinate, draught, rot, sog, cog, heading)
            latitude, longitude, mmsi, fact_ais.type_of_mobile_id, fact_id, eta_date_id, eta_time_id, fact_ais.ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, navigational_status_id, cargo_type_id, coordinate, draught, rot, sog, cog, heading
        FROM fact_ais INNER JOIN public.dim_ship on fact_ais.ship_id = dim_ship.ship_id, public.danish_waters
        WHERE 
            (draught < 28.5 OR draught IS NULL)
            AND width < 75
            AND length < 488
            AND mmsi > 99999999
            AND mmsi < 1000000000
            AND ST_Contains(geom ,coordinate::geometry)
    """

    # Disable triggers during load for efficiency
    cur.execute(DISABLE_TRIGGERS)

    # Retrieve the points with initial cleaning rules applied directly to where conditions
    cleaned_data = SQLSource(connection=connection, query=INITIAL_CLEAN_QUERY)

    transformer = Transformer.from_crs("epsg:4326", "epsg:32632")

    def set_cell_row_and_column(data):
        pass

    ais_df = pd.DataFrame(cleaned_data)
    print("Creating initial commit!!")
    connection.commit()  # Required in order to release locks
    ships_grouped = ais_df.groupby(by=['mmsi'])

    dict_updated_ships = {}

    for mmsi, ship_data in ships_grouped:
        print(len(ship_data))
        if len(ship_data) < 2:  # We need to have at least 2 rows to choose from
            continue

        # Create a sequence and count the number of occurrences for each mobile type recorded
        mobile_type_count = ship_data['type_of_mobile_id'].squeeze(
        ).value_counts()

        # Define variable based on the mobile type that have occurred the most
        best_type = mobile_type_count.reset_index(0)['index'][0]

        # Define the ship_id that have been reported with the previously defined mobile type
        seq_ship_type = ship_data[ship_data['type_of_mobile_id'] == best_type].squeeze()[
            'ship_id'].value_counts()
        best_ship_id = seq_ship_type.reset_index(0)['index'][0]

        # Some ships have not reported multiple ship_id's. Therefore, we want to store a list of those that have, so
        # that we know which ships to update later. That is done by flagging them and adding to a dictionary.
        # flagged = len(seq_ship_type.value_counts()) > 1
        # if flagged:

        dict_updated_ships[mmsi] = best_ship_id

    # Iterate through all the ships that require an update and update their ship_id for the dataframe
    # for ship in dict_updated_ships:
    #     print("Changing ship value of: " + str(ship))
    #     ais_df.loc[ais_df['mmsi'] == ship,
    #                'ship_id'] = dict_updated_ships[ship]
    
    for index, data in ais_df.iterrows():
        shipExist = dict_updated_ships[data['mmsi']]

        if (shipExist):
            ais_df.at[index, 'ship_id'] = shipExist

        # set_cell_row_and_column(ship_data)
        x, y = transformer.transform(data['latitude'], data['longitude'])

        columnx, rowy = ceil((x - 0) /
                             50), ceil((y - 5900000) / 50)
        cell_id = (columnx - 1) * MAX_ROWS + rowy
        ais_df.at[index, 'cell_id'] = 0 # TODO: Change this value to cell_id when calculation is correct and data exists
        # print(cell_id)
        print("Iterating through ais_df")

    # Remove mmsi column. It was only required during computation
    del ais_df['mmsi']
    ais_df['audit_id'] = audit_id
    print("AIS_DF to SQL is being called!!")
    ais_df.to_sql('fact_ais_clean', con=engine,
                  chunksize=500000, index=False, if_exists='append')
    print("DONE!!! AIS_DF_TO_SQL HAS BEEN CALLED!!")

    END_TIME = datetime.today()

    audit_obj['processed_records'] = len(ais_df)
    audit_obj['inserted_records'] = len(ais_df)
    audit_obj['etl_duration'] = END_TIME - START_TIME
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    print("Creating connection commit!!")
    connection.commit()
    cur.execute(ENABLE_TRIGGERS)

    print("Creating __dw_conn commit!!")
    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()
