from datetime import datetime, timedelta

import pandas as pd
import pygrametl
from pygrametl.datasources import SQLSource
from sqlalchemy import create_engine

from database_connection import connect_to_local, connect_via_ssh
from helper_functions import create_audit_dimension

VERSION = 1


def clean_data(config, date_id):
    if (config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()

    engineString = f"""postgresql://{config["Database"]["dbuser"]}:{config["Database"]["dbpass"]}@{config["Database"]["hostname"]}:5432/{config["Database"]["dbname"]}"""
    engine = create_engine(engineString)

    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    START_TIME = datetime.today()

    CREATE_DATABASE_QUERY = f"""
    CREATE TABLE IF NOT EXISTS fact_ais_clean_v{VERSION} (
            fact_id BIGSERIAL NOT NULL PRIMARY KEY,
            eta_date_id INTEGER NOT NULL DEFAULT 0,
            eta_time_id INTEGER NOT NULL DEFAULT 0,
            ship_id INTEGER NOT NULL,
            ts_date_id INTEGER NOT NULL,
            ts_time_id INTEGER NOT NULL,
            data_source_type_id INTEGER NOT NULL DEFAULT 0,
            destination_id INTEGER NOT NULL DEFAULT 0,
            type_of_mobile_id INTEGER NOT NULL DEFAULT 0,
            navigational_status_id INTEGER NOT NULL DEFAULT 0,
            cargo_type_id INTEGER NOT NULL DEFAULT 0,
            type_of_position_fixing_device_id INTEGER NOT NULL DEFAULT 0,
            ship_type_id INTEGER NOT NULL DEFAULT 0,
            coordinate geography(point) NOT NULL,
            draught DOUBLE PRECISION,
            rot DOUBLE PRECISION,
            sog DOUBLE PRECISION,
            cog DOUBLE PRECISION,
            heading SMALLINT,
            audit_id INTEGER NOT NULL,
    
            FOREIGN KEY (audit_id)
                REFERENCES dim_audit (audit_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
            FOREIGN KEY (eta_date_id)
                REFERENCES dim_date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (eta_time_id)
                REFERENCES dim_time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ship_id)
                REFERENCES dim_ship (ship_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ts_date_id)
                REFERENCES dim_date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ts_time_id)
                REFERENCES dim_time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (data_source_type_id)
                REFERENCES dim_data_source_type (data_source_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (destination_id)
                REFERENCES dim_destination (destination_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_mobile_id)
                REFERENCES dim_type_of_mobile (type_of_mobile_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (navigational_status_id)
                REFERENCES dim_navigational_status (navigational_status_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (cargo_type_id)
                REFERENCES dim_cargo_type (cargo_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_position_fixing_device_id)
                REFERENCES dim_type_of_position_fixing_device (type_of_position_fixing_device_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ship_type_id)
                REFERENCES dim_ship_type (ship_type_id)
                ON UPDATE CASCADE
        );
    CREATE INDEX IF NOT EXISTS ts_date_id ON fact_ais_clean_v{VERSION} (ts_date_id);
    """

    audit_dimension = create_audit_dimension()

    audit_obj = {'timestamp': datetime.now(),
                 'processed_records': 0,
                 'inserted_records': 0,
                 'etl_duration': timedelta(seconds=1),
                 'source_system': config["Audit"]["source_system"],
                 'etl_version': config["Audit"]["elt_version"],
                 'table_name': f"fact_ais_clean_v{VERSION}",
                 'description': f"Date: {date_id}"}

    audit_id = audit_dimension.insert(audit_obj)

    cur = connection.cursor()
    cur.execute(CREATE_DATABASE_QUERY)

    DISABLE_TRIGGERS = f"""
        ALTER TABLE fact_ais_clean_v{VERSION} DISABLE TRIGGER ALL;
    """

    ENABLE_TRIGGERS = f"""
        ALTER TABLE fact_ais_clean_v{VERSION} ENABLE TRIGGER ALL;	
    """

    INITIAL_CLEAN_QUERY = f"""
    SELECT DISTINCT ON (eta_date_id, eta_time_id, ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, 
                    navigational_status_id, cargo_type_id, coordinate, draught, rot, sog, cog, heading)
            mmsi, fact_ais.type_of_mobile_id, fact_id, eta_date_id, eta_time_id, fact_ais.ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, navigational_status_id, cargo_type_id, coordinate, draught, rot, sog, cog, heading
        FROM fact_ais INNER JOIN public.dim_ship on fact_ais.ship_id = dim_ship.ship_id, public.danish_waters
        WHERE 
            (draught < 28.5 OR draught IS NULL)
            AND width < 75
            AND length < 488
            AND mmsi > 99999999
            AND mmsi < 1000000000
            AND ST_Contains(geom ,coordinate::geometry)
    """

    cur.execute(DISABLE_TRIGGERS)  # Disable triggers during load for efficiency

    # Retrieve the points with initial cleaning rules applied directly to where conditions
    cleaned_data = SQLSource(connection=connection, query=INITIAL_CLEAN_QUERY)

    ais_df = pd.DataFrame(cleaned_data)
    connection.commit()  # Required in order to release locks
    ships_grouped = ais_df.groupby(by=['mmsi'])

    dict_updated_ships = {}

    for mmsi, ship_data in ships_grouped:
        if len(ship_data) < 2:  # We need to have at least 2 rows to choose from
            continue

        # Create a sequence and count the number of occurrences for each mobile type recorded
        mobile_type_count = ship_data['type_of_mobile_id'].squeeze().value_counts()

        # Define variable based on the mobile type that have occurred the most
        best_type = mobile_type_count.reset_index(0)['index'][0]

        # Define the ship_id that have been reported with the previously defined mobile type
        seq_ship_type = ship_data[ship_data['type_of_mobile_id'] == best_type].squeeze()['ship_id'].value_counts()
        best_ship_id = seq_ship_type.reset_index(0)['index'][0]

        # Some ships have not reported multiple ship_id's. Therefore, we want to store a list of those that have, so
        # that we know which ships to update later. That is done by flagging them and adding to a dictionary.
        flagged = len(seq_ship_type.value_counts()) > 1
        if flagged:
            dict_updated_ships[mmsi] = best_ship_id

    # Iterate through all the ships that require an update and update their ship_id for the dataframe
    for ship in dict_updated_ships:
        print("Changing ship value of: " + str(ship))
        ais_df.loc[ais_df['mmsi'] == ship, 'ship_id'] = dict_updated_ships[ship]

    del ais_df['mmsi']  # Remove mmsi column. It was only required during computation
    ais_df['audit_id'] = audit_id
    ais_df.to_sql('fact_ais_clean_v1', con=engine, chunksize=500000, index=False, if_exists='append')

    END_TIME = datetime.today()

    audit_obj['processed_records'] = len(ais_df)
    audit_obj['inserted_records'] = len(ais_df)
    audit_obj['etl_duration'] = END_TIME - START_TIME
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    connection.commit()
    cur.execute(ENABLE_TRIGGERS)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()
