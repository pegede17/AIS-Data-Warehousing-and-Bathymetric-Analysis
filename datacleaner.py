import pygrametl
from datetime import date, datetime
from helper_functions import create_audit_dimension
from database_connection import connect_to_local, connect_via_ssh

VERSION = 2


def clean_data(config, date_id):

    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    create_query = f"""
    CREATE TABLE IF NOT EXISTS fact_ais_clean_v{VERSION} (
            fact_id SERIAL NOT NULL PRIMARY KEY,
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
                 'source_system': config["Audit"]["source_system"],
                 'etl_version': config["Audit"]["elt_version"],
                 'table_name': f"fact_ais_clean_v{VERSION}",
                 'comment': f"Date: {date_id}"}

    audit_id = audit_dimension.insert(audit_obj)

    cur = connection.cursor()
    cur.execute(create_query)

    disable_query = f"""
        ALTER TABLE fact_ais_clean_v{VERSION} DISABLE TRIGGER ALL;
    """

    enable_query = f"""
        ALTER TABLE fact_ais_clean_v{VERSION} ENABLE TRIGGER ALL;	
    """

    clean_query = f"""
    INSERT INTO fact_ais_clean_v{VERSION}
	    SELECT DISTINCT ON (eta_date_id, eta_time_id, ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, type_of_mobile_id, navigational_status_id, cargo_type_id, type_of_position_fixing_device_id, ship_type_id, coordinate, draught, rot, sog, cog, heading)
            fact_id, eta_date_id, eta_time_id, fact_ais.ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, type_of_mobile_id, navigational_status_id, cargo_type_id, type_of_position_fixing_device_id, ship_type_id, coordinate, draught, rot, sog, cog, heading, {audit_id}
        FROM fact_ais INNER JOIN public.dim_ship on fact_ais.ship_id = dim_ship.ship_id, public.danish_waters
        WHERE 
            ts_date_id = {date_id}
            AND (draught < 28.5 OR draught IS NULL)
            AND mmsi > 99999999
            AND mmsi < 1000000000
            AND ST_Contains(geom ,coordinate::geometry);
    """

    cur.execute(disable_query)
    cur.execute(clean_query)

    audit_obj['processed_records'] = cur.rowcount
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    cur.execute(enable_query)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()
