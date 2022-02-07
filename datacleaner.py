import pygrametl
from datetime import date, datetime
from helper_functions import create_audit_dimension
from database_connection import connect_to_local, connect_via_ssh

VERSION = 3


def clean_data(config, date_id):

    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    create_query = f"""
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
        ) PARTITION BY RANGE (ts_date_id);
    CREATE INDEX IF NOT EXISTS ts_date_id ON fact_ais_clean_v{VERSION} (ts_date_id);

    CREATE TABLE if not exists fact_ais_y2021d01 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210101') TO ('20210102); 
    CREATE TABLE if not exists fact_ais_y2021d02 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210102') TO ('20210103); 
    CREATE TABLE if not exists fact_ais_y2021d03 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210103') TO ('20210104); 
    CREATE TABLE if not exists fact_ais_y2021d04 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210104') TO ('20210105); 
    CREATE TABLE if not exists fact_ais_y2021d05 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210105') TO ('20210106); 
    CREATE TABLE if not exists fact_ais_y2021d06 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210106') TO ('20210107); 
    CREATE TABLE if not exists fact_ais_y2021d07 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210107') TO ('20210108); 
    CREATE TABLE if not exists fact_ais_y2021d08 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210108') TO ('20210109); 
    CREATE TABLE if not exists fact_ais_y2021d09 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210109') TO ('20210110); 
    CREATE TABLE if not exists fact_ais_y2021d10 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210110') TO ('20210111); 
    CREATE TABLE if not exists fact_ais_y2021d11 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210111') TO ('20210112); 
    CREATE TABLE if not exists fact_ais_y2021d12 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210112') TO ('20210113); 
    CREATE TABLE if not exists fact_ais_y2021d13 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210113') TO ('20210114); 
    CREATE TABLE if not exists fact_ais_y2021d14 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210114') TO ('20210115); 
    CREATE TABLE if not exists fact_ais_y2021d15 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210115') TO ('20210116); 
    CREATE TABLE if not exists fact_ais_y2021d16 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210116') TO ('20210117); 
    CREATE TABLE if not exists fact_ais_y2021d17 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210117') TO ('20210118); 
    CREATE TABLE if not exists fact_ais_y2021d18 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210118') TO ('20210119); 
    CREATE TABLE if not exists fact_ais_y2021d19 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210119') TO ('20210120); 
    CREATE TABLE if not exists fact_ais_y2021d20 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210120') TO ('20210121); 
    CREATE TABLE if not exists fact_ais_y2021d21 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210121') TO ('20210122); 
    CREATE TABLE if not exists fact_ais_y2021d22 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210122') TO ('20210123); 
    CREATE TABLE if not exists fact_ais_y2021d23 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210123') TO ('20210124); 
    CREATE TABLE if not exists fact_ais_y2021d24 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210124') TO ('20210125); 
    CREATE TABLE if not exists fact_ais_y2021d25 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210125') TO ('20210126); 
    CREATE TABLE if not exists fact_ais_y2021d26 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210126') TO ('20210127); 
    CREATE TABLE if not exists fact_ais_y2021d27 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210127') TO ('20210128); 
    CREATE TABLE if not exists fact_ais_y2021d28 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210128') TO ('20210129); 
    CREATE TABLE if not exists fact_ais_y2021d29 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210129') TO ('20210130); 
    CREATE TABLE if not exists fact_ais_y2021d30 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210130') TO ('20210131); 
    CREATE TABLE if not exists fact_ais_y2021d31 PARTITION OF
     fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210131') TO ('20210132); 
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
            AND width < 75
            AND length < 488
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
