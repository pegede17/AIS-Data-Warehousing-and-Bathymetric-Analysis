import pygrametl
from datetime import date, datetime
from helper_functions import create_audit_dimension
from database_connection import connect_to_local, connect_via_ssh

VERSION = 4

def clean_data(config, date_id):

    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    create_query = f"""
    CREATE TABLE IF NOT EXISTS fact_ais_clean_v{VERSION} (
            fact_id BIGSERIAL NOT NULL,
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

            PRIMARY KEY (fact_id, ts_date_id),
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

    CREATE TABLE if not exists fact_ais_y2021w1 PARTITION OF 
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210101') TO ('20210108');
    CREATE TABLE if not exists fact_ais_y2021w2 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210108') TO ('20210115');
    CREATE TABLE if not exists fact_ais_y2021w3 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210115') TO ('20210122');
    CREATE TABLE if not exists fact_ais_y2021w4 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210122') TO ('20210129');
    CREATE TABLE if not exists fact_ais_y2021w5 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210129') TO ('20210205');
    CREATE TABLE if not exists fact_ais_y2021w6 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210205') TO ('20210212');
    CREATE TABLE if not exists fact_ais_y2021w7 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210212') TO ('20210219');
    CREATE TABLE if not exists fact_ais_y2021w8 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210219') TO ('20210226');
    CREATE TABLE if not exists fact_ais_y2021w9 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210226') TO ('20210305');
    CREATE TABLE if not exists fact_ais_y2021w10 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210305') TO ('20210312');
    CREATE TABLE if not exists fact_ais_y2021w11 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210312') TO ('20210319');
    CREATE TABLE if not exists fact_ais_y2021w12 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210319') TO ('20210326');
    CREATE TABLE if not exists fact_ais_y2021w13 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210326') TO ('20210402');
    CREATE TABLE if not exists fact_ais_y2021w14 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210402') TO ('20210409');
    CREATE TABLE if not exists fact_ais_y2021w15 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210409') TO ('20210416');
    CREATE TABLE if not exists fact_ais_y2021w16 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210416') TO ('20210423');
    CREATE TABLE if not exists fact_ais_y2021w17 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210423') TO ('20210430');
    CREATE TABLE if not exists fact_ais_y2021w18 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210430') TO ('20210507');
    CREATE TABLE if not exists fact_ais_y2021w19 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210507') TO ('20210514');
    CREATE TABLE if not exists fact_ais_y2021w20 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210514') TO ('20210521');
    CREATE TABLE if not exists fact_ais_y2021w21 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210521') TO ('20210528');
    CREATE TABLE if not exists fact_ais_y2021w22 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210528') TO ('20210604');
    CREATE TABLE if not exists fact_ais_y2021w23 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210604') TO ('20210611');
    CREATE TABLE if not exists fact_ais_y2021w24 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210611') TO ('20210618');
    CREATE TABLE if not exists fact_ais_y2021w25 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210618') TO ('20210625');
    CREATE TABLE if not exists fact_ais_y2021w26 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210625') TO ('20210702');
    CREATE TABLE if not exists fact_ais_y2021w27 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210702') TO ('20210709');
    CREATE TABLE if not exists fact_ais_y2021w28 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210709') TO ('20210716');
    CREATE TABLE if not exists fact_ais_y2021w29 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210716') TO ('20210723');
    CREATE TABLE if not exists fact_ais_y2021w30 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210723') TO ('20210730');
    CREATE TABLE if not exists fact_ais_y2021w31 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210730') TO ('20210806');
    CREATE TABLE if not exists fact_ais_y2021w32 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210806') TO ('20210813');
    CREATE TABLE if not exists fact_ais_y2021w33 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210813') TO ('20210820');
    CREATE TABLE if not exists fact_ais_y2021w34 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210820') TO ('20210827');
    CREATE TABLE if not exists fact_ais_y2021w35 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210827') TO ('20210903');
    CREATE TABLE if not exists fact_ais_y2021w36 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210903') TO ('20210910');
    CREATE TABLE if not exists fact_ais_y2021w37 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210910') TO ('20210917');
    CREATE TABLE if not exists fact_ais_y2021w38 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210917') TO ('20210924');
    CREATE TABLE if not exists fact_ais_y2021w39 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210924') TO ('20211001');
    CREATE TABLE if not exists fact_ais_y2021w40 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211001') TO ('20211008');
    CREATE TABLE if not exists fact_ais_y2021w41 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211008') TO ('20211015');
    CREATE TABLE if not exists fact_ais_y2021w42 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211015') TO ('20211022');
    CREATE TABLE if not exists fact_ais_y2021w43 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211022') TO ('20211029');
    CREATE TABLE if not exists fact_ais_y2021w44 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211029') TO ('20211105');
    CREATE TABLE if not exists fact_ais_y2021w45 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211105') TO ('20211112');
    CREATE TABLE if not exists fact_ais_y2021w46 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211112') TO ('20211119');
    CREATE TABLE if not exists fact_ais_y2021w47 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211119') TO ('20211126');
    CREATE TABLE if not exists fact_ais_y2021w48 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211126') TO ('20211203');
    CREATE TABLE if not exists fact_ais_y2021w49 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211203') TO ('20211210');
    CREATE TABLE if not exists fact_ais_y2021w50 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211210') TO ('20211217');
    CREATE TABLE if not exists fact_ais_y2021w51 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211217') TO ('20211224');
    CREATE TABLE if not exists fact_ais_y2021w52 PARTITION OF
        fact_ais_clean_v{VERSION} FOR VALUES FROM ('20211224') TO ('20211231');
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
