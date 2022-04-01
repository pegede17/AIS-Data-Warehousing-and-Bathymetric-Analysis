
import re
from pygrametl.tables import BulkFactTable, CachedDimension, Dimension, FactTable, SnowflakedDimension


def create_tables():
    """ create tables in the PostgreSQL database"""
    return (
        """
        CREATE TABLE IF NOT EXISTS dim_data_source_type (
            data_source_type_id SERIAL NOT NULL,
            data_source_type VARCHAR(10),
            PRIMARY KEY (data_source_type_id)
        );
        INSERT INTO dim_data_source_type (data_source_type) VALUES ('Unknown');
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_ship_type (
            ship_type_id SERIAL NOT NULL PRIMARY KEY,
            ship_type VARCHAR(25)
        );
        INSERT INTO dim_ship_type (ship_type) VALUES ('Unknown');
        """,
        """ 
        CREATE TABLE IF NOT EXISTS dim_destination(
            destination_id SERIAL NOT NULL,
            user_defined_destination VARCHAR(100),
            mapped_destination VARCHAR(100),
            PRIMARY KEY (destination_id)
        );
        INSERT INTO dim_destination (user_defined_destination, mapped_destination) VALUES ('Unknown', 'Unknown');
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_type_of_mobile (
            type_of_mobile_id SERIAL NOT NULL,
            mobile_type VARCHAR(50),
            PRIMARY KEY (type_of_mobile_id)
        );
        INSERT INTO dim_type_of_mobile (mobile_type) VALUES ('Unknown');
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_cargo_type (
            cargo_type_id SERIAL NOT NULL,
            cargo_type VARCHAR(50),
            PRIMARY KEY (cargo_type_id)
        );
        INSERT INTO dim_cargo_type (cargo_type) VALUES ('Unknown');
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_navigational_status (
            navigational_status_id SERIAL NOT NULL,
            navigational_status VARCHAR(100),
            PRIMARY KEY (navigational_status_id)
        );
        INSERT INTO dim_navigational_status (navigational_status) VALUES ('Unknown');
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_type_of_position_fixing_device (
            type_of_position_fixing_device_id SERIAL NOT NULL,
            device_type VARCHAR(50),
            PRIMARY KEY (type_of_position_fixing_device_id)
        );
        INSERT INTO dim_type_of_position_fixing_device (device_type) VALUES ('Unknown');
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_trustworthiness (
            trust_id SERIAL NOT NULL,
            trust_score DOUBLE PRECISION,
            trust_category INTEGER,
            PRIMARY KEY (trust_id)
        );
        INSERT INTO dim_trustworthiness (trust_score, trust_category) VALUES 
            (-1,-1),
            (0,0),
            (1,0),
            (2,1),
            (3,1),
            (4,1),
            (5,2),
            (6,2),
            (7,2),
            (8,3),
            (9,3),
            (10,3);
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_ship (
            ship_id SERIAL NOT NULL,
            mmsi BIGINT NOT NULL,
            imo BIGINT,
            name VARCHAR(100),
            width SMALLINT,
            length SMALLINT,
            callsign VARCHAR(25),
            size_a DOUBLE PRECISION,
            size_b DOUBLE PRECISION,
            size_c DOUBLE PRECISION,
            size_d DOUBLE PRECISION,
            ship_type_id INTEGER NOT NULL DEFAULT 1,
            type_of_position_fixing_device_id INTEGER NOT NULL DEFAULT 1,
            type_of_mobile_id INTEGER NOT NULL DEFAULT 1,
            trust_id INTEGER NOT NULL DEFAULT 1,

            PRIMARY KEY (ship_id),
            FOREIGN KEY (ship_type_id)
                REFERENCES dim_ship_type (ship_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_position_fixing_device_id)
                REFERENCES dim_type_of_position_fixing_device (type_of_position_fixing_device_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_mobile_id)
                REFERENCES dim_type_of_mobile (type_of_mobile_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (trust_id)
                REFERENCES dim_trustworthiness (trust_id)
                ON UPDATE CASCADE
        );
        INSERT INTO dim_ship (mmsi, imo, name, width, length, callsign, size_a, size_b, size_c, size_d, ship_type_id, type_of_position_fixing_device_id, type_of_mobile_id, trust_id) 
            VALUES (0,0,'Unknown',0,0,'Unknown',0,0,0,0,1,1,1,1);
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id SERIAL PRIMARY KEY,
            millennium DOUBLE PRECISION NOT NULL,
            century DOUBLE PRECISION NOT NULL,
            decade DOUBLE PRECISION NOT NULL,
            iso_year DOUBLE PRECISION NOT NULL,
            year DOUBLE PRECISION NOT NULL,
            month DOUBLE PRECISION NOT NULL,
            day DOUBLE PRECISION NOT NULL,
            day_of_week DOUBLE PRECISION NOT NULL,
            iso_day_of_week DOUBLE PRECISION NOT NULL,
            day_of_year DOUBLE PRECISION NOT NULL,
            quarter DOUBLE PRECISION NOT NULL,
            epoch DOUBLE PRECISION NOT NULL,
            week DOUBLE PRECISION NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_time (
            time_id SERIAL PRIMARY KEY,
            hour SMALLINT NOT NULL,
            minute SMALLINT NOT NULL,
            second SMALLINT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_audit (
            audit_id SERIAL NOT NULL,
            timestamp timestamp with time zone NOT NULL,
            processed_records BIGINT NOT NULL,
            inserted_records BIGINT NOT NULL,
            source_system VARCHAR(100),
            etl_version VARCHAR(10),
            table_name VARCHAR(25),
            etl_duration INTERVAL NOT NULL, 
            description VARCHAR(200),
            PRIMARY KEY(audit_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_ais (
            fact_id BIGSERIAL NOT NULL PRIMARY KEY,
            eta_date_id INTEGER NOT NULL DEFAULT 0,
            eta_time_id INTEGER NOT NULL DEFAULT 0,
            ship_id INTEGER NOT NULL,
            ts_date_id INTEGER NOT NULL,
            ts_time_id INTEGER NOT NULL,
            data_source_type_id INTEGER NOT NULL DEFAULT 1,
            destination_id INTEGER NOT NULL DEFAULT 1,
            type_of_mobile_id INTEGER NOT NULL DEFAULT 1,
            navigational_status_id INTEGER NOT NULL DEFAULT 1,
            cargo_type_id INTEGER NOT NULL DEFAULT 1,
            type_of_position_fixing_device_id INTEGER NOT NULL DEFAULT 1,
            ship_type_id INTEGER NOT NULL DEFAULT 1,
            coordinate GEOGRAPHY(POINT) NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
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
        )
        """,
        """
        CREATE INDEX ON fact_ais (ts_date_id);
        """,
        """
        INSERT INTO public.dim_time(time_id, hour, minute, second)
        select to_char(second, 'hh24miss')::integer AS time_id,
        extract(hour from second) as Hour, 
        extract(minute from second) as Minute,
        extract(second from second) as Second
        from (SELECT '0:00'::time + (sequence.second || ' seconds')::interval AS second
        FROM generate_series(0,86399) AS sequence(second)
        GROUP BY sequence.second
        ) DQ
        order by 1;
""",
        """
INSERT INTO public.dim_date(
	date_id, millennium, century, decade, iso_year, year, month, day, day_of_week, iso_day_of_week, day_of_year, quarter, epoch, week)
	VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
""",
        """
    INSERT INTO public.dim_date(
        date_id, millennium, century, decade, iso_year, year, month, day, day_of_week, iso_day_of_week, day_of_year, quarter, epoch, week)
        SELECT
    -- 	datum as Date,
        to_char(datum, 'YYYYMMDD')::integer,
        extract(millennium from datum) AS Millenium,
        extract(century from datum) AS Century,
        extract(decade from datum) AS Decade,
        extract(isoyear from datum) AS iso_year,
        extract(year from datum) AS Year,
        extract(month from datum) AS Month,
        extract(day from datum) AS Day,
        extract(dow from datum) AS day_of_week,
        extract(isodow from datum) AS iso_day_of_week,
        extract(doy from datum) AS DayOfYear,
        extract(quarter from datum) AS Quarter,
        extract(epoch from datum) AS Epoch,
        extract(week from datum) AS Week
    FROM (
        SELECT '2021-01-01'::DATE + sequence.day AS datum
        FROM generate_series(0,2000) AS sequence(day)
        GROUP BY sequence.day
        ) DQ
    order by 1;
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_trajectory_sailing (
            trajectory_id BIGSERIAL PRIMARY KEY,
            ship_id INTEGER NOT NULL,
            eta_time_id INTEGER NOT NULL,
            eta_date_id INTEGER NOT NULL,
            time_start_id INTEGER NOT NULL,
            date_start_id INTEGER NOT NULL,
            time_end_id INTEGER NOT NULL,
            date_end_id INTEGER NOT NULL,
            data_source_type_id INTEGER NOT NULL,
            type_of_position_fixing_device_id INTEGER NOT NULL,
            destination_id INTEGER NOT NULL,
            type_of_mobile_id INTEGER NOT NULL,
            cargo_type_id INTEGER NOT NULL,
            coordinates geometry(linestring) NOT NULL,
            length_meters FLOAT NOT NULL,
            duration INTEGER NOT NULL,
            audit_id INTEGER NOT NULL,
            total_points INTEGER NOT NULL,
            draught FLOAT[2],
            ship_type_id INTEGER NOT NULL DEFAULT 0,
            avg_speed_knots DOUBLE PRECISION NOT NULL,

            FOREIGN KEY (audit_id)
                REFERENCES dim_audit (audit_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
            FOREIGN KEY (ship_id)
                REFERENCES dim_ship (ship_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (time_start_id)
                REFERENCES dim_time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (date_start_id)
                REFERENCES dim_date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (time_end_id )
                REFERENCES dim_time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (date_end_id)
                REFERENCES dim_date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ship_type_id)
                REFERENCES dim_ship_type (ship_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (eta_time_id)
                REFERENCES dim_time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (eta_date_id)
                REFERENCES dim_date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (data_source_type_id)
                REFERENCES dim_data_source_type (data_source_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_position_fixing_device_id)
                REFERENCES dim_type_of_position_fixing_device (type_of_position_fixing_device_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (destination_id)
                REFERENCES dim_destination (destination_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_mobile_id)
                REFERENCES dim_type_of_mobile (type_of_mobile_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (cargo_type_id)
                REFERENCES dim_cargo_type (cargo_type_id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_trajectory_stopped (
            trajectory_id BIGSERIAL PRIMARY KEY,
            ship_id INTEGER NOT NULL,
            eta_time_id INTEGER NOT NULL,
            eta_date_id INTEGER NOT NULL,
            time_start_id INTEGER NOT NULL,
            date_start_id INTEGER NOT NULL,
            time_end_id INTEGER NOT NULL,
            date_end_id INTEGER NOT NULL,
            data_source_type_id INTEGER NOT NULL,
            type_of_position_fixing_device_id INTEGER NOT NULL,
            destination_id INTEGER NOT NULL,
            type_of_mobile_id INTEGER NOT NULL,
            cargo_type_id INTEGER NOT NULL,
            coordinates geometry(linestring) NOT NULL,
            length_meters FLOAT NOT NULL,
            duration INTEGER NOT NULL,
            audit_id INTEGER NOT NULL,
            total_points INTEGER NOT NULL,
            draught FLOAT[2],
            ship_type_id INTEGER NOT NULL DEFAULT 0,
            avg_speed_knots DOUBLE PRECISION NOT NULL,

            FOREIGN KEY (audit_id)
                REFERENCES dim_audit (audit_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
            FOREIGN KEY (ship_id)
                REFERENCES dim_ship (ship_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (time_start_id)
                REFERENCES dim_time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (date_start_id)
                REFERENCES dim_date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (time_end_id )
                REFERENCES dim_time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (date_end_id)
                REFERENCES dim_date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ship_type_id)
                REFERENCES dim_ship_type (ship_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (eta_time_id)
                REFERENCES dim_time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (eta_date_id)
                REFERENCES dim_date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (data_source_type_id)
                REFERENCES dim_data_source_type (data_source_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_position_fixing_device_id)
                REFERENCES dim_type_of_position_fixing_device (type_of_position_fixing_device_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (destination_id)
                REFERENCES dim_destination (destination_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_mobile_id)
                REFERENCES dim_type_of_mobile (type_of_mobile_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (cargo_type_id)
                REFERENCES dim_cargo_type (cargo_type_id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE dim_cell (
            cell_id BIGSERIAL PRIMARY KEY,
            columnx_50m BIGINT,
            rowy_50m BIGINT,
            columnx_100m BIGINT,
            rowy_100m BIGINT,
            columnx_500m BIGINT,
            rowy_500m BIGINT,
            columnx_1000m BIGINT,
            rowy_1000m BIGINT,
            boundary_50m GEOMETRY,
            boundary_100m GEOMETRY,
            boundary_500m GEOMETRY,
            boundary_1000m GEOMETRY
        );
        """,
        """
        CREATE TABLE bridge_traj_sailing_cell (
            cell_id BIGINT NOT NULL,
            trajectory_id BIGINT NOT NULL,
            
            FOREIGN KEY (cell_id)
                REFERENCES dim_cell (cell_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (trajectory_id)
                REFERENCES fact_trajectory_sailing (trajectory_id)
                ON UPDATE CASCADE,
            PRIMARY KEY (cell_id, trajectory_id)
        );
        """,
        """
        CREATE TABLE bridge_traj_stopped_cell (
            cell_id BIGINT NOT NULL,
            trajectory_id BIGINT NOT NULL,
            
            FOREIGN KEY (cell_id)
                REFERENCES dim_cell (cell_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (trajectory_id)
                REFERENCES fact_trajectory_stopped (trajectory_id)
                ON UPDATE CASCADE,
            PRIMARY KEY (cell_id, trajectory_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS junk_ais_clean (
            junk_id SERIAL NOT NULL PRIMARY KEY,
            patchedShipRef BOOLEAN NOT NULL,
            isOutlier BOOLEAN NOT NULL
        );

        INSERT INTO junk_ais_clean (patchedShipRef, isOutlier) 
        VALUES (FALSE, FALSE), (FALSE, TRUE), (TRUE, TRUE), (TRUE, FALSE);
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_ais_clean (
            fact_id BIGINT NOT NULL,
            eta_date_id INTEGER NOT NULL DEFAULT 0,
            eta_time_id INTEGER NOT NULL DEFAULT 0,
            ship_id INTEGER NOT NULL,
            ts_date_id INTEGER NOT NULL,
            ts_time_id INTEGER NOT NULL,
            data_source_type_id INTEGER NOT NULL DEFAULT 1,
            destination_id INTEGER NOT NULL DEFAULT 1,
            junk_id INTEGER NOT NULL,
            type_of_mobile_id INTEGER NOT NULL DEFAULT 1,
            navigational_status_id INTEGER NOT NULL DEFAULT 1,
            cargo_type_id INTEGER NOT NULL DEFAULT 1,
            type_of_position_fixing_device_id INTEGER NOT NULL DEFAULT 1,
            ship_type_id INTEGER NOT NULL DEFAULT 1,
            coordinate GEOGRAPHY(POINT) NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            draught DOUBLE PRECISION,
            rot DOUBLE PRECISION,
            sog DOUBLE PRECISION,
            cog DOUBLE PRECISION,
            heading SMALLINT,
            audit_id INTEGER NOT NULL,
            cell_id INTEGER DEFAULT NULL,
            trajectory_stopped_id INTEGER DEFAULT NULL,
            trajectory_sailing_id INTEGER DEFAULT NULL,
    
            PRIMARY KEY (fact_id, ts_date_id, ship_id, type_of_position_fixing_device_id, type_of_mobile_id, ship_type_id, cargo_type_id, ts_time_id, audit_id, eta_date_id, eta_time_id, data_source_type_id, 
                destination_id, navigational_status_id, cell_id, junk_id),
            FOREIGN KEY (audit_id)
                REFERENCES dim_audit (audit_id)
                ON DELETE CASCADE,
            FOREIGN KEY (eta_date_id)
                REFERENCES dim_date (date_id),
            FOREIGN KEY (eta_time_id)
                REFERENCES dim_time (time_id),
            FOREIGN KEY (ship_id)
                REFERENCES dim_ship (ship_id),
            FOREIGN KEY (ts_date_id)
                REFERENCES dim_date (date_id),
            FOREIGN KEY (ts_time_id)
                REFERENCES dim_time (time_id),
            FOREIGN KEY (data_source_type_id)
                REFERENCES dim_data_source_type (data_source_type_id),
            FOREIGN KEY (destination_id)
                REFERENCES dim_destination (destination_id),
            FOREIGN KEY (type_of_mobile_id)
                REFERENCES dim_type_of_mobile (type_of_mobile_id),
            FOREIGN KEY (navigational_status_id)
                REFERENCES dim_navigational_status (navigational_status_id),
            FOREIGN KEY (cargo_type_id)
                REFERENCES dim_cargo_type (cargo_type_id),
            FOREIGN KEY (type_of_position_fixing_device_id)
                REFERENCES dim_type_of_position_fixing_device (type_of_position_fixing_device_id),
            FOREIGN KEY (ship_type_id)
                REFERENCES dim_ship_type (ship_type_id),
            FOREIGN KEY (cell_id)
                REFERENCES dim_cell (cell_id)
                ON DELETE SET DEFAULT,
            FOREIGN KEY (trajectory_stopped_id)
                REFERENCES dim_cell (cell_id)
                ON DELETE SET DEFAULT,
            FOREIGN KEY (trajectory_sailing_id)
                REFERENCES fact_trajectory_sailing (trajectory_id)
                ON DELETE SET DEFAULT,
            FOREIGN KEY (junk_id)
                REFERENCES junk_ais_clean (junk_id)
        ) PARTITION BY RANGE(ts_date_id);
		CREATE TABLE fact_ais_clean_y2021week1 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210101') TO ('20210108');
        CREATE TABLE fact_ais_clean_y2021week2 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210108') TO ('20210115');
        CREATE TABLE fact_ais_clean_y2021week3 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210115') TO ('20210122');
        CREATE TABLE fact_ais_clean_y2021week4 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210122') TO ('20210129');
        CREATE TABLE fact_ais_clean_y2021week5 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210129') TO ('20210205');
        CREATE TABLE fact_ais_clean_y2021week6 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210205') TO ('20210212');
        CREATE TABLE fact_ais_clean_y2021week7 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210212') TO ('20210219');
        CREATE TABLE fact_ais_clean_y2021week8 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210219') TO ('20210226');
        CREATE TABLE fact_ais_clean_y2021week9 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210226') TO ('20210305');
        CREATE TABLE fact_ais_clean_y2021week10 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210305') TO ('20210312');
        CREATE TABLE fact_ais_clean_y2021week11 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210312') TO ('20210319');
        CREATE TABLE fact_ais_clean_y2021week12 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210319') TO ('20210326');
        CREATE TABLE fact_ais_clean_y2021week13 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210326') TO ('20210402');
        CREATE TABLE fact_ais_clean_y2021week14 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210402') TO ('20210409');
        CREATE TABLE fact_ais_clean_y2021week15 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210409') TO ('20210416');
        CREATE TABLE fact_ais_clean_y2021week16 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210416') TO ('20210423');
        CREATE TABLE fact_ais_clean_y2021week17 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210423') TO ('20210430');
        CREATE TABLE fact_ais_clean_y2021week18 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210430') TO ('20210507');
        CREATE TABLE fact_ais_clean_y2021week19 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210507') TO ('20210514');
        CREATE TABLE fact_ais_clean_y2021week20 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210514') TO ('20210521');
        CREATE TABLE fact_ais_clean_y2021week21 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210521') TO ('20210528');
        CREATE TABLE fact_ais_clean_y2021week22 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210528') TO ('20210604');
        CREATE TABLE fact_ais_clean_y2021week23 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210604') TO ('20210611');
        CREATE TABLE fact_ais_clean_y2021week24 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210611') TO ('20210618');
        CREATE TABLE fact_ais_clean_y2021week25 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210618') TO ('20210625');
        CREATE TABLE fact_ais_clean_y2021week26 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210625') TO ('20210702');
        CREATE TABLE fact_ais_clean_y2021week27 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210702') TO ('20210709');
        CREATE TABLE fact_ais_clean_y2021week28 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210709') TO ('20210716');
        CREATE TABLE fact_ais_clean_y2021week29 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210716') TO ('20210723');
        CREATE TABLE fact_ais_clean_y2021week30 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210723') TO ('20210730');
        CREATE TABLE fact_ais_clean_y2021week31 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210730') TO ('20210806');
        CREATE TABLE fact_ais_clean_y2021week32 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210806') TO ('20210813');
        CREATE TABLE fact_ais_clean_y2021week33 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210813') TO ('20210820');
        CREATE TABLE fact_ais_clean_y2021week34 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210820') TO ('20210827');
        CREATE TABLE fact_ais_clean_y2021week35 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210827') TO ('20210903');
        CREATE TABLE fact_ais_clean_y2021week36 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210903') TO ('20210910');
        CREATE TABLE fact_ais_clean_y2021week37 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210910') TO ('20210917');
        CREATE TABLE fact_ais_clean_y2021week38 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210917') TO ('20210924');
        CREATE TABLE fact_ais_clean_y2021week39 PARTITION OF fact_ais_clean FOR VALUES FROM ('20210924') TO ('20211001');
        CREATE TABLE fact_ais_clean_y2021week40 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211001') TO ('20211008');
        CREATE TABLE fact_ais_clean_y2021week41 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211008') TO ('20211015');
        CREATE TABLE fact_ais_clean_y2021week42 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211015') TO ('20211022');
        CREATE TABLE fact_ais_clean_y2021week43 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211022') TO ('20211029');
        CREATE TABLE fact_ais_clean_y2021week44 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211029') TO ('20211105');
        CREATE TABLE fact_ais_clean_y2021week45 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211105') TO ('20211112');
        CREATE TABLE fact_ais_clean_y2021week46 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211112') TO ('20211119');
        CREATE TABLE fact_ais_clean_y2021week47 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211119') TO ('20211126');
        CREATE TABLE fact_ais_clean_y2021week48 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211126') TO ('20211203');
        CREATE TABLE fact_ais_clean_y2021week49 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211203') TO ('20211210');
        CREATE TABLE fact_ais_clean_y2021week50 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211210') TO ('20211217');
        CREATE TABLE fact_ais_clean_y2021week51 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211217') TO ('20211224');
        CREATE TABLE fact_ais_clean_y2021week52 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211224') TO ('20211231');
        CREATE TABLE fact_ais_clean_y2021week53 PARTITION OF fact_ais_clean FOR VALUES FROM ('20211231') TO ('20220107');
        """,
        """
        CREATE TABLE raster_50m_test (
            ras raster
        );
        """
    )


def create_ship_type_dimension():
    return CachedDimension(
        name='dim_ship_type',
        key="ship_type_id",
        attributes=['ship_type'],
        prefill=True,
        cacheoninsert=True,
        defaultidvalue=1
    )


def create_fact_table(pgbulkloader, tb_name):
    return BulkFactTable(
        name=tb_name,
        keyrefs=['eta_date_id', 'eta_time_id', 'ship_id', 'ts_date_id', 'ts_time_id', 'data_source_type_id', 'destination_id',
                 'type_of_mobile_id', 'navigational_status_id', 'cargo_type_id', 'type_of_position_fixing_device_id', 'ship_type_id', 'audit_id'],
        measures=['coordinate', 'draught', 'rot', 'sog',
                  'cog', 'heading', 'latitude', 'longitude'],
        bulkloader=pgbulkloader,
        fieldsep=',',
        rowsep='\\r\n',
        nullsubst=str(None),
        bulksize=500000,
        usefilename=False,
    )


def create_cargo_type_dimension():
    return CachedDimension(
        name='dim_cargo_type',
        key='cargo_type_id',
        attributes=['cargo_type'],
        prefill=True,
        cacheoninsert=True,
        defaultidvalue=1
    )


def create_audit_dimension():
    return Dimension(
        name='dim_audit',
        key='audit_id',
        attributes=['timestamp', 'processed_records', 'inserted_records', 'source_system',
                    'etl_version', 'table_name', 'etl_duration', 'description']
    )


def create_navigational_status_dimension():
    return CachedDimension(
        name='dim_navigational_status',
        key='navigational_status_id',
        attributes=['navigational_status'],
        prefill=True,
        cacheoninsert=True,
        defaultidvalue=1
    )


def create_type_of_mobile_dimension():
    return CachedDimension(
        name='dim_type_of_mobile',
        key='type_of_mobile_id',
        attributes=['mobile_type'],
        prefill=True,
        cacheoninsert=True,
        defaultidvalue=1
    )


def create_destination_dimension():
    return CachedDimension(
        name='dim_destination',
        key='destination_id',
        attributes=['user_defined_destination', 'mapped_destination'],
        prefill=True,
        cacheoninsert=True,
        defaultidvalue=1
    )


def create_data_source_type_dimension():
    return CachedDimension(
        name='dim_data_source_type',
        key='data_source_type_id',
        attributes=['data_source_type'],
        prefill=True,
        cacheoninsert=True,
        defaultidvalue=1
    )


def create_type_of_position_fixing_device_dimension():
    return CachedDimension(
        name='dim_type_of_position_fixing_device',
        key='type_of_position_fixing_device_id',
        attributes=['device_type'],
        prefill=True,
        cacheoninsert=True,
        defaultidvalue=1
    )


def create_trustworthiness_dimension():
    return CachedDimension(
        name='dim_trustworthiness',
        key='trust_id',
        attributes=['trust_score', 'trust_category'],
        prefill=True,
        cacheoninsert=True,
        defaultidvalue=-1,
        lookupatts=['trust_score']
    )


def create_ship_dimension():
    return CachedDimension(
        name='dim_ship',
        key='ship_id',
        attributes=['mmsi', 'IMO', 'Name', 'Width', 'Length',
                    'Callsign', 'size_a', 'size_b', 'size_c',
                    'size_d', 'ship_type_id', 'type_of_mobile_id',
                    'type_of_position_fixing_device_id'],
        cachefullrows=True,
        prefill=True,
        cacheoninsert=True,
        lookupatts=['mmsi', 'ship_type_id',
                    'type_of_position_fixing_device_id', 'type_of_mobile_id'],
        size=0,
        defaultidvalue=1
    )


def create_time_dimension():
    return CachedDimension(
        name='dim_time',
        key='time_id',  # Lav den til en smartkey.
        attributes=['hour', 'minute', 'second'],
            prefill=True,
        cacheoninsert=True,
        size=0,
        defaultidvalue=0
    )


def create_date_dimension():
    return CachedDimension(
        name='dim_date',
        key='date_id',  # Lav den til en smartkey.
        attributes=['millennium', 'century', 'decade', 'iso_year', 'year', 'month', 'day',
                    'day_of_week', 'iso_day_of_week', 'day_of_year', 'quarter', 'epoch', 'week'],
        prefill=True,
        cacheoninsert=True,
        size=0,
        defaultidvalue=0
    )


def create_trajectory_sailing_fact_table():
    return FactTable(
        name='fact_trajectory_sailing',
        keyrefs=['ship_id', 'time_start_id', 'date_start_id', 'time_end_id', 'date_end_id',
                 'audit_id', 'ship_type_id', 'eta_time_id', 'eta_date_id', 'data_source_type_id',
                 'type_of_position_fixing_device_id', 'destination_id', 'type_of_mobile_id', 'cargo_type_id'],
        measures=['coordinates', 'duration', 'length_meters',
                  'draught', 'total_points', 'avg_speed_knots']
    )
def create_trajectory_stopped_fact_table():
    return FactTable(
        name='fact_trajectory_stopped',
        keyrefs=['ship_id', 'time_start_id', 'date_start_id', 'time_end_id', 'date_end_id',
                 'audit_id', 'ship_type_id', 'eta_time_id', 'eta_date_id', 'data_source_type_id',
                 'type_of_position_fixing_device_id', 'destination_id', 'type_of_mobile_id', 'cargo_type_id'],
        measures=['coordinates', 'duration', 'length_meters',
                  'draught', 'total_points', 'avg_speed_knots']
    )
