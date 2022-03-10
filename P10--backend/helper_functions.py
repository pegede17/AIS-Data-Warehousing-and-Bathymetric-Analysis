
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
        CREATE TABLE IF NOT EXISTS fact_ais_clean (
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
            cell_id INTEGER DEFAULT NULL,
            trajectory_stopped_id INTEGER DEFAULT NULL,
            trajectory_sailing_id INTEGER DEFAULT NULL,

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
                ON DELETE SET DEFAULT
        )
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
        attributes=['timestamp', 'processed_records', 'inserted_records' ,'source_system',
                    'etl_version', 'table_name', 'etl_duration' , 'description']
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
        lookupatts=['mmsi', 'ship_type_id', 'type_of_position_fixing_device_id', 'type_of_mobile_id'],
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


def create_trajectory_fact_table(tb_name):
    return FactTable(
        name=tb_name,
        keyrefs=['ship_id', 'time_start_id', 'date_start_id', 'time_end_id', 'date_end_id', 
                'audit_id', 'ship_type_id', 'eta_time_id', 'eta_date_id', 'data_source_type_id', 
                'type_of_position_fixing_device_id', 'destination_id', 'type_of_mobile', 'cargo_type_id'],
        measures=['coordinates', 'duration', 'length_meters', 'draught', 'total_points', 'avg_speed_knots']
    )


def create_cell_dim_queries():
    return("""
	
WITH grid AS (
SELECT (ST_SquareGrid(10000, ST_Transform(geom,3034))).*
FROM danish_waters
WHERE 
),
denmark as (
	SELECT ST_Transform(ST_Simplify(geom,0.05),3034) geom from danish_waters
)
  SELECT ST_Transform(g.geom,4326)
  FROM grid g, denmark d
  WHERE ST_Intersects(g.geom, d.geom)
  
  
CREATE TABLE boundaries_50m(
	boundary_id BIGSERIAL PRIMARY KEY,
	boundary GEOMETRY(POLYGON)
);

CREATE TABLE boundaries_1000m(
	boundary_id BIGSERIAL PRIMARY KEY,
	boundary GEOMETRY(POLYGON)
);

CREATE TABLE dim_cell (
	cell_id BIGSERIAL PRIMARY KEY,
	boundary_50m_id BIGINT,
	boundary_1000m_id BIGINT,
	FOREIGN KEY (boundary_50m_id)
		REFERENCES boundaries_50m (boundary_id),
	FOREIGN KEY (boundary_1000m_id)
		REFERENCES boundaries_1000m (boundary_id)
);

WITH grid AS (
SELECT (ST_SquareGrid(1000, ST_Transform(geom,3034))).*
FROM danish_waters
) --,
-- denmark as (
-- 	SELECT ST_Transform(ST_Simplify(geom,0.05),3034) geom from danish_waters
-- )  
INSERT INTO boundaries_1000m (boundary)
SELECT ST_Transform(g.geom,4326)
  FROM grid g --, denmark d
--   WHERE ST_Intersects(g.geom, d.geom)

WITH grid AS (
SELECT (ST_SquareGrid(50, ST_Transform(geom,3034))).*
FROM danish_waters
) --,
-- denmark as (
-- 	SELECT ST_Transform(ST_Simplify(geom,0.05),3034) geom from danish_waters
-- )
INSERT INTO boundaries_50m (boundary)
SELECT ST_Transform(g.geom,4326)
  FROM grid g --, denmark d
--   WHERE ST_Intersects(g.geom, d.geom)

INSERT INTO dim_cell (boundary_50m_id, boundary_1000m_id)
SELECT b50.boundary_id, b1000.boundary_id
FROM boundaries_50m b50, boundaries_1000m b1000,
WHERE ST_Contains(b1000.boundary, b50.boundary)""",
           """SELECT count(*) from boundaries_1000m

CREATE INDEX geom_1000_idx
  ON boundaries_1000m
  USING GIST (boundary);
  
CREATE INDEX geom_50_idx
  ON boundaries_50m
  USING GIST (boundary);
  
 CREATE INDEX geom_1000_idx_text
  ON boundaries_1000m
  USING btree(boundary);
  
SELECT boundary_id, boundary
	FROM public.boundaries_1000m
	WHERE ST_INTERSECTS(ST_SetSRID( ST_Point(10.990976, 58.011979), 4326),boundary)
	LIMIT 1;""")
