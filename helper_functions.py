
from pygrametl.tables import BulkFactTable, CachedDimension, Dimension, FactTable


def create_tables():
    """ create tables in the PostgreSQL database"""
    return (
        """
        CREATE TABLE IF NOT EXISTS dim_data_source_type (
            data_source_type_id SERIAL NOT NULL,
            data_source_type VARCHAR(10),
            PRIMARY KEY (data_source_type_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_ship_type (
            ship_type_id SERIAL NOT NULL PRIMARY KEY,
            ship_type VARCHAR(25)
        )
        """,
        """ 
        CREATE TABLE IF NOT EXISTS dim_destination(
            destination_id SERIAL NOT NULL,
            user_defined_destination VARCHAR(100),
            mapped_destination VARCHAR(100),
            PRIMARY KEY (destination_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_type_of_mobile (
            type_of_mobile_id SERIAL NOT NULL,
            mobile_type VARCHAR(256),
            PRIMARY KEY (type_of_mobile_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_cargo_type (
            cargo_type_id SERIAL NOT NULL,
            cargo_type VARCHAR(256),
            PRIMARY KEY (cargo_type_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_navigational_status (
            navigational_status_id SERIAL NOT NULL,
            navigational_status VARCHAR(256),
            PRIMARY KEY (navigational_status_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_type_of_position_fixing_device (
            type_of_position_fixing_device_id SERIAL NOT NULL,
            device_type VARCHAR(256),
            PRIMARY KEY (type_of_position_fixing_device_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_ship (
            ship_id INTEGER NOT NULL,
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
            PRIMARY KEY (ship_id)
        )
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
            source_system VARCHAR(100),
            etl_version VARCHAR(10),
            table_name VARCHAR(25),
            comment VARCHAR(200),
            PRIMARY KEY(audit_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_ais (
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
        )
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
        -- There are 3 leap years in this range, so calculate 365 * 10 + 3 records
        SELECT '2021-01-01'::DATE + sequence.day AS datum
        FROM generate_series(0,2000) AS sequence(day)
        GROUP BY sequence.day
        ) DQ
    order by 1;
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_trajectory (
            trajectory_id SERIAL PRIMARY KEY,
            ship_id INTEGER NOT NULL,
            time_start_id INTEGER NOT NULL,
            date_start_id INTEGER NOT NULL,
            time_end_id INTEGER NOT NULL,
            date_end_id INTEGER NOT NULL,
            coordinates geometry(linestring) NOT NULL,
            length_meters FLOAT NOT NULL,
            duration INTEGER NOT NULL,
            audit_id INTEGER NOT NULL,
            total_points INTEGER NOT NULL,
            draught FLOAT[2],

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
                ON UPDATE CASCADE
        )
        """,
    )


def create_ship_type_dimension():
    return CachedDimension(
        name='dim_ship_type',
        key="ship_type_id",
        attributes=['ship_type'],
        prefill=True,
        cacheoninsert=True,

    )


def create_fact_table(pgbulkloader, tb_name):
    return BulkFactTable(
        name=tb_name,
        keyrefs=['eta_date_id', 'eta_time_id', 'ship_id', 'ts_date_id', 'ts_time_id', 'data_source_type_id', 'destination_id',
                 'type_of_mobile_id', 'navigational_status_id', 'cargo_type_id', 'type_of_position_fixing_device_id', 'ship_type_id', 'audit_id'],
        measures=['coordinate', 'draught', 'rot', 'sog', 'cog', 'heading'],
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
    )


def create_audit_dimension():
    return Dimension(
        name='dim_audit',
        key='audit_id',
        attributes=['timestamp', 'processed_records', 'source_system',
                    'etl_version', 'table_name', 'comment', ]
    )


def create_navigational_status_dimension():
    return CachedDimension(
        name='dim_navigational_status',
        key='navigational_status_id',
        attributes=['navigational_status'],
        prefill=True,
        cacheoninsert=True,
    )


def create_type_of_mobile_dimension():
    return CachedDimension(
        name='dim_type_of_mobile',
        key='type_of_mobile_id',
        attributes=['mobile_type'],
        prefill=True,
        cacheoninsert=True,
    )


def create_destination_dimension():
    return CachedDimension(
        name='dim_destination',
        key='destination_id',
        attributes=['user_defined_destination', 'mapped_destination'],
        prefill=True,
        cacheoninsert=True,
    )


def create_data_source_type_dimension():
    return CachedDimension(
        name='dim_data_source_type',
        key='data_source_type_id',
        attributes=['data_source_type'],
        prefill=True,
        cacheoninsert=True,
    )


def create_type_of_position_fixing_device_dimension():
    return CachedDimension(
        name='dim_type_of_position_fixing_device',
        key='type_of_position_fixing_device_id',
        attributes=['device_type'],
        prefill=True,
        cacheoninsert=True,
    )


def create_ship_dimension():
    return CachedDimension(
        name='dim_ship',
        key='ship_id',
        attributes=['mmsi', 'IMO', 'Name', 'Width', 'Length',
                    'Callsign', 'size_a', 'size_b', 'size_c', 'size_d'],
        cachefullrows=True,
        prefill=True,
        cacheoninsert=True,
        lookupatts=['mmsi'],
        size=0
    )


def create_time_dimension():
    return CachedDimension(
        name='dim_time',
        key='time_id',  # Lav den til en smartkey.
        attributes=['hour', 'minute', 'second'],
            prefill=True,
        cacheoninsert=True,
        size=0
    )


def create_date_dimension():
    return CachedDimension(
        name='dim_date',
        key='date_id',  # Lav den til en smartkey.
        attributes=['millennium', 'century', 'decade', 'iso_year', 'year', 'month', 'day',
                    'day_of_week', 'iso_day_of_week', 'day_of_year', 'quarter', 'epoch', 'week'],
        prefill=True,
        cacheoninsert=True,
        size=0
    )


def create_trajectory_fact_table(tb_name):
    return FactTable(
        name=tb_name,
        keyrefs=['ship_id', 'time_start_id', 'date_start_id',
                 'time_end_id', 'date_end_id', 'audit_id'],
        measures=['linestring', 'duration', 'length_meters', 'draught']
    )
