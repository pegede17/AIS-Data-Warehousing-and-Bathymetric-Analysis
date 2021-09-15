
def create_tables():
    """ create tables in the PostgreSQL database"""
    return (    
        """
        CREATE TABLE IF NOT EXISTS data_source_type (
            data_source_type_id SERIAL NOT NULL,
            data_source_type VARCHAR(10),
            PRIMARY KEY (data_source_type_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ship_type (
            ship_type_id SERIAL NOT NULL PRIMARY KEY,
            ship_type VARCHAR(25)
        )
        """,
        """ 
        CREATE TABLE IF NOT EXISTS destination(
            destination_id SERIAL NOT NULL,
            user_defined_destination VARCHAR(100),
            mapped_destination VARCHAR(100),
            PRIMARY KEY (destination_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS type_of_mobile (
            type_of_mobile_id SERIAL NOT NULL,
            mobile_type VARCHAR(256),
            PRIMARY KEY (type_of_mobile_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS cargo_type (
            cargo_type_id SERIAL NOT NULL,
            cargo_type VARCHAR(256),
            PRIMARY KEY (cargo_type_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS navigational_status (
            navigational_status_id SERIAL NOT NULL,
            navigational_status VARCHAR(256),
            PRIMARY KEY (navigational_status_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS type_of_position_fixing_device (
            type_of_position_fixing_device_id SERIAL NOT NULL,
            device_type VARCHAR(256),
            PRIMARY KEY (type_of_position_fixing_device_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ship (
            ship_id INTEGER NOT NULL,
            mmsi BIGINT NOT NULL,
            imo BIGINT,
            name VARCHAR(100),
            width SMALLINT,
            length SMALLINT,
            callsign VARCHAR(25),
            draught DOUBLE PRECISION,
            size_a DOUBLE PRECISION,
            size_b DOUBLE PRECISION,
            size_c DOUBLE PRECISION,
            size_d DOUBLE PRECISION,
            PRIMARY KEY (ship_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS date (
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
        CREATE TABLE IF NOT EXISTS time (
            time_id SERIAL PRIMARY KEY,
            hour SMALLINT NOT NULL,
            minute SMALLINT NOT NULL,
            second SMALLINT NOT NULL
        )
        """,    
        """
        CREATE TABLE IF NOT EXISTS fact_table (
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
            rot DOUBLE PRECISION,
            sog DOUBLE PRECISION,
            cog DOUBLE PRECISION,
            heading SMALLINT,

            FOREIGN KEY (eta_date_id)
                REFERENCES date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (eta_time_id)
                REFERENCES time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ship_id)
                REFERENCES ship (ship_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ts_date_id)
                REFERENCES date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ts_time_id)
                REFERENCES time (time_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (data_source_type_id)
                REFERENCES data_source_type (data_source_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (destination_id)
                REFERENCES destination (destination_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_mobile_id)
                REFERENCES type_of_mobile (type_of_mobile_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (navigational_status_id)
                REFERENCES navigational_status (navigational_status_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (cargo_type_id)
                REFERENCES cargo_type (cargo_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (type_of_position_fixing_device_id)
                REFERENCES type_of_position_fixing_device (type_of_position_fixing_device_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (ship_type_id)
                REFERENCES ship_type (ship_type_id)
                ON UPDATE CASCADE
        )
        """,
        )

    