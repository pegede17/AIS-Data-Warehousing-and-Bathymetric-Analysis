from time import perf_counter
from utils.database_connection import connect_to_db
import pygrametl
from utils.helper_functions import create_audit_dimension, create_cargo_type_dimension, create_data_source_type_dimension, create_destination_dimension, create_fact_table, create_navigational_status_dimension, create_ship_dimension, create_ship_type_dimension, create_type_of_mobile_dimension, create_type_of_position_fixing_device_dimension
from pygrametl.datasources import CSVSource, TransformingSource
from datetime import datetime, timedelta


def load_data_into_db(config, date_id, filename):
    t_start = perf_counter()

    # Initialize variables
    connection = None
    dw_conn_wrapper = None

    connection = connect_to_db(config)

    dw_conn_wrapper = pygrametl.ConnectionWrapper(
        connection=connection)

    def convertTimestampToTimeId(timestamp):
        if(timestamp):
            date = datetime.strptime(timestamp, "%d/%m/%Y %H:%M:%S")
            formatted = int(date.strftime("%H%M%S"))
            return formatted
        else:
            return 0

    def convertTimestampToDateId(timestamp):
        if(timestamp):
            date = datetime.strptime(timestamp, "%d/%m/%Y %H:%M:%S")
            return int(date.strftime("%Y%m%d"))
        else:
            return 0

    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        cursor = connection.cursor()
        cursor.copy_from(file=filehandle, table=name, sep=fieldsep, null=str(nullval),
                         columns=attributes)

    # Creation of dimension and fact table abstractions for use in the ETL flow

    ship_type_dimension = create_ship_type_dimension()

    type_of_position_fixing_device_dimension = create_type_of_position_fixing_device_dimension()

    cargo_type_dimension = create_cargo_type_dimension()

    type_of_mobile_dimension = create_type_of_mobile_dimension()

    ship_dimension = create_ship_dimension()

    navigational_status_dimension = create_navigational_status_dimension()

    destination_dimension = create_destination_dimension()

    data_source_type_dimension = create_data_source_type_dimension()

    audit_dimension = create_audit_dimension()

    fact_table = create_fact_table(
        pgbulkloader=pgbulkloader, tb_name="fact_ais")

    audit_obj = {'timestamp': datetime.now(),
                 'processed_records': 0,
                 'inserted_records': 0,
                 'etl_duration': timedelta(minutes=0),
                 'source_system': config["Audit"]["source_system"],
                 'etl_version': config["Audit"]["elt_version"],
                 'table_name': fact_table.name,
                 'description': f"Loading date: {date_id}"}

    audit_id = audit_dimension.insert(audit_obj)

    print(datetime.now())

    def validateToNull(val): return None if (
        val == "" or val == "Unknown") else val

    def validateToZero(val): return 0 if (
        val is None or val == "" or val == "Unknown") else val

    def transformNulls(row):
        for value in row:
            val = row[value]
            row[value] = validateToNull(val)

    ais_file_handle = open(
        config["Environment"]["FILE_PATH"] + filename, 'r')
    ais_source = CSVSource(f=ais_file_handle, delimiter=',')

    transformeddata = TransformingSource(ais_source, transformNulls)

    i = 0

    print("Starting loading " + str(datetime.now()))
    for row in transformeddata:
        timestamp = convertTimestampToTimeId(row["# Timestamp"])

        i = i + 1

        fact = {}
        fact["audit_id"] = audit_id

        row['MMSI'] = int(row['MMSI'])

        fact["cargo_type_id"] = cargo_type_dimension.ensure(row, {
            'cargo_type': 'Cargo type'
        })

        fact["navigational_status_id"] = navigational_status_dimension.ensure(row, {
            'navigational_status': 'Navigational status'
        })

        fact["destination_id"] = destination_dimension.ensure(row, {
            'user_defined_destination': 'Destination',
            'mapped_destination': 'Destination'
        })

        fact["data_source_type_id"] = data_source_type_dimension.ensure(row, {
            'data_source_type': 'Data source type'
        })

        row['type_of_position_fixing_device_id'] = type_of_position_fixing_device_dimension.ensure(row, {
            'device_type': 'Type of position fixing device'
        })
        fact["type_of_position_fixing_device_id"] = row['type_of_position_fixing_device_id']

        row['type_of_mobile_id'] = type_of_mobile_dimension.ensure(row, {
            'mobile_type': 'Type of mobile'
        })
        fact["type_of_mobile_id"] = row['type_of_mobile_id']

        row['ship_type_id'] = ship_type_dimension.ensure(row, {
            'ship_type': 'Ship type'
        })
        fact["ship_type_id"] = row['ship_type_id']

        fact["ship_id"] = ship_dimension.ensure(row, {
            'size_a': 'A',
            'size_b': 'B',
            'size_c': 'C',
            'size_d': 'D',
            'mmsi': 'MMSI',
            'ship_type_id': 'ship_type_id',
            'type_of_position_fixing_device_id': "type_of_position_fixing_device_id",
            'type_of_mobile_id': 'type_of_mobile_id'
        })

        # Retrieve attributes that are obtained either by formula or as a measure from dataset

        fact_extra = {
            'eta_date_id': convertTimestampToDateId(row["ETA"]),
            'eta_time_id': convertTimestampToTimeId(row["ETA"]),
            'ts_date_id': convertTimestampToDateId(row["# Timestamp"]),
            'ts_time_id': timestamp,
            'coordinate': ("POINT(" + row["Longitude"] + " " + row["Latitude"] + ")"),
            'draught': row["Draught"],
            'rot': row["ROT"],
            'sog': row["SOG"],
            'cog': row["COG"],
            'heading': validateToZero(row["Heading"]),
            'longitude': float(row['Longitude']),
            'latitude': float(row['Latitude'])
        }

        fact.update(fact_extra)  # Adds the extra attributes to the fact object

        fact_table.insert(fact)
    t_end = perf_counter()

    audit_obj['processed_records'] = i
    audit_obj['inserted_records'] = i
    audit_obj['etl_duration'] = str(
        timedelta(seconds=(t_end - t_start))),
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()

    print(datetime.now())
