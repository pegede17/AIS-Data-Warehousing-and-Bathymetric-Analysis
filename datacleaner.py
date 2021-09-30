import configparser
from datetime import datetime
from helper_functions import create_audit_dimension, create_fact_table, create_ship_dimension

import pygrametl
from pygrametl import ConnectionWrapper
from pygrametl.datasources import SQLSource
from database_connection import connect_to_local, connect_via_ssh

version = 1
counter = 0


def data_not_in_denmark(row):
    lat = row["lat"]
    long = row["long"]
    if(long < 2 or long > 18 or lat < 53 or lat > 59):
        global counter
        counter = counter + 1
        return True
    else:
        return False


def clean_data(config):

    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    create_query = """
    CREATE TABLE IF NOT EXISTS fact_ais_clean_v{} AS 
    TABLE fact_ais 
    WITH NO DATA;
    """.format(version)

    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        cursor = connection.cursor()
        cursor.copy_from(file=filehandle, table=name, sep=fieldsep, null=str(nullval),
                         columns=attributes)

    ship_dimension = create_ship_dimension()
    audit_dimension = create_audit_dimension()
    fact_table = create_fact_table(pgbulkloader=pgbulkloader, tb_name="fact_ais_clean_v{}".format(version))

    cur = connection.cursor()
    cur.execute(create_query)

    query = """
    SELECT fact_id, eta_date_id, eta_time_id, ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, type_of_mobile_id, navigational_status_id, cargo_type_id, type_of_position_fixing_device_id, ship_type_id, coordinate ,ST_X(coordinate::geometry) as long, ST_Y(coordinate::geometry) as lat, rot, sog, cog, heading, audit_id
	FROM public.fact_ais WHERE "audit_id" = 15;
    """

    ais_source = SQLSource(connection=connection, query=query)

    audit_obj = {'timestamp': datetime.now(),
                 'processed_records': 0,
                 'source_system': config["Audit"]["source_system"],
                 'etl_version': config["Audit"]["elt_version"],
                 'table_name': fact_table.name,
                 'comment': config["Audit"]["comment"]}

    audit_id = audit_dimension.insert(audit_obj)

    i = 0
    for row in ais_source:
        if (i % 10000 == 0):
            print(str(datetime.now()) + " Reached milestone: " + str(i))
        if data_not_in_denmark(row):
            continue
        row["audit_id"] = audit_id
        fact_table.insert(row)
        i = i + 1

    audit_obj['processed_records'] = i
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()

    print("done")
    print(counter)
