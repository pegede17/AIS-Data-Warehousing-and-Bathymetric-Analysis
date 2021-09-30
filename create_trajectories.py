from datetime import datetime
from helper_functions import create_audit_dimension, create_tables, create_trajectory_fact_table
from pygrametl.datasources import SQLSource
from pygrametl.tables import FactTable
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import Dimension, FactTable
import pygrametl
import configparser
from database_connection import connect_to_local, connect_via_ssh

# For interactive work (on ipython) it's easier to work with explicit objects
# instead of contexts.


def create_trajectories():

    version = 12
    trajectories = []

    temp_trajectory = {
        'ship_id': 0,
        'time_start_id': 0,
        'date_start_id': 0,
        'time_end_id': 0,
        'date_end_id': 0,
        'coordinates': "LineString("
    }

    trajectory_length = 0

    current_ship_id = 0

    def add_to_trajectory(row):
        global trajectory_length
        trajectory_length = trajectory_length + 1
        temp_trajectory["time_end_id"] = row["ts_time_id"]
        temp_trajectory["date_end_id"] = row["ts_date_id"]
        temp_trajectory['coordinates'] = temp_trajectory['coordinates'] + \
            "," + ((row["coordinate"][6:-1]))

    def create_new_trajectory(row):
        global trajectory_length
        trajectory_length = 0
        temp_trajectory["ship_id"] = row["ship_id"]
        temp_trajectory["time_start_id"] = row["ts_time_id"]
        temp_trajectory["date_start_id"] = row["ts_date_id"]
        temp_trajectory["coordinates"] = "LineString(" + \
            row["coordinate"][6:-1]

    def end_trajectory():
        global trajectory_length
        temp_trajectory['coordinates'] = temp_trajectory['coordinates'] + ")"
        if(trajectory_length > 4):
            trajectories.append(temp_trajectory.copy())
        temp_trajectory.clear()

    config = configparser.ConfigParser()
    config.read('application.properties')

    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    query = """
    SELECT fact_id, ts_date_id, ship_id, ts_time_id, ST_AsText(coordinate) as coordinate, sog from fact_ais_clean_v8 
        ORDER BY ship_id, ts_time_id ASC;
    """

    create_query = """
    CREATE TABLE fact_trajectory_clean_v{} AS 
    TABLE fact_trajectory 
    WITH NO DATA;
    """.format(version)

    cur = connection.cursor()
    cur.execute(create_query)

    ais_source = SQLSource(connection=connection, query=query)

    trajectory_fact_table = create_trajectory_fact_table("fact_trajectory_clean_v{}".format(version))

    audit_dimension = create_audit_dimension()

    audit_obj = {'timestamp': datetime.now(),
                 'source_system': config["Audit"]["source_system"],
                 'etl_version': config["Audit"]["elt_version"],
                 'comment': config["Audit"]["comment"],
                 'table_name': trajectory_fact_table.name,
                 'processed_records': 0}

    audit_id = audit_dimension.insert(audit_obj)

    i = 0
    isCreatingRoute = False

    for row in ais_source:
        i = i + 1
        if (i % 100000 == 0):
            #break
            print("Reached milestone: " + str(i))
            print(datetime.now())
        sog = row["sog"]
        if(sog != 0 and sog != None):
            if(not isCreatingRoute):
                current_ship_id = row["ship_id"]
                create_new_trajectory(row)
                isCreatingRoute = True
            else:
                if(current_ship_id == row["ship_id"]):
                    add_to_trajectory(row)
                else:
                    end_trajectory()
                    create_new_trajectory(row)
        else:
            if(isCreatingRoute):
                end_trajectory()
                isCreatingRoute = False

    for traj in trajectories:
        traj["audit_id"] = audit_id
        trajectory_fact_table.insert(traj)

    audit_obj['processed_records'] = len(trajectories)
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()

    # Close connections
    connection.close()
