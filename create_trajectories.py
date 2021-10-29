from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import Dimension, FactTable
import pygrametl
from database_connection import connect_to_local, connect_via_ssh
import geopandas as gpd
import movingpandas as mpd
from shapely.geometry import Point
from datetime import datetime, timedelta
from sktime.transformations.series.outlier_detection import HampelFilter
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import configparser
import pyproj
from datetime import datetime
from helper_functions import create_audit_dimension, create_tables, create_trajectory_fact_table
import concurrent.futures
import multiprocessing as mp

## Configurations
# Libraries
pd.set_option('display.max_columns', 50)
np.set_printoptions('threshold', 2000)
np.random.seed(0)

# File specific settings
filtered_points = 5
hampel_filter = HampelFilter(window_length=filtered_points)
version = 4
date_to_lookup = 20211008

def set_global(args):
    global trajectories_per_ship
    trajectories_per_ship = args

def apply_filter_on_trajectories(trajectory_list, filter_func, filter_length):
    trajectories = []

    for trajectory in trajectory_list:
        long = trajectory.to_point_gdf().geometry.x
        lat = trajectory.to_point_gdf().geometry.y

        if (len(long) > filter_length and len(lat) > filter_length):
            filtered_long = filter_func.fit_transform(long)
            filtered_lat = filter_func.fit_transform(lat)

            #filtered_long.apply(lambda p: print(p))
            filtered_result = pd.concat([filtered_long, filtered_lat], axis=1, keys=['long', 'lat']).dropna(axis=0)

            test_gdf = gpd.GeoDataFrame(filtered_result.drop(['long', 'lat'], axis=1), crs="EPSG:4326", geometry=gpd.points_from_xy(filtered_result.long, filtered_result.lat))
            trajectories.append(mpd.Trajectory(test_gdf, 1))
        else:
            filtered_result = pd.concat([long, lat], axis=1, keys=['long', 'lat']).dropna(axis=0)

            test_gdf = gpd.GeoDataFrame(filtered_result.drop(['long', 'lat'], axis=1), crs="EPSG:4326", geometry=gpd.points_from_xy(filtered_result.long, filtered_result.lat))
            trajectories.append(mpd.Trajectory(test_gdf, 1))
        
    trajectory_collection = mpd.TrajectoryCollection(trajectories, 't')

    return trajectory_collection

def debug_apply(list):
    print("Starting debug apply")
    mmsi, data = list

    if (len(data) < 2):
        return
    
    # print("("+str(mmsi)+"):" + str(i) + ' ud af ' + str(total))

    # time_now = datetime.now()
    # 0.5 knot => meter /sec
    speed_limit = 0.971922246 
    data['speed'] = data['sog']
    trajectory = mpd.Trajectory(data, mmsi)
    # time_next = (time_now - datetime.now()).total_seconds()
    # print("Trajectory creation: " + str(time_next))

    if not (trajectory.is_valid()):
        return

    ## Define and split trajectories based on idle duration
    # time_now = datetime.now()
    stops = mpd.SpeedSplitter(trajectory).split(duration=timedelta(minutes=5), speed=speed_limit)
    # time_next = (time_now - datetime.now()).total_seconds()
    # print("Stop creation: " + str(time_next))
    # print("Stops: " + str(len(stops)))

    ## Simplify trajectories using douglas peucker algorithm
    # time_now = datetime.now()
    traj_simplified = mpd.DouglasPeuckerGeneralizer(stops).generalize(tolerance=0.0001)
    # time_next = (time_now - datetime.now()).total_seconds()
    # print("Simplifying: " + str(time_next))

    ## Apply Hampel filter on trajectories
    # time_now = datetime.now()
    filtered_trajectories = apply_filter_on_trajectories(traj_simplified, hampel_filter, filtered_points)
    # time_next = (time_now - datetime.now()).total_seconds()
    # print("Filtering: " + str(time_next))

    trajectories_per_ship[mmsi] = filtered_trajectories
    # print("--------")

if __name__ == '__main__':
    # Config settings
    config = configparser.ConfigParser()
    config.read('application.properties')
        
    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    # Queries defined
    query = """
    SELECT fact_id, ts_date_id, ship_id, ts_time_id, audit_id, ST_X(coordinate::geometry) as long, ST_Y(coordinate::geometry) as lat, sog, hour, minute, second, draught
    FROM fact_ais
    INNER JOIN dim_time ON dim_time.time_id = ts_time_id
    WHERE ts_date_id = {}
    """.format(date_to_lookup)

    date_query = """
    SELECT year, month, day
    FROM dim_date
    where date_id = {}
    """.format(date_to_lookup)

    create_query = """
    CREATE TABLE IF NOT EXISTS fact_trajectory_clean_v{} (LIKE fact_trajectory INCLUDING ALL);
    """.format(version)

    cur = connection.cursor()
    cur.execute(create_query)

    data = SQLSource(connection=connection, query=query)
    data_date = SQLSource(connection=connection, query=date_query)

    trajectory_fact_table = create_trajectory_fact_table("fact_trajectory_clean_v{}".format(version))

    audit_dimension = create_audit_dimension()

    audit_obj = {'timestamp': datetime.now(),
                    'source_system': config["Audit"]["source_system"],
                    'etl_version': config["Audit"]["elt_version"],
                    'comment': config["Audit"]["comment"],
                    'table_name': trajectory_fact_table.name,
                    'processed_records': 0}

    data_date_df = pd.DataFrame(data_date)

    data_trajectories = pd.DataFrame(data)
    data_trajectories['year'] = int(data_date_df['year'])
    data_trajectories['month'] = int(data_date_df['month'])
    data_trajectories['day'] = int(data_date_df['day'])

    print(list(data_trajectories.columns))

    data_trajectories['t'] = pd.to_datetime(data_trajectories[['year', 'month', 'day', 'hour', 'minute', 'second']])
    data_trajectories = data_trajectories.set_index('t')


    gdf = gpd.GeoDataFrame(data_trajectories, crs='EPSG:4326', geometry=gpd.points_from_xy(data_trajectories.long, data_trajectories.lat))
    gdf_grouped = gdf.groupby(by=['ship_id'])
    print("Finished grouping")
    print(len(gdf_grouped))

    # Generate draught for each ship
    draught_per_ship = {}
    for mmsi, data in gdf_grouped:
        draughts = data.draught.value_counts().reset_index(name='Count').sort_values(['Count'], ascending=False)['index'].tolist()
        if (len(draughts) > 0):
            draught_per_ship[mmsi] = draughts
        else:
            draught_per_ship[mmsi] = None

    # Function that takes a trajectory collection and creates a filtered set of trajectories

    i = 1
    total = len(gdf_grouped.groups)
    
        # print(len(gdf_grouped.get_group(mmsi).columns))

    ## Generate a trajectory for each ship
    start_time = datetime.now()

    trajectories_per_ship = mp.Manager().dict()
    with concurrent.futures.ProcessPoolExecutor(initializer=set_global, initargs=(trajectories_per_ship,)) as executor:
        executor.map(debug_apply, gdf_grouped)

    print((start_time - datetime.now()).total_seconds())

    print(len(trajectories_per_ship))

    audit_id = audit_dimension.insert(audit_obj)

    for ship in trajectories_per_ship:
        for traj in trajectories_per_ship[ship]:
            trajectory_dto = {
                'ship_id': ship,
                'duration': traj.get_duration().total_seconds(),
                'time_start_id': int(traj.get_start_time().strftime("%H%M%S")),
                'date_start_id': int(traj.get_start_time().strftime("%Y%m%d")),
                'time_end_id': int(traj.get_end_time().strftime("%H%M%S")),
                'date_end_id': int(traj.get_end_time().strftime("%Y%m%d")),
                'linestring': str(traj.to_linestring()),
                'length_meters': traj.get_length(),
                'audit_id': audit_id,
                'draught': draught_per_ship[ship]
                }

            trajectory_fact_table.insert(trajectory_dto)


    audit_obj['processed_records'] = len(trajectories_per_ship)
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()

    # Close connections
    connection.close()
