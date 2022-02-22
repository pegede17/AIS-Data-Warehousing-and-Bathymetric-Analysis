from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import Dimension, FactTable
import pygrametl
from database_connection import connect_to_local, connect_via_ssh
import geopandas as gpd
import movingpandas as mpd
from shapely.geometry import Point
from datetime import date, datetime, timedelta
from sktime.transformations.series.outlier_detection import HampelFilter
import numpy as np  # linear algebra
# qr_cleaned_data processing, CSV file I/O (e.g. pd.read_csv)
import pandas as pd
import configparser
import pyproj
from helper_functions import create_audit_dimension, create_tables, create_trajectory_fact_table
import concurrent.futures
from time import perf_counter
import json
import gc

# Configurations and global variables
np.random.seed(0)
required_no_points = 5
hampel_filter = HampelFilter(window_length=required_no_points)
speed_split = 0.971922246  # 0.5 knots in metres /sec
max_speed = 18.0055556  # 35 knots in metres /sec


def set_global_variables(args):
    global trajectories_per_ship
    trajectories_per_ship = args


def apply_filter_on_trajectories(trajectory_list, filter_func, filter_length):
    trajectories = []

    for trajectory in trajectory_list:
        long = trajectory.to_point_gdf().geometry.x
        lat = trajectory.to_point_gdf().geometry.y

        if (len(long) >= filter_length and len(lat) >= filter_length):
            filtered_long = filter_func.fit_transform(long)
            filtered_lat = filter_func.fit_transform(lat)

            filtered_result = pd.concat([filtered_long, filtered_lat], axis=1, keys=[
                                        'long', 'lat']).dropna(axis=0)

            temp_gdf = gpd.GeoDataFrame(filtered_result.drop(
                ['long', 'lat'], axis=1), crs="EPSG:4326", geometry=gpd.points_from_xy(filtered_result.long, filtered_result.lat))
            trajectories.append(mpd.Trajectory(temp_gdf, 1))
        # else:
        #     print("Can't apply hampel filter, length of lat + long is not >= 5")
        #     filtered_result = pd.concat([long, lat], axis=1, keys=['long', 'lat']).dropna(axis=0)

        #     temp_gdf = gpd.GeoDataFrame(filtered_result.drop(['long', 'lat'], axis=1), crs="EPSG:4326", geometry=gpd.points_from_xy(filtered_result.long, filtered_result.lat))
        #     trajectories.append(mpd.Trajectory(temp_gdf, 1))

    trajectory_collection = mpd.TrajectoryCollection(trajectories, 't')
    del trajectories
    del trajectory_list

    return trajectory_collection


def apply_trajectory_manipulation(list):
    print(list)
    mmsi, qr_cleaned_data = list

    qr_cleaned_data['speed'] = qr_cleaned_data['sog']
    trajectory = mpd.Trajectory(qr_cleaned_data, mmsi)

    if (trajectory.size() <= required_no_points):
        return

    if not (trajectory.is_valid()):
        return

    # Define and split trajectories based on idle duration
    stops = mpd.SpeedSplitter(trajectory).split(duration=timedelta(
        minutes=5), speed=speed_split, max_speed=max_speed)

    # Apply Hampel filter on trajectories
    filtered_trajectories = apply_filter_on_trajectories(
        stops, hampel_filter, required_no_points)

    # Simplify trajectories using douglas peucker algorithm
    traj_simplified = mpd.DouglasPeuckerGeneralizer(
        filtered_trajectories).generalize(tolerance=0.0001)

    trajectories_per_ship[mmsi] = traj_simplified

    del list
    del mmsi
    del qr_cleaned_data
    del stops
    del filtered_trajectories


def testingPOC(date_to_lookup, config):

    trajectories_per_ship = {}

    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    # Queries defined
    query = """
    SELECT ship_type_id, ts_date_id, ship_id, ts_time_id, audit_id, ST_X(coordinate::geometry) as long, ST_Y(coordinate::geometry) as lat, sog, hour, minute, second, draught
    FROM fact_ais_clean_v2
    INNER JOIN dim_time ON dim_time.time_id = ts_time_id
    WHERE ts_date_id = 20211026 AND ship_id = 3673
    ORDER BY ts_time_id ASC
    """

    date_query = """
    SELECT year, month, day
    FROM dim_date
    where date_id = 20211026
    """

    t_query_execution_start = perf_counter()

    qr_cleaned_data = SQLSource(connection=connection, query=query)
    qr_date_details = SQLSource(connection=connection, query=date_query)

    t_query_execution_stop = perf_counter()

    t_dataframe_creation_start = perf_counter()
    data_date_df = pd.DataFrame(qr_date_details)
    del qr_date_details

    data_trajectories = pd.DataFrame(qr_cleaned_data)
    del qr_cleaned_data

    data_trajectories['year'] = int(data_date_df['year'])
    data_trajectories['month'] = int(data_date_df['month'])
    data_trajectories['day'] = int(data_date_df['day'])

    print(list(data_trajectories.columns))

    data_trajectories['t'] = pd.to_datetime(
        data_trajectories[['year', 'month', 'day', 'hour', 'minute', 'second']])
    data_trajectories = data_trajectories.set_index('t')

    print(data_trajectories)

    gdf = gpd.GeoDataFrame(data_trajectories, crs='EPSG:4326', geometry=gpd.points_from_xy(
        data_trajectories.long, data_trajectories.lat))
    del data_trajectories

    # TODO-Future: Research faster way to group. Maybe this? https://stackoverflow.com/questions/38143717/groupby-in-python-pandas-fast-way
    gdf_grouped = gdf.groupby(by=['ship_id'])
    del gdf

    print("Finished grouping and converting to gdf! Size: ", len(gdf_grouped))
    t_dataframe_creation_stop = perf_counter()

    # Generate draught for each ship
    t_draught_calculation_start = perf_counter()
    draught_per_ship = {}
    for mmsi, qr_cleaned_data in gdf_grouped:
        draughts = qr_cleaned_data.draught.value_counts().reset_index(
            name='Count').sort_values(['Count'], ascending=False)['index'].tolist()
        if (len(draughts) > 0):
            draught_per_ship[mmsi] = draughts
        else:
            draught_per_ship[mmsi] = None
    t_draught_calculation_stop = perf_counter()

    # Create dictionary with ship type id based on their MMSI
    shiptype_based_on_mmsi = {}
    for mmsi, qr_cleaned_data in gdf_grouped:
        type = qr_cleaned_data.ship_type_id.value_counts().reset_index(
            name='Count').sort_values(['Count'], ascending=False)['index'].tolist()[0]
        shiptype_based_on_mmsi[mmsi] = type

    t_multiprocessing_start = perf_counter()

    # Multiprocessing
    # apply_trajectory_manipulation(gdf_grouped)


    for mmsi, qr_cleaned_data in gdf_grouped:
        qr_cleaned_data['speed'] = qr_cleaned_data['sog']
        trajectory = mpd.Trajectory(qr_cleaned_data, mmsi)

        if (trajectory.size() <= required_no_points):
            return

        if not (trajectory.is_valid()):
            return

        # Define and split trajectories based on idle duration
        stops = mpd.SpeedSplitter(trajectory).split(duration=timedelta(
            minutes=5), speed=speed_split, max_speed=max_speed)

        # Apply Hampel filter on trajectories
        filtered_trajectories = apply_filter_on_trajectories(
            stops, hampel_filter, required_no_points)

        # Simplify trajectories using douglas peucker algorithm
        traj_simplified = mpd.DouglasPeuckerGeneralizer(
            filtered_trajectories).generalize(tolerance=0.0001)

        trajectories_per_ship[mmsi] = traj_simplified

    t_multiprocessing_stop = perf_counter()
    del gdf_grouped

    temp_trajectories = []

    t_db_traj_insertion_start = perf_counter()
    for ship in trajectories_per_ship:
        for traj in trajectories_per_ship[ship]:
            trajectory_dto = {
                'ship_id': ship,
                'duration': traj.get_duration().total_seconds(),
                'time_start_id': int(traj.get_start_time().strftime("%H%M%S")),
                'date_start_id': int(traj.get_start_time().strftime("%Y%m%d")),
                'time_end_id': int(traj.get_end_time().strftime("%H%M%S")),
                'date_end_id': int(traj.get_end_time().strftime("%Y%m%d")),
                'coordinates': str(traj.to_linestring()),
                'length_meters': traj.get_length(),
                'total_points': traj.size(),
                'draught': draught_per_ship[ship],
                'ship_type_id': int(shiptype_based_on_mmsi[ship])
            }
            temp_trajectories.append(trajectory_dto)

    t_db_traj_insertion_stop = perf_counter()

    # Duration calculation to format: (H:M:S)
    t_query_execution_duration = str(
        timedelta(seconds=(t_query_execution_stop - t_query_execution_start)))
    t_dataframe_creation_duration = str(
        timedelta(seconds=(t_dataframe_creation_stop - t_dataframe_creation_start)))
    t_draught_calculation_duration = str(
        timedelta(seconds=(t_draught_calculation_stop - t_draught_calculation_start)))
    t_multiprocessing_duration = str(
        timedelta(seconds=(t_multiprocessing_stop - t_multiprocessing_start)))
    t_db_traj_insertion_duration = str(
        timedelta(seconds=(t_db_traj_insertion_stop - t_db_traj_insertion_start)))

    print(temp_trajectories)

    del trajectories_per_ship

    gc.collect(generation=2)

    # Close connections
    connection.close()
