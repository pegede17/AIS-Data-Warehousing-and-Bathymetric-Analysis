from time import perf_counter, perf_counter_ns
import pygrametl
from database_connection import connect_to_local, connect_via_ssh
from pygrametl.datasources import SQLSource
import pandas as pd
import math
from datetime import date, datetime, timedelta
from helper_functions import create_trajectory_sailing_fact_table, create_trajectory_stopped_fact_table, create_audit_dimension
from shapely.geometry import LineString
import geopandas as gpd
import multiprocessing as mp
import concurrent.futures


def set_global_variables(args):
    global trajectories_per_ship
    trajectories_per_ship = args


def degrees_to_radians(degrees):
    return degrees * math.pi / 180


def distance_in_km_between_earth_coordinates(lat1, lon1, lat2, lon2):
    earth_radius_km = 6371

    dLat = degrees_to_radians(lat2-lat1)
    dLon = degrees_to_radians(lon2-lon1)

    lat1 = degrees_to_radians(lat1)
    lat2 = degrees_to_radians(lat2)
    a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * \
        math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return earth_radius_km * c


def get_distance_and_time_since_last_point(point, previous_point, i):
    if(i != 0):
        time_since_last_point = point.time - previous_point.time
        meters_to_last_point = distance_in_km_between_earth_coordinates(
            point.lat, point.long, previous_point.lat, previous_point.long) * 1000
    else:
        time_since_last_point = timedelta(0)
        meters_to_last_point = 0
    return time_since_last_point, meters_to_last_point


def get_speed_in_knots(meters_to_last_point, time_since_last_point, point, i):
    sog = float(point.sog)
    if (i == 0 or meters_to_last_point == 0 or time_since_last_point.seconds == 0):
        return sog
    else:
        # TODO fix enhed så det er i knob
        calculated_speed = meters_to_last_point/time_since_last_point.seconds * 1.94
        if(abs(calculated_speed-sog) > 2):
            return calculated_speed
        else:
            return sog


def handle_time_gap(points: pd.DataFrame, trajectories: list, first_point_not_handled: int, i: int, journey: pd.DataFrame):
    if(first_point_not_handled != -1):
        points = pd.concat(
            [points, journey.iloc[first_point_not_handled:i - 1, :]])
        first_point_not_handled = -1
    trajectories.append(points.copy())
    points = points.iloc[0:0, :]
    return trajectories, points, first_point_not_handled


def skip_point(points: pd.DataFrame, first_point_not_handled: int, i: int, journey: pd.DataFrame):
    # time_skip_start = perf_counter_ns()
    points = pd.concat(
        [points, journey.iloc[first_point_not_handled:i - 1, :]])
    first_point_not_handled = -1
    # time_skip_end = perf_counter_ns()
    # timer.append(time_skip_end-time_skip_start)

    return points, first_point_not_handled


def create_database_object(trajectory):
    if(len(trajectory) <= 2):
        return None
    linestring = LineString(
        [p for p in list(zip(trajectory.long, trajectory.lat))])
    projected_linestring = LineString([p for p in trajectory.geometry])
    duration = trajectory.time.iat[-1] - trajectory.time.iat[0]
    if (duration.seconds == 0):
        return None
    draughts = trajectory.draught.value_counts().reset_index(
        name='Count').sort_values(['Count'], ascending=False)['index'].tolist()
    if (len(draughts) == 0):
        draughts = None
    database_object = {
        "ship_id": int(trajectory.ship_id.iat[0]),
        "ship_type_id": int(trajectory.ship_type_id.iat[0]),
        "type_of_mobile_id": int(trajectory.type_of_mobile_id.iat[0]),
        "eta_time_id": int(trajectory.eta_time_id.value_counts().reset_index(
            name='Count').sort_values(['Count'], ascending=False)['index'].tolist()[0]),
        "eta_date_id": int(trajectory.eta_date_id.value_counts().reset_index(
            name='Count').sort_values(['Count'], ascending=False)['index'].tolist()[0]),
        "time_start_id": int(trajectory.ts_time_id.sort_values(ascending=True).tolist()[0]),
        "date_start_id": int(trajectory.ts_date_id.sort_values(ascending=True).tolist()[0]),
        "time_end_id": int(trajectory.ts_time_id.sort_values(ascending=False).tolist()[0]),
        "date_end_id": int(trajectory.ts_date_id.sort_values(ascending=False).tolist()[0]),
        "cargo_type_id": int(trajectory.cargo_type_id.value_counts().reset_index(
            name='Count').sort_values(['Count'], ascending=False)['index'].tolist()[0]),
        "destination_id": int(trajectory.destination_id.value_counts().reset_index(
            name='Count').sort_values(['Count'], ascending=False)['index'].tolist()[0]),
        "data_source_type_id": int(trajectory.data_source_type_id.value_counts().reset_index(
            name='Count').sort_values(['Count'], ascending=False)['index'].tolist()[0]),
        "type_of_position_fixing_device_id": int(trajectory.type_of_position_fixing_device_id.value_counts().reset_index(
            name='Count').sort_values(['Count'], ascending=False)['index'].tolist()[0]),
        "audit_id": -1,
        "draught": draughts,
        "duration": duration.seconds,
        "coordinates": str(linestring.simplify(tolerance=0.0001)),
        "length_meters": projected_linestring.length,
        "avg_speed_knots": projected_linestring.length / duration.seconds,  # FIX
        "total_points": len(trajectory)
    }
    return database_object


def traj_splitter(ship):
    speed_treshold = 0.5
    time_threshold = 300
    SOG_limit = 100
    id, journey = ship
    print(id)

    journey = journey.reset_index(0)
    journey["time"] = journey.apply(lambda row: datetime(year=1, month=1, day=1, hour=row['hour'],
                                                         minute=row['minute'], second=row['second']), axis=1)

    first_point_not_handled = -1
    sailing_points = pd.DataFrame(columns=journey.columns)
    stopped_points = pd.DataFrame(columns=journey.columns)
    sailing_trajectories = []
    stopped_trajectories = []
    time_since_above_threshold = timedelta(minutes=0)
    last_point_over_threshold_index = 0
    # time_initial = []
    # time_skip = []
    # time_above = []
    # time_below = []

    for i in journey.index:
        # time_initial_start = perf_counter_ns()
        point = journey.iloc[i, :]

        # Determine time since last point or set to 0 if it is the first point
        time_since_last_point, meters_to_last_point = get_distance_and_time_since_last_point(
            point, journey.iloc[i-1, :], i)

        # Set speed depending on if time has passed since last point/if we have a previous point
        speed = get_speed_in_knots(meters_to_last_point,
                                   time_since_last_point, point, i)

        # TODO lav switch på de 4 cases
        # Split trajectories if it has been too long since last point
        if(time_since_last_point > timedelta(minutes=5)):
            # End current trajectory session and push to trajectory list
            if(len(sailing_points) > 0):
                sailing_trajectories, sailing_points, first_point_not_handled = handle_time_gap(
                    sailing_points, sailing_trajectories, first_point_not_handled, i, journey)
            else:
                stopped_trajectories, stopped_points, first_point_not_handled = handle_time_gap(
                    stopped_points, stopped_trajectories, first_point_not_handled, i, journey)
        # time_initial_end = perf_counter_ns()
        # time_initial.append(time_initial_end-time_initial_start)

        # Skip a point if it is has a too high speed/it is an outlier
        if(speed > SOG_limit):
            if(first_point_not_handled == -1):
                continue
            if(len(stopped_points) > 0):
                stopped_points, first_point_not_handled = skip_point(
                    stopped_points, first_point_not_handled, i, journey)
                continue
            elif (len(sailing_points) > 0):
                sailing_points, first_point_not_handled = skip_point(
                    sailing_points, first_point_not_handled, i, journey)
                continue

        if(speed >= speed_treshold):
            # time_above_start = perf_counter_ns()
            # Reset counters
            last_point_over_threshold_index = i
            # End a "stopped" session and push to stops list
            if(len(stopped_points) > 0):
                stopped_trajectories.append(stopped_points.copy())
                stopped_points = stopped_points.iloc[0:0]
            # Add points to current trajectory
            if(first_point_not_handled != -1):
                sailing_points = pd.concat(
                    [sailing_points, journey.iloc[first_point_not_handled:i, :]])
            else:
                sailing_points.loc[len(
                    sailing_points.index)] = journey.iloc[i]
            time_since_above_threshold = timedelta(minutes=0)
            first_point_not_handled = -1
            # time_above_end = perf_counter_ns()
            # time_above.append(time_above_end-time_above_start)
            continue

        if(speed < speed_treshold):
            # time_below_start = perf_counter_ns()
            # Keep track of the first point with a low speed
            if(first_point_not_handled == -1):
                first_point_not_handled = i
            # Update time since above threshold
            # timeSinceAboveThreshold = (datetime.strptime(point['timestamp'], '%d-%m-%Y, %H:%M:%S') - datetime.strptime(journey[lastPointOverThreshold]['timestamp'],  '%d-%m-%Y, %H:%M:%S') )
            time_since_above_threshold = point['ts_time_id'] - \
                journey.at[last_point_over_threshold_index, 'ts_time_id']

            if(time_since_above_threshold <= time_threshold):
                continue

            if(time_since_above_threshold > time_threshold):
                # End "sailing session and push to trajectory list
                if(len(sailing_points) > 0):
                    sailing_trajectories.append(sailing_points.copy())
                    sailing_points = sailing_points.iloc[0:0]
                # Add points to current stop session
                if(i == first_point_not_handled or first_point_not_handled == -1):
                    stopped_points.loc[len(
                        stopped_points.index)] = journey.iloc[i]
                else:
                    stopped_points = pd.concat(
                        [stopped_points, journey.iloc[first_point_not_handled:i, :]])
                first_point_not_handled = -1
            # time_below_end = perf_counter_ns()
            # time_below.append(time_below_end-time_below_start)

    if(len(stopped_points) > 0):
        stopped_trajectories.append(stopped_points.copy())
        stopped_points = stopped_points.iloc[0:0, :]
    if(len(sailing_points) > 0):
        sailing_trajectories.append(sailing_points.copy())
        sailing_points = sailing_points.iloc[0:0, :]
    # if(len(time_initial) > 0):
    #     print(f"{'Initial:':<12}" + str(sum(time_initial)/len(time_initial)))
    # if(len(time_skip) > 0):
    #     print(f"{'Skip:':<12}" + str(sum(time_skip)/len(time_skip)))
    # if(len(time_above) > 0):
    #     print(f"{'Above:':<12}" + str(sum(time_above)/len(time_above)))
    # if(len(time_below) > 0):
    #     print(f"{'Below:':<12}" + str(sum(time_below)/len(time_below)))
    stopped_db_objects = []
    for trajectory in stopped_trajectories:
        geoSeries = gpd.GeoSeries.from_wkb(
            trajectory['coordinate'], crs=4326)
        geoSeries = geoSeries.to_crs("epsg:3034")
        trajectory = gpd.GeoDataFrame(
            trajectory, crs='EPSG:3034', geometry=geoSeries)
        stopped_db_objects.append(create_database_object(trajectory))

    sailing_db_objects = []
    for trajectory in sailing_trajectories:
        geoSeries = gpd.GeoSeries.from_wkb(
            trajectory['coordinate'], crs=4326)
        geoSeries = geoSeries.to_crs("epsg:3034")
        trajectory = gpd.GeoDataFrame(
            trajectory, crs='EPSG:3034', geometry=geoSeries)
        sailing_db_objects.append(create_database_object(trajectory))

    trajectories = {"stopped": stopped_trajectories,
                    "stopped_db_objects": stopped_db_objects,
                    "sailing": sailing_trajectories,
                    "sailing_db_objects": sailing_db_objects}
    trajectories_per_ship[id] = trajectories


# gets all AIS data from a given day, to create a journey - then calls splitter - and sets trajectories into database
def create_trajectories(date_to_lookup, config):

    def insert_trajectory(trajectory_db_object, sailing: bool):
        if(trajectory_db_object == None):
            return 0
        if (sailing):
            trajectory_db_object["audit_id"] = audit_sailing_id
            trajectory_sailing_fact_table.insert(trajectory_db_object)
        else:
            trajectory_db_object["audit_id"] = audit_stopped_id
            trajectory_stopped_fact_table.insert(trajectory_db_object)
        return 1

    t_start = perf_counter()
    # sets connetction
    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    # query to select all AIS points from the given day
    query_get_all_ais_from_date = f'''
        SELECT ship_type_id, eta_time_id, eta_date_id, cargo_type_id, type_of_mobile_id, destination_id, ts_date_id, data_source_type_id, type_of_position_fixing_device_id, ship_id, ts_time_id,
                audit_id, coordinate, ST_X(coordinate::geometry) as long, ST_Y(coordinate::geometry) as lat, sog, hour, minute, second, draught
        FROM fact_ais_clean
        INNER JOIN dim_time ON dim_time.time_id = ts_time_id
        WHERE ts_date_id = {date_to_lookup}
        ORDER BY ship_id, ts_time_id ASC
        limit(1000000)
        '''

    # translate query to groupby dataframe on ship id
    all_journeys_as_dataframe = pd.DataFrame(
        SQLSource(connection=connection, query=query_get_all_ais_from_date)).groupby(['ship_id'])

    trajectory_sailing_fact_table = create_trajectory_sailing_fact_table()
    trajectory_stopped_fact_table = create_trajectory_stopped_fact_table()
    audit_dimension = create_audit_dimension()

    sailing_audit_obj = {'timestamp': datetime.now(),
                         'processed_records': 0,
                         'inserted_records': 0,
                         'etl_duration': timedelta(minutes=0),
                         'source_system': config["Audit"]["source_system"],
                         'etl_version': config["Audit"]["elt_version"],
                         'table_name': trajectory_sailing_fact_table.name,
                         'description': config["Audit"]["comment"]}

    stopped_audit_obj = {'timestamp': datetime.now(),
                         'processed_records': 0,
                         'inserted_records': 0,
                         'etl_duration': timedelta(minutes=0),
                         'source_system': config["Audit"]["source_system"],
                         'etl_version': config["Audit"]["elt_version"],
                         'table_name': trajectory_stopped_fact_table.name,
                         'description': config["Audit"]["comment"]}

    audit_sailing_id = audit_dimension.insert(sailing_audit_obj)
    audit_stopped_id = audit_dimension.insert(stopped_audit_obj)

    processed_records = 0
    inserted_sailing_records = 0
    inserted_stopped_records = 0

    trajectories_per_ship = mp.Manager().dict()
    with concurrent.futures.ProcessPoolExecutor(initializer=set_global_variables, initargs=(trajectories_per_ship,)) as executor:
        executor.map(traj_splitter, all_journeys_as_dataframe)

    t_test_end = perf_counter()

    for _, ship in all_journeys_as_dataframe:
        processed_records = processed_records + len(ship)
    for ship in trajectories_per_ship:
        if(len(trajectories_per_ship[ship]["sailing_db_objects"]) > 0):
            for trajectory in trajectories_per_ship[ship]["sailing_db_objects"]:
                inserted_sailing_records += insert_trajectory(trajectory, True)
        if(len(trajectories_per_ship[ship]["stopped_db_objects"]) > 0):
            for trajectory in trajectories_per_ship[ship]["stopped_db_objects"]:
                inserted_stopped_records += insert_trajectory(
                    trajectory, False)

    t_end = perf_counter()

    print(timedelta(minutes=(t_test_end-t_start)))

    sailing_audit_obj['processed_records'] = processed_records
    sailing_audit_obj['inserted_records'] = inserted_sailing_records
    sailing_audit_obj['etl_duration'] = timedelta(seconds=(t_end - t_start))
    sailing_audit_obj['audit_id'] = audit_sailing_id

    stopped_audit_obj['processed_records'] = processed_records
    stopped_audit_obj['inserted_records'] = inserted_stopped_records
    stopped_audit_obj['etl_duration'] = timedelta(seconds=(t_end - t_start))
    stopped_audit_obj['audit_id'] = audit_stopped_id

    audit_dimension.update(sailing_audit_obj)
    audit_dimension.update(stopped_audit_obj)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()
    connection.close()
