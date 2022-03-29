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
    # return math.hypot(abs(lat2 - lat1), abs(lon2 - lon1))
    # return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)


def traj_splitter(journey: pd.DataFrame, speed_treshold, time_threshold, SOG_limit):
    first_point_not_handled = -1
    temp_trajectory_list = pd.DataFrame(columns=journey.columns)
    temp_stop_list = pd.DataFrame(columns=journey.columns)
    ship_trajectories = []
    ship_stops = []
    time_since_above_threshold = timedelta(minutes=0)
    last_point_over_threshold = 0

    journey = journey.reset_index(0)
    journey["time"] = journey.apply(lambda row: datetime(year=1, month=1, day=1, hour=row['hour'],
                                                         minute=row['minute'], second=row['second']), axis=1)

    for i in journey.index:

        # journey.at(i)['time'] = datetime.strptime(str(journey.at(i)['ts_time_id']), "%H%M%S")
        # TODO brug apply måske
        # journey.at[i, 'time'] = datetime(year=1, month=1, day=1, hour=journey.at[i, 'hour'],
        #                                  minute=journey.at[i, 'minute'], second=journey.at[i, 'second'])
        point = journey.iloc[i, :]

        # Determine time since last point or set to 0 if it is the first point
        if(i != 0):
            previous_point = journey.iloc[i-1, :]
            time_since_last_point = point.time - previous_point.time
            # distance_to_last_point = distance_in_km_between_earth_coordinates(
            #     point.geometry.x, point.geometry.y, previous_point.geometry.x, previous_point.geometry.y)
            distance_to_last_point = distance_in_km_between_earth_coordinates(
                point['lat'], point['long'], previous_point['lat'], previous_point['long']) * 1000
            # distance_to_last_point = point.geometry.distance(
            #     previous_point.geometry)
        else:
            time_since_last_point = timedelta(0)
            distance_to_last_point = 0

        # Set speed depending on if time has passed since last point/if we have a previous point
        # TODO check om nødvendig
        sog = float(point['sog'])
        if (i == 0 or distance_to_last_point == 0 or time_since_last_point == timedelta(0)):
            speed = sog
        else:
            # TODO fix enhed så det er i knob
            calculated_speed = distance_to_last_point/time_since_last_point.seconds
            if(abs(calculated_speed-sog) > 2):
                speed = calculated_speed
            else:
                speed = sog

        # TODO lav switch på de 4 cases
        # Split trajectories if it has been too long since last point
        if(time_since_last_point > timedelta(minutes=5)):
            # End current trajectory session and push to trajectory list
            if(len(temp_trajectory_list) > 0):
                # TODO slet dette check
                if(first_point_not_handled != -1):
                    temp_trajectory_list = pd.concat(
                        [temp_trajectory_list, journey.iloc[first_point_not_handled:i - 1, :]], axis=0)
                    first_point_not_handled = -1
                ship_trajectories.append(temp_trajectory_list.copy())
                # TODO check om mest effektiv måde at clear på
                temp_trajectory_list = temp_trajectory_list.iloc[0:0]

            else:
                if(first_point_not_handled != -1):
                    temp_stop_list = pd.concat(
                        [temp_stop_list, journey.iloc[first_point_not_handled:i - 1, :]], axis=0)
                    first_point_not_handled = -1
                ship_stops.append(temp_stop_list.copy())
                temp_stop_list = temp_stop_list.iloc[0:0]
        # Skip a point if it is has a too high speed/it is an outlier
        if(speed > SOG_limit):
            if(first_point_not_handled == -1):
                continue
            if(len(temp_stop_list) > 0):
                temp_stop_list = pd.concat(
                    [temp_stop_list, journey.iloc[first_point_not_handled:i - 1, :]], axis=0)
            elif (len(temp_trajectory_list) > 0):
                temp_trajectory_list = pd.concat(
                    [temp_trajectory_list, journey.iloc[first_point_not_handled:i - 1, :]], axis=0)
            first_point_not_handled = -1
            continue

        if(speed < speed_treshold):
            # Keep track of the first point with a low speed
            if(first_point_not_handled == -1):
                first_point_not_handled = i
            # Update time since above threshold
            # timeSinceAboveThreshold = (datetime.strptime(point['timestamp'], '%d-%m-%Y, %H:%M:%S') - datetime.strptime(journey[lastPointOverThreshold]['timestamp'],  '%d-%m-%Y, %H:%M:%S') )
            time_since_above_threshold = point['ts_time_id'] - \
                journey.at[last_point_over_threshold, 'ts_time_id']

            if(time_since_above_threshold <= time_threshold):
                continue

            if(time_since_above_threshold > time_threshold):
                # End "sailing session and push to trajectory list
                if(len(temp_trajectory_list) > 0):
                    ship_trajectories.append(temp_trajectory_list.copy())
                    temp_trajectory_list = temp_trajectory_list.iloc[0:0]
                # Add points to current stop session
                if(i == first_point_not_handled or first_point_not_handled == -1):
                    temp_stop_list.loc[len(
                        temp_stop_list.index)] = journey.iloc[i]
                else:
                    temp_stop_list = pd.concat(
                        [temp_stop_list, journey.iloc[first_point_not_handled:i, :]], axis=0)
                first_point_not_handled = -1
                # if(first_point_not_handled != -1 and first_point_not_handled != i):
                #     temp_stop_list = pd.concat(
                #         [temp_stop_list, journey.iloc[first_point_not_handled:i, :]], axis=0)
                # else:
                #     temp_stop_list = pd.concat(
                #         [temp_stop_list, journey.iloc[first_point_not_handled:i, :]], axis=0)
                # first_point_not_handled = -1

        if(speed >= speed_treshold):
            # Reset counters
            last_point_over_threshold = i
            # End a "stopped" session and push to stops list
            if(len(temp_stop_list) > 0):
                ship_stops.append(temp_stop_list.copy())
                temp_stop_list = temp_stop_list.iloc[0:0]
            # Add points to current trajectory
            if(first_point_not_handled != -1):
                temp_trajectory_list = pd.concat(
                    [temp_trajectory_list, journey.iloc[first_point_not_handled:i, :]], axis=0)
            else:
                temp_trajectory_list = pd.concat(
                    [temp_trajectory_list, journey.iloc[i, :].to_frame().transpose()], axis=0)
            time_since_above_threshold = timedelta(minutes=0)
            first_point_not_handled = -1

    if(len(temp_stop_list) > 0):
        ship_stops.append(temp_stop_list.copy())
        temp_stop_list = temp_stop_list.iloc[0:0]
    if(len(temp_trajectory_list) > 0):
        ship_trajectories.append(temp_trajectory_list.copy())
        temp_trajectory_list = temp_trajectory_list.iloc[0:0]

    return ship_trajectories, ship_stops


# gets all AIS data from a given day, to create a journey - then calls splitter - and sets trajectories into database
def create_trajectories(date_to_lookup, config):

    def insert_trajectory(trajectory, sailing: bool):
        geoSeries = gpd.GeoSeries.from_wkb(
            trajectory['coordinate'], crs=4326)
        geoSeries = geoSeries.to_crs("epsg:3034")
        trajectory = gpd.GeoDataFrame(
            trajectory, crs='EPSG:3034', geometry=geoSeries)

        if(len(trajectory) < 2):
            return 0
        linestring = LineString(
            [p for p in list(zip(trajectory.long, trajectory.lat))])
        projected_linestring = LineString([p for p in trajectory.geometry])
        duration = trajectory.time.iat[-1] - trajectory.time.iat[0]
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
            "audit_id": audit_sailing_id if sailing else audit_stopped_id,
            "draught": draughts,
            "duration": duration.seconds,
            "coordinates": str(linestring.simplify(tolerance=0.0001)),
            "length_meters": projected_linestring.length,
            "avg_speed_knots": projected_linestring.length / duration.seconds,  # FIX
            "total_points": len(trajectory)
        }
        if (sailing):
            trajectory_sailing_fact_table.insert(database_object)
        else:
            trajectory_stopped_fact_table.insert(database_object)
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
        FROM fact_ais
        INNER JOIN dim_time ON dim_time.time_id = ts_time_id
        WHERE ts_date_id = {date_to_lookup}
        ORDER BY ship_id, ts_time_id ASC
        limit(1000)
        '''

    # translate query to groupby dataframe on ship id
    all_journeys_as_dataframe = pd.DataFrame(
        SQLSource(connection=connection, query=query_get_all_ais_from_date)).groupby(['ship_id'])
    # geoSeries = gpd.GeoSeries.from_wkb(
    #     all_journeys_as_dataframe['coordinate'], crs=4326)
    # geoSeries = geoSeries.to_crs("epsg:3034")
    # all_journeys_as_geodataframe = gpd.GeoDataFrame(
    #     all_journeys_as_dataframe, crs='EPSG:3034', geometry=geoSeries).groupby(['ship_id'])

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
    t_test_start = perf_counter()
    for _, ship in all_journeys_as_dataframe:
        processed_records = processed_records + len(ship)
        sailing, stopped = traj_splitter(ship, speed_treshold=0.5,
                                         time_threshold=300, SOG_limit=200)
        inserted_sailing_records = inserted_sailing_records
        for trajectory in sailing:
            inserted_sailing_records += insert_trajectory(trajectory, True)
        for trajectory in stopped:
            inserted_stopped_records += insert_trajectory(trajectory, False)

    t_end = perf_counter()

    print(timedelta(minutes=(t_end-t_test_start)))

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
