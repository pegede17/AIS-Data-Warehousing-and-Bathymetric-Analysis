from time import perf_counter, perf_counter_ns
import pygrametl
from utils.database_connection import connect_to_db
from pygrametl.datasources import SQLSource
import pandas as pd
import math
from datetime import date, datetime, timedelta
from utils.helper_functions import create_trajectory_sailing_fact_table, create_trajectory_stopped_fact_table, create_audit_dimension
from shapely.geometry import LineString
import geopandas as gpd
import multiprocessing as mp
import concurrent.futures
from datetime import datetime, timedelta
from math import ceil
import pandas as pd
import pygrametl
from pygrametl.datasources import SQLSource
from sqlalchemy import create_engine
from pyproj import Transformer
from haversine import haversine

# Global variables


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
        meters_to_last_point = haversine(
            (point.latitude, point.longitude), (previous_point.latitude, previous_point.longitude)) * 1000
    else:
        time_since_last_point = timedelta(0)
        meters_to_last_point = 0
    return time_since_last_point, meters_to_last_point


def get_speed_in_knots(meters_to_last_point, time_since_last_point, point, i):
    sog = float(point.sog)
    if (i == 0 or meters_to_last_point == 0):
        return sog
    elif(time_since_last_point == timedelta(0)):
        return 999
    else:
        calculated_speed = meters_to_last_point/time_since_last_point.seconds * 1.94
        if(abs(calculated_speed-sog) > 2):
            return calculated_speed
        else:
            return sog


def handle_time_gap(points: pd.DataFrame, trajectories: list, first_point_not_handled: int, i: int, journey: pd.DataFrame):
    if(first_point_not_handled != -1):
        points = pd.concat(
            [points, journey.iloc[first_point_not_handled:i, :]])
        first_point_not_handled = -1
    if(len(points) > 2):
        trajectories.append(points.copy())
    points = points.iloc[0:0, :]
    return trajectories, points, first_point_not_handled


def skip_point(points: pd.DataFrame, first_point_not_handled: int, i: int, journey: pd.DataFrame):
    points = pd.concat(
        [points, journey.iloc[first_point_not_handled:i, :]])
    first_point_not_handled = -1

    return points, first_point_not_handled


def create_database_object(trajectory):
    if(len(trajectory) <= 2):
        return None
    linestring = LineString(
        [p for p in list(zip(trajectory.longitude, trajectory.latitude))])
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
        "avg_speed_knots": projected_linestring.length / duration.seconds * 1.94,  # FIX
        "total_points": len(trajectory),
        "is_draught_trusted": 'True'
    }
    return database_object


def traj_splitter_except_catch(ship):
    try:
        traj_splitter(ship)
    except Exception as e:
        print(str(e))


def traj_splitter(ship):
    speed_treshold = 0.5
    time_threshold = 300
    SOG_limit = 100
    id, journey = ship

    journey["time"] = journey.apply(lambda row: datetime(year=1, month=1, day=1, hour=row['hour'],
                                                         minute=row['minute'], second=row['second']), axis=1)
    journey = journey.sort_values(
        by=['ts_time_id'], ascending=True).reset_index(0)

    last_point_not_skipped = 0
    first_point_not_handled = -1
    sailing_points = pd.DataFrame(columns=journey.columns)
    stopped_points = pd.DataFrame(columns=journey.columns)
    sailing_trajectories = []
    stopped_trajectories = []
    time_since_above_threshold = timedelta(minutes=0)
    last_point_over_threshold_index = 0

    for i in journey.index:
        point = journey.iloc[i, :]

        # Determine time since last point or set to 0 if it is the first point
        time_since_last_point, meters_to_last_point = get_distance_and_time_since_last_point(
            point, journey.iloc[last_point_not_skipped, :], i)

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

        # Skip a point if it is has a too high speed/it is an outlier
        if(speed > SOG_limit):
            if(first_point_not_handled == -1):
                continue
            elif(len(stopped_points) > 0):
                stopped_points, first_point_not_handled = skip_point(
                    stopped_points, first_point_not_handled, i, journey)
                first_point_not_handled = -1
                continue
            elif (len(sailing_points) > 0):
                sailing_points, first_point_not_handled = skip_point(
                    sailing_points, first_point_not_handled, i, journey)
                first_point_not_handled = -1
                continue
            else:
                first_point_not_handled = -1
                continue

        if(speed >= speed_treshold):
            # Reset counters
            last_point_over_threshold_index = i
            # End a "stopped" session and push to stops list
            if(len(stopped_points) > 0):
                if(len(stopped_points) > 2):
                    stopped_trajectories.append(stopped_points.copy())
                stopped_points = stopped_points.iloc[0:0]
            # Add points to current trajectory
            if(first_point_not_handled != -1):
                sailing_points = pd.concat(
                    [sailing_points, journey.iloc[first_point_not_handled:i+1, :]])
                last_point_not_skipped = i
            else:
                sailing_points = pd.concat(
                    [sailing_points, journey.iloc[i:i+1]])
                last_point_not_skipped = i
            time_since_above_threshold = timedelta(minutes=0)
            first_point_not_handled = -1
            continue

        if(speed < speed_treshold):
            # Keep track of the first point with a low speed
            if(first_point_not_handled == -1):
                first_point_not_handled = i
            # Update time since above threshold
            time_since_above_threshold = point['ts_time_id'] - \
                journey.at[last_point_over_threshold_index, 'ts_time_id']

            if(time_since_above_threshold <= time_threshold):
                continue

            if(time_since_above_threshold > time_threshold):
                # End "sailing session and push to trajectory list
                if(len(sailing_points) > 0):
                    if(len(sailing_points) > 2):
                        sailing_trajectories.append(sailing_points.copy())
                    sailing_points = sailing_points.iloc[0:0]
                # Add points to current stop session
                if(i == first_point_not_handled or first_point_not_handled == -1):
                    stopped_points = pd.concat(
                        [stopped_points, journey.iloc[i:i+1]])
                    last_point_not_skipped = i
                else:
                    stopped_points = pd.concat(
                        [stopped_points, journey.iloc[first_point_not_handled:i+1, :]])
                    last_point_not_skipped = i
                first_point_not_handled = -1

    if(len(stopped_points) > 2):
        stopped_trajectories.append(stopped_points.copy())
        stopped_points = stopped_points.iloc[0:0, :]
    if(len(sailing_points) > 2):
        sailing_trajectories.append(sailing_points.copy())
        sailing_points = sailing_points.iloc[0:0, :]

    stopped_db_objects = []
    for trajectory in stopped_trajectories:
        geoSeries = gpd.GeoSeries.from_wkb(
            trajectory['coordinate'], crs=4326)
        geoSeries = geoSeries.to_crs("epsg:3034")
        trajectory = gpd.GeoDataFrame(
            trajectory, crs='EPSG:3034', geometry=geoSeries)
        db_object = create_database_object(trajectory)
        if (db_object == None):
            continue
        stopped_db_objects.append(db_object)
        trajectory["stopped_traj_identifier"] = str(
            db_object["ship_id"]) + str(db_object["time_start_id"]) + "stopped"
        trajectory["sailing_traj_identifier"] = None

    sailing_db_objects = []
    for trajectory in sailing_trajectories:
        geoSeries = gpd.GeoSeries.from_wkb(
            trajectory['coordinate'], crs=4326)
        geoSeries = geoSeries.to_crs("epsg:3034")
        trajectory = gpd.GeoDataFrame(
            trajectory, crs='EPSG:3034', geometry=geoSeries)
        db_object = create_database_object(trajectory)
        if (db_object == None):
            continue
        sailing_db_objects.append(db_object)
        trajectory["sailing_traj_identifier"] = str(
            db_object["ship_id"]) + str(db_object["time_start_id"]) + "sailing"
        trajectory["stopped_traj_identifier"] = None

    trajectories = {"stopped": stopped_trajectories,
                    "stopped_db_objects": stopped_db_objects,
                    "sailing": sailing_trajectories,
                    "sailing_db_objects": sailing_db_objects}
    trajectories_per_ship[id] = trajectories


def clean_and_reconstruct(config, date_to_lookup):
    connection = connect_to_db(config)

    # Create engine for to_sql method in pandas
    engineString = f"""postgresql://{config["Database"]["dbuser"]}:{config["Database"]["dbpass"]}@{config["Database"]["hostname"]}:5432/{config["Database"]["dbname"]}"""
    engine = create_engine(engineString, executemany_mode='values_plus_batch')

    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    audit_dimension = create_audit_dimension()

    audit_obj = {'timestamp': datetime.now(),
                 'processed_records': 0,
                 'inserted_records': 0,
                 'etl_duration': timedelta(seconds=1),
                 'source_system': config["Audit"]["source_system"],
                 'etl_version': config["Audit"]["elt_version"],
                 'table_name': f"fact_ais_clean",
                 'description': f"Date: {date_to_lookup}"}

    audit_id = audit_dimension.insert(audit_obj)

    cur = connection.cursor()

    DISABLE_TRIGGERS = f"""
        ALTER TABLE fact_ais_clean DISABLE TRIGGER ALL;
    """

    ENABLE_TRIGGERS = f"""
        ALTER TABLE fact_ais_clean ENABLE TRIGGER ALL;
    """

    JUNK_DATA_QUERY = f"""
        SELECT patchedShipRef, isOutlier
        FROM junk_ais_clean
    """

    INITIAL_CLEAN_QUERY = f"""
    SELECT DISTINCT ON (eta_date_id, eta_time_id, ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id,
                    navigational_status_id, cargo_type_id, coordinate, draught, rot, sog, cog, heading)
            fact_ais.type_of_position_fixing_device_id, hour, minute, second, fact_ais.ship_type_id, latitude, longitude, mmsi, fact_ais.type_of_mobile_id, fact_id, eta_date_id, eta_time_id, fact_ais.ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, navigational_status_id, cargo_type_id, coordinate, draught, rot, sog, cog, heading
        FROM fact_ais
        INNER JOIN public.dim_ship on fact_ais.ship_id = dim_ship.ship_id
        INNER JOIN public.dim_time on dim_time.time_id = ts_time_id, public.danish_waters
        WHERE
            ts_date_id = {date_to_lookup}
            AND (draught < 28.5 OR draught IS NULL)
            AND (width < 75)
            AND (length < 488)
            AND mmsi > 99999999
            AND mmsi < 990000000
            AND NOT (mmsi > 111000000 and mmsi < 112000000)
            AND ST_Contains(geom ,coordinate::geometry)
        ORDER BY ship_id, ts_time_id ASC
    """

    FILL_BRIDGE_TABLE_QUERY = f"""
    ALTER TABLE bridge_traj_sailing_cell_3034 DISABLE TRIGGER ALL;
    WITH raster as (
    SELECT ST_AddBand(ST_MakeEmptyRaster({ceil(int(config["Map"]["columns"]))},{ceil(int(config["Map"]["rows"]))},{int(config["Map"]["southwestx"])}::float,{int(config["Map"]["southwesty"])}::float,50::float,50::float,0::float,0::float,3034),
                    '8BUI'::text, 0, null) ras
    ),
    trajectory as(
    SELECT trajectory_id, coordinates
        FROM public.fact_trajectory_sailing
        WHERE date_start_id = {date_to_lookup}
    ), cells as (
    SELECT (((rowy - 1) * {ceil(int(config["Map"]["columns"]))}) + columnx), trajectory_id as cell_id from(
    SELECT trajectory_id, (ST_WorldToRasterCoord(ras,(
                ST_PixelAsPoints(
                    ST_AsRaster(
                        ST_Transform(
                            ST_SetSRID(coordinates, 4326),3034), ras, '8BUI'::text,1,0,true))).geom)).*
    FROM raster, trajectory) foo)
    INSERT INTO bridge_traj_sailing_cell_3034
    SELECT * from cells;
    ALTER TABLE bridge_traj_sailing_cell_3034 ENABLE TRIGGER ALL;"""

    ASSIGN_TRUST_QUERY = f"""
    UPDATE public.fact_trajectory_sailing ft
    SET is_draught_trusted = 
                draught IS NOT NULL 
                AND length is NOT NULL
                AND width IS NOT NULL 
                AND (draught[1] < 3 or ((length * width) / ft.draught[1]) > 60)
	FROM dim_ship s
	WHERE s.ship_id = ft.ship_id AND date_start_id = {date_to_lookup}
    """

    FULL_START_TIME = perf_counter()

    # Disable triggers during load for efficiency
    cur.execute(DISABLE_TRIGGERS)

    # Retrieve junk data and save in memory as dataframe for future use
    junk_data = SQLSource(connection=connection, query=JUNK_DATA_QUERY)
    junk_df = pd.DataFrame(junk_data)
    junk_df.index += 1  # Make index 1-indexed

    # Retrieve the points with initial cleaning rules applied directly to where conditions
    cleaned_data = SQLSource(connection=connection, query=INITIAL_CLEAN_QUERY)

    ais_df = pd.DataFrame(cleaned_data)

    connection.commit()  # Required in order to release locks
    ships_grouped = ais_df.groupby(by=['mmsi'])

    print("Finished data retrieval and grouping")

    dict_updated_ships = {}

    for mmsi, ship_data in ships_grouped:
        if len(ship_data) < 2:  # We need to have at least 2 rows to choose from
            continue

        # Create a sequence and count the number of occurrences for each mobile type recorded
        mobile_type_count = ship_data['type_of_mobile_id'].squeeze(
        ).value_counts()

        # Define variable based on the mobile type that have occurred the most
        best_type = mobile_type_count.reset_index(0)['index'][0]

        # Define the ship_id that have been reported with the previously defined mobile type
        seq_ship_type = ship_data[ship_data['type_of_mobile_id'] == best_type].squeeze()[
            'ship_id']

        if (len(ship_data[ship_data['type_of_mobile_id'] == best_type]) > 1):
            seq_ship_type = seq_ship_type.value_counts()
            best_ship_id = seq_ship_type.reset_index(0)['index'][0]
        else:
            best_ship_id = seq_ship_type

        if (len(mobile_type_count) > 1 or len(seq_ship_type) > 1):
            # Find which indexies needs to be updated

            dict_updated_ships[mmsi] = best_ship_id

    # Assign default value of junk_id to all ships
    cond_patched = junk_df['patchedshipref'] == False
    cond_outlier = junk_df['isoutlier'] == False
    ais_df['junk_id'] = junk_df.index[cond_patched & cond_outlier].tolist()[0]

    # TEMP
    cond_patched_diff = junk_df['patchedshipref'] == True

    # Iterate through all the ships that require an update and update their ship_id for the dataframe
    # TODO-Future: Maybe use dataframe.map(dictionary) instead here?
    for ship in dict_updated_ships:
        shipValue = dict_updated_ships[ship]

        # df.loc[(df['col1'] == 3) & (df['col2'] != 2.0), 'col4'] = "123"

        ais_df.loc[(ais_df['mmsi'] == ship) & (
            ais_df['ship_id'] != shipValue), 'ship_id'] = shipValue
        ais_df.loc[(ais_df['mmsi'] == ship) & (ais_df['ship_id'] != shipValue),
                   'ship_id'] = junk_df.index[(cond_patched_diff) & (cond_outlier)].tolist()[0]
        # ais_df.loc[ais_df['mmsi'] == ship,
        #            'ship_id'] = dict_updated_ships[ship]
        # ais_df.loc[ais_df['mmsi'] == ship,
        #            'junk_id'] = junk_df.index[(cond_patched_diff) & (cond_outlier)].tolist()[0]

    # Function to calculate the correct cell_id given (lat, long)
    def calculateCellID(lat, long, transformer):
        x, y = transformer.transform(lat, long)

        columnx, rowy = ceil((x - int(config["Map"]["southwestx"])) /
                             50), ceil((y - int(config["Map"]["southwesty"])) / 50)
        cell_id = ((rowy - 1) * int(config["Map"]["columns"])) + columnx

        return cell_id

    # Create a new column and apply a function that calculates the cell_id based on row coordinates
    print("Applying cell_id calculations to all rows")
    transformer = Transformer.from_crs(
        "epsg:4326", "epsg:3034")
    ais_df['cell_id'] = ais_df.apply(lambda row: calculateCellID(
        row['latitude'], row['longitude'], transformer), axis=1)
    # ais_df['cell_id'] = 0

    # Remove mmsi column. It was only required during computation
    # TODO: Remove if not necessary
    # del ais_df['mmsi']

    trajectory_df = ais_df.copy().groupby(['ship_id'])
    print(len(trajectory_df))

    del ais_df['mmsi']
    del ais_df['second']
    del ais_df['minute']
    del ais_df['hour']
    del ais_df['ship_type_id']
    del ais_df['type_of_position_fixing_device_id']

    trajectory_sailing_fact_table = create_trajectory_sailing_fact_table()
    trajectory_stopped_fact_table = create_trajectory_stopped_fact_table()
    audit_dimension = create_audit_dimension()

    def insert_trajectory(trajectory_db_object, sailing: bool):
        if(trajectory_db_object == None):
            return 0
        if (sailing):
            trajectory_db_object["audit_id"] = audit_sailing_id
            trajectory_db_object["trajectory_sailing_id"] = trajectory_sailing_fact_table.insert(
                trajectory_db_object)
            trajectory_db_object["trajectory_stopped_id"] = None
        else:
            trajectory_db_object["audit_id"] = audit_stopped_id
            trajectory_db_object["trajectory_stopped_id"] = trajectory_stopped_fact_table.insert(
                trajectory_db_object)
            trajectory_db_object["trajectory_sailing_id"] = None
        return 1

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

    START_TRAJ_TIME = perf_counter()

    processed_records = 0
    inserted_sailing_records = 0
    inserted_stopped_records = 0

    print("Splitting trajectories")

    trajectories_per_ship = mp.Manager().dict()
    with concurrent.futures.ProcessPoolExecutor(initializer=set_global_variables, initargs=(trajectories_per_ship,)) as executor:
        executor.map(traj_splitter_except_catch, trajectory_df)

    print("Inserting trajectories")
    for _, ship in trajectory_df:
        processed_records = processed_records + len(ship)
    for ship in trajectories_per_ship:
        if(len(trajectories_per_ship[ship]["sailing_db_objects"]) > 0):
            for trajectory in trajectories_per_ship[ship]["sailing_db_objects"]:
                inserted_sailing_records += insert_trajectory(trajectory, True)
        if(len(trajectories_per_ship[ship]["stopped_db_objects"]) > 0):
            for trajectory in trajectories_per_ship[ship]["stopped_db_objects"]:
                inserted_stopped_records += insert_trajectory(
                    trajectory, False)

    END_TRAJ_TIME = perf_counter()

    sailing_audit_obj['processed_records'] = processed_records
    sailing_audit_obj['inserted_records'] = inserted_sailing_records
    sailing_audit_obj['etl_duration'] = timedelta(
        seconds=(END_TRAJ_TIME - START_TRAJ_TIME))
    sailing_audit_obj['audit_id'] = audit_sailing_id

    stopped_audit_obj['processed_records'] = processed_records
    stopped_audit_obj['inserted_records'] = inserted_stopped_records
    stopped_audit_obj['etl_duration'] = timedelta(
        seconds=(END_TRAJ_TIME - START_TRAJ_TIME))
    stopped_audit_obj['audit_id'] = audit_stopped_id

    audit_dimension.update(sailing_audit_obj)
    audit_dimension.update(stopped_audit_obj)

    ais_df['audit_id'] = audit_id
    print("AIS_DF to SQL is being called!!")
    print(datetime.today())
    ais_df.to_sql('fact_ais_clean', index=False, con=engine,
                  if_exists='append', chunksize=1000000)
    print(datetime.today())
    print("DONE!!! AIS_DF_TO_SQL HAS BEEN CALLED!!")

    FULL_END_TIME = perf_counter()

    audit_obj['processed_records'] = len(ais_df)
    audit_obj['inserted_records'] = len(ais_df)
    audit_obj['etl_duration'] = timedelta(
        seconds=(FULL_END_TIME - FULL_START_TIME))
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    print("Creating connection commit!!")
    connection.commit()
    cur.execute(ENABLE_TRIGGERS)
    print("Filling bridge table")
    cur.execute(FILL_BRIDGE_TABLE_QUERY)
    cur.execute(ASSIGN_TRUST_QUERY)
    connection.commit()

    print("Creating __dw_conn commit!!")
    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()
    connection.close()
