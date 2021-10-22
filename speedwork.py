import geopandas as gpd
import movingpandas as mpd
from shapely.geometry import Point
from datetime import datetime, timedelta
from sktime.transformations.series.outlier_detection import HampelFilter
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Library configurations
pd.set_option('display.max_columns', 50)
np.set_printoptions('threshold', 2000)
np.random.seed(0)

data  = pd.read_csv("aisdk_20210729.csv")

df = pd.DataFrame(data)
print(list(df.columns))

data_trajectories = df[['MMSI','# Timestamp', 'Latitude', 'Longitude', 'SOG']]
data_trajectories['t'] = pd.to_datetime(data_trajectories['# Timestamp'])
data_trajectories = data_trajectories.set_index('t')

gdf = gpd.GeoDataFrame(data_trajectories.drop(['# Timestamp', 'Latitude', 'Longitude'], axis=1), crs="EPSG:4326", geometry=gpd.points_from_xy(data_trajectories.Longitude, data_trajectories.Latitude))
gdf_grouped = gdf.groupby(by=['MMSI'])
print("Finished grouping")

# Function that takes a trajectory collection and creates a filtered set of trajectories
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

## Generate a trajectory for each ship
trajectories_per_ship = {}
filtered_points = 4
hampel_filter = HampelFilter(window_length=filtered_points)

i = 1
total = len(gdf_grouped.groups)
print(datetime.now())

for mmsi, data in gdf_grouped:
    if (len(str(mmsi)) < 9):
        i = i + 1
        continue
    if (len(data) < 2):
        i = i + 1
        continue
    if ((data.geometry.x > 180).any() or (data.geometry.x < -180).any()):
        i = i + 1
        continue
    if ((data.geometry.y > 90).any() or (data.geometry.y < -90).any()):
        i = i + 1
        continue
    
    print("--------")
    print("("+str(mmsi)+"):" + str(i) + ' ud af ' + str(total))

    time_now = datetime.now()
    speed_limit = 0.5
    data['speed'] = data['SOG']
    trajectory = mpd.Trajectory(data, mmsi)
    time_next = (time_now - datetime.now()).total_seconds()
    print("Trajectory creation: " + str(time_next))

    if not (trajectory.is_valid()):
        i = i + 1
        continue

    ## Define and split trajectories based on idle duration
    time_now = datetime.now()
    stops = mpd.SpeedSplitter(trajectory).split(duration=timedelta(minutes=5), speed=speed_limit)
    time_next = (time_now - datetime.now()).total_seconds()
    print("Stop creation: " + str(time_next))
    print("Stops: " + str(len(stops)))

    ## Simplify trajectories using douglas peucker algorithm
    time_now = datetime.now()
    traj_simplified = mpd.DouglasPeuckerGeneralizer(stops).generalize(tolerance=0.0001)
    time_next = (time_now - datetime.now()).total_seconds()
    print("Simplifying: " + str(time_next))

    ## Apply Hampel filter on trajectories
    time_now = datetime.now()
    filtered_trajectories = apply_filter_on_trajectories(traj_simplified, hampel_filter, filtered_points)
    time_next = (time_now - datetime.now()).total_seconds()
    print("Filtering: " + str(time_next))

    trajectories_per_ship[mmsi] = filtered_trajectories
    i = i + 1
    print("--------")

print(datetime.now())