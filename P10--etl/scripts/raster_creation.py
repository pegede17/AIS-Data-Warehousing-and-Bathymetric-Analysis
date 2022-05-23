from osgeo import gdal
from osgeo import osr
import numpy as np
from pygrametl.datasources import CSVSource, SQLSource
from sqlalchemy import column
from utils.database_connection import connect_to_db
import configparser

config = configparser.ConfigParser()
config.read('../application.properties')

connection = connect_to_db(config)
print("Starting")

query = """
        SELECT columnx_50m, rowy_50m,CASE WHEN draught is null then -1 else draught END from 
        dim_cell_3034 d inner join 
        (SELECT cell_id , max(max_draught) draught
	    FROM fact_cell_3034_50m
        WHERE is_draught_trusted
 	    GROUP BY cell_id) f
	    on f.cell_id = d.cell_id
        """

sql_source = SQLSource(connection=connection, query=query)

rows = int(config["Map"]["rows"])
columns = int(config["Map"]["columns"])


#  Initialize the Image Size
image_size = (rows, columns)

draughts = np.zeros((image_size), dtype=np.float32)

for entry in sql_source:
    row = int(entry["rowy_50m"])
    column = int(entry["columnx_50m"])
    if (row >= 0 and row < rows and column >= 0 and column < columns):
        draughts[int(entry["rowy_50m"]) - 1, int(
            entry["columnx_50m"]) - 1] = float(entry['draught'])

print("matrix done")

# set geotransform
nx = image_size[0]
ny = image_size[1]
# xmin, ymin, xmax, ymax = [0, 6700000, 1100000, 5900000]
xmin, xmax, ymin, ymax = [int(config["Map"]["southwestx"]), int(config["Map"]["southwestx"]) + (
    columns * 50), int(config["Map"]["southwesty"]) + (rows * 50), int(config["Map"]["southwesty"])]
xres = 50
yres = 50
geotransform = (xmin, xres, 0, ymax, 0, yres)

# create the 3-band raster file
dst_ds = gdal.GetDriverByName('GTiff').Create(
    'AllCells50mOnlyTrust.tif', ny, nx, 1, gdal.GDT_Float32)

dst_ds.SetGeoTransform(geotransform)    # specify coords
srs = osr.SpatialReference()            # establish encoding
srs.ImportFromEPSG(int(config["Map"]["projectionasnumber"]))
dst_ds.SetProjection(srs.ExportToWkt())  # export coords to file
dst_ds.GetRasterBand(1).SetNoDataValue(0)
dst_ds.GetRasterBand(1).WriteArray(draughts)
dst_ds.FlushCache()                     # write to disk
dst_ds = None

print("Done")
