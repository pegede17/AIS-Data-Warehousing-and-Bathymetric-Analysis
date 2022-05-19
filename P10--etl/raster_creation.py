from osgeo import gdal
from osgeo import osr
import numpy as np
import os
import sys
from pygrametl.datasources import CSVSource, SQLSource
from database_connection import connect_to_local, connect_via_ssh

connection = connect_to_local()
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

#  Initialize the Image Size
image_size = (8324, 15798)

draughts = np.zeros((image_size), dtype=np.float32)


# for row in csv_source:
#     draughts[int(row["rowy_50m"]), int(
#         row["columnx_50m"])] = float(row['draught']) + 200


for row in sql_source:
    draughts[int(row["rowy_50m"]), int(
        row["columnx_50m"])] = float(row['draught'])

print("matrix done")

#  Create Each Channel

# set geotransform
nx = image_size[0]
ny = image_size[1]
# xmin, ymin, xmax, ymax = [0, 6700000, 1100000, 5900000]
xmin, ymin, xmax, ymax = [3602375, 3471675, 4392275, 3055475]
xres = 50
yres = 50
geotransform = (xmin, xres, 0, ymax, 0, yres)

# create the 3-band raster file
dst_ds = gdal.GetDriverByName('GTiff').Create(
    'AllCells50mOnlyTrust.tif', ny, nx, 1, gdal.GDT_Float32)

dst_ds.SetGeoTransform(geotransform)    # specify coords
srs = osr.SpatialReference()            # establish encoding
srs.ImportFromEPSG(3034)                # WGS84 lat/long
dst_ds.SetProjection(srs.ExportToWkt())  # export coords to file
dst_ds.GetRasterBand(1).SetNoDataValue(0)
dst_ds.GetRasterBand(1).WriteArray(draughts)
# dst_ds.GetRasterBand(1).WriteArray(r_pixels)   # write r-band to the raster
# dst_ds.GetRasterBand(2).WriteArray(g_pixels)   # write g-band to the raster
# dst_ds.GetRasterBand(3).WriteArray(b_pixels)   # write b-band to the raster
dst_ds.FlushCache()                     # write to disk
dst_ds = None

print("Done")
