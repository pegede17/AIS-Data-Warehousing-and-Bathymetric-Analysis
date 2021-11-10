CREATE TABLE IF NOT EXISTS depth_raster (
	ras raster,
	date_id integer,

    FOREIGN KEY (date_id)
    REFERENCES dim_date (date_id)
    ON UPDATE CASCADE
);

WITH template as(
	SELECT (ST_AddBand(ST_MakeEmptyRaster( 0, 0, 0, 0, 100::float, 100::float, 0, 0, 3034),1, '32BF'::text, 0, 0)) as ras
)
INSERT INTO depth_raster(ras, date_id)
	VALUES ((SELECT (ST_AsRaster(ST_Transform(geom,3034),ras, '32BF'::text, value => 0,nodataval=>-1))from danish_waters, template), 0);

INSERT INTO depth_raster (ras, date_id)
	SELECT(ST_AsRaster(ST_Transform(ST_SetSRID(linestring,4326),3034),ras, '32BF'::text, value => draught[1], nodataval=>-1)), date_start_id from fact_trajectory_clean_v6, depth_raster

SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';
SELECT oid, lowrite(lo_open(oid, 131072), png) As num_bytes
 FROM 
 ( VALUES (lo_create(0), 
   ST_AsTIFF((SELECT ST_Union(ST_SetBandNoDataValue(ras,255),'MAX') from depth_raster )
  )) ) As v(oid,png);