WITH raster as (
INSERT INTO test_raster SELECT ST_AddBand(ST_MakeEmptyRaster(22000,16000,0::float,5900000::float,50::float,50::float,0::float,0::float,32632),
				 '8BUI'::text, 0, null) ras
)

UPDATE test_raster
SET ras = ST_SetValue(ras,
(SELECT ARRAY (SELECT columnx_50m::integer x, rowy_50m::integer y, draught from 
dim_cell d inner join 
(SELECT cell_id, max(max_draught) draught, sum(trajectory_count)trajectory_count
	FROM fact_cell --where date_id = 20210602 or date_id = 20210601  --and cell_id in (
		--SELECT cell_id FROM dim_cell where columnx_1000m between 400 and 700 and rowy_1000m between 200 and 550)
	GROUP BY cell_id
	  LIMIT 100) f
	  on f.cell_id = d.cell_id))) foo, raster
	  
CREATE table test_raster(
ras raster)

UPDATE test_raster
SET ras = ST_SetValues(
  ras,
  1,    -- nband
  (
    SELECT ARRAY(
      SELECT (ST_SetSRID(ST_Point(columnx_50m::integer, rowy_50m::integer + 5900000),32632), CASE WHEN draught is null then -1 else draught END)::geomval from 
dim_cell d inner join 
(SELECT cell_id, max(max_draught) draught, sum(trajectory_count)trajectory_count
	FROM fact_cell
	GROUP BY cell_id
	  LIMIT 100) f
	  on f.cell_id = d.cell_id
    )
  )
);



SELECT oid, lowrite(lo_open(oid, 131072), tiff) As num_bytes
FROM
( VALUES (lo_create(0),
ST_Astiff( (SELECT ras FROM test_raster) )
) ) As v(oid,tiff);

SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';