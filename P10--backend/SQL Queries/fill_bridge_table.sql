ALTER TABLE bridge_traj_sailing_cell DISABLE TRIGGER ALL;
WITH raster as (
SELECT ST_AddBand(ST_MakeEmptyRaster(22000,16000,0::float,5900000::float,50::float,50::float,0::float,0::float,32632),
				 '8BUI'::text, 0, null) ras
),
trajectory as(
SELECT trajectory_id, coordinates
	FROM public.fact_trajectory_sailing
	WHERE date_start_id = 20210517
), cells as (

SELECT (((rowy - 1) * 22000) + columnx), trajectory_id as cell_id from(
SELECT trajectory_id, (ST_WorldToRasterCoord(ras,(
			ST_PixelAsPoints(
				ST_AsRaster(
					ST_Transform(
						ST_SetSRID(coordinates, 4326),32632), ras, '8BUI'::text,1,0,true))).geom)).*
FROM raster, trajectory) foo)
INSERT INTO bridge_traj_sailing_cell 
SELECT * from cells;
ALTER TABLE bridge_traj_sailing_cell ENABLE TRIGGER ALL;