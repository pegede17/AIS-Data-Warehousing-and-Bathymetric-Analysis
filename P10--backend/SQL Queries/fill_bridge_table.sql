ALTER TABLE bridge_traj_sailing_cell_3034 DISABLE TRIGGER ALL;
WITH raster as (
SELECT ST_AddBand(ST_MakeEmptyRaster(15798,8324,3602375::float,3055475::float,50::float,50::float,0::float,0::float,3034),
				 '8BUI'::text, 0, null) ras
),
trajectory as(
SELECT trajectory_id, coordinates
	FROM public.fact_trajectory_sailing
), cells as (

SELECT (((rowy - 1) * 15798) + columnx), trajectory_id as geo_id from(
SELECT trajectory_id, (ST_WorldToRasterCoord(ras,(
			ST_PixelAsPoints(
				ST_AsRaster(
					ST_Transform(
						ST_SetSRID(coordinates, 4326),3034), ras, '8BUI'::text,1,0,true))).geom)).*
FROM raster, trajectory) foo)
INSERT INTO bridge_traj_sailing_cell_3034 
SELECT * from cells;
ALTER TABLE bridge_traj_sailing_cell_3034 ENABLE TRIGGER ALL;