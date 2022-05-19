SELECT json_build_object(
    'type', 'Feature',
    'geometry', ST_AsGeoJSON(geom)::jsonb,
    'properties', json_build_object(
        'count', traj_count, 
        'max', draught
    ))
FROM (SELECT ST_Transform(ST_SetSRID(boundary_1000m, 32632),4326) geom, MAX(draught) draught, sum(trajectory_count) traj_count 
	  from dim_cell inner join(
	SELECT cell_id cell, max(max_draught) draught, sum(trajectory_count)trajectory_count
	FROM fact_cell where date_id = 20210602  --and cell_id in (
		--SELECT cell_id FROM dim_cell where columnx_1000m between 400 and 700 and rowy_1000m between 200 and 550)
	GROUP BY cell_id) foo
on cell = cell_id
-- 	  where columnx_1000m between 500 and 700 and rowy_1000m between 400 and 550
GROUP BY boundary_1000m) foo

