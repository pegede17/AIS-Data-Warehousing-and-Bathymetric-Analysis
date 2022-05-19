BEGIN;
UPDATE public.fact_trajectory_sailing ft
    SET is_draught_trusted = draught IS NOT NULL AND length is NOT NULL and width IS NOT NULL AND (draught[1] < 3 or ((length * width) / ft.draught[1]) > 60)
	FROM dim_ship s
	WHERE s.ship_id = ft.ship_id and trajectory_id < 1000000

SELECT * from fact_trajectory_sailing where trajectory_id < 40000 limit 1000
    FROM (SELECT ((ship.length * ship.width) / ft.draught[0]) ratio 
          FROM public.fact_trajectory_sailing ft INNER JOIN public.dim_ship ship ON ft.ship_id = ship.ship_id) foo
    WHERE (ft.draught[0] < 3 OR ratio > 60) AND ft.trajectory_id < 40000
ROLLBACK;
COMMIT;
SELECT draught IS NOT NULL AND length is NOT NULL and width IS NOT NULL AND (draught[1] < 3 or ((length * width) / ft.draught[1]) > 60)
FROM dim_ship s inner join fact_trajectory_sailing ft on ft.ship_id = s.ship_id
WHERE trajectory_id < 40000

SELECT coordinates::geography, draught, trajectory_id from fact_trajectory_sailing where not is_draught_trusted and draught[1] < 5 limit 1000