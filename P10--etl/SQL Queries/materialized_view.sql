CREATE MATERIALIZED VIEW fact_cell_3034_1000m_month_05
as
SELECT 	05 as month,
        cell_id, 
        ship_type_id, 
        type_of_mobile_id,
        is_draught_trusted,
        sum(trajectory_count), 
        MAX(max_draught) max_draught,
        MIN(min_draught) min_draught,
        SUM(avg_draught * trajectory_count)/ 
                    CASE WHEN SUM(CASE WHEN avg_draught IS NOT NULL THEN trajectory_count ELSE 0 END) > 0 
                        THEN SUM(CASE WHEN avg_draught IS NOT NULL THEN trajectory_count ELSE 0 END) 
                        ELSE 1 END avg_draught,
        MAX (max_traj_speed) max_traj_speed,
        MIN (min_traj_speed) min_traj_speed,
        SUM(avg_traj_speed * trajectory_count)/ 
                    CASE WHEN SUM(CASE WHEN avg_traj_speed IS NOT NULL THEN trajectory_count ELSE 0 END) > 0 
                        THEN SUM(CASE WHEN avg_traj_speed IS NOT NULL THEN trajectory_count ELSE 0 END) 
                        ELSE 1 END avg_traj_speed,
		(WITH histos as (
		 SELECT histogram_draught,
			histogram_traj_speed
		 	FROM fact_cell_3034_1000m f1
		 	WHERE f1.cell_id = f2.cell_id
			AND f1.ship_type_id = f2.ship_type_id
			AND f1.type_of_mobile_id = f2.type_of_mobile_id
			AND f1.is_draught_trusted = f2.is_draught_trusted
			AND f1.date_id between 20210501 and 20210531
		 )
		 SELECT 
		 ARRAY (
          SELECT 
            sum(elem) 
          FROM
			histos t,
            unnest(t.histogram_draught) WITH ORDINALITY x(elem, rn)
          GROUP BY 
            rn 
          ORDER BY 
            rn
        )) histogram_draught,
		(WITH histos as (
		 SELECT histogram_draught,
			histogram_traj_speed
		 	FROM fact_cell_3034_1000m f1
		 	WHERE f1.cell_id = f2.cell_id
			AND f1.ship_type_id = f2.ship_type_id
			AND f1.type_of_mobile_id = f2.type_of_mobile_id
			AND f1.is_draught_trusted = f2.is_draught_trusted
			AND f1.date_id between 20210501 and 20210531
		 )
		 SELECT ARRAY (
          SELECT 
            sum(elem) 
          FROM
			histos t,
            unnest(t.histogram_traj_speed) WITH ORDINALITY x(elem, rn)
          GROUP BY 
            rn 
          ORDER BY 
            rn
        ) histogram_traj_speed)
from fact_cell_3034_1000m f2
WHERE date_id between 20210501 and 20210531
GROUP BY cell_id,
        ship_type_id, 
        type_of_mobile_id,
        is_draught_trusted;