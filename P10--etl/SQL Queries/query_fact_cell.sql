SELECT draught, ST_Transform(ST_SetSRID(boundary_1000m, 32632),4326) from (
SELECT max(max_draught) draught, columnx_1000m, rowy_1000m
FROM fact_cell f inner join dim_cell d on d.cell_id = f.cell_id
GROUP BY columnx_1000m, rowy_1000m
LIMIT 10000) d1 inner join dim_cell d2 on d1.columnx_1000m = d2.columnx_1000m and d1.rowy_1000m = d2.rowy_1000m

SELECT max(max_draught), ST_Transform(ST_SetSRID(boundary_1000m, 32632),4326)
FROM fact_cell f inner join dim_cell d on d.cell_id = f.cell_id
GROUP BY ST_Transform(ST_SetSRID(boundary_1000m, 32632),4326)
LIMIT 10
