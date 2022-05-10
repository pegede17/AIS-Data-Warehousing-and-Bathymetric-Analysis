testSelect = """
SELECT * 
FROM dim_time
LIMIT 100
"""

testSelect2 = """
SELECT * 
FROM dim_time
LIMIT 1
"""

boxRaster = """
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
    )
FROM ( SELECT boundary_1000m, max(max_draught)
     from box_11_12_56_57 b
     inner join fact_cell_3034 f
         on b.cell_id = f.cell_id
      WHERE date_id = 20210501
     GROUP BY boundary_1000m
      LIMIT 2
     ) as t(geom, draught);
"""