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
FROM ( SELECT ST_Transform(boundary,4326), max_draught from dim_cell_3034_1000m d inner join
(SELECT max(max_draught) max_draught, cell_id
from fact_cell_3034_1000m
WHERE cell_id in (SELECT cell_id
            FROM   dim_cell_3034_1000m
            WHERE  ST_Intersects(boundary,

                ST_Transform(ST_MakeEnvelope (
                    8.920898437500002, 55.1961157065271, -- bounding
                    12.455749511718752, 56.674338416158825, -- box limits
                    4326), 3034)))
GROUP BY cell_id) foo on foo.cell_id = d.cell_id
     ) as t(geom, draught);
"""
