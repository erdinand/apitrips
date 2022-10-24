--From the two most commonly appearing regions, which is the latest datasource?
WITH rank_regions AS (
	SELECT
		  t.region
		, RANK() OVER (ORDER BY count(1) DESC) as pos
	FROM 
		trips t 
	GROUP BY
		t.region 
), 
rank_region_dt AS (
	SELECT
		  t.region
		, t.date
		, t.time
		, RANK() OVER (
			PARTITION BY t.region
			ORDER BY t.date DESC, t.time DESC
		) as pos
		, t.datasource 
	FROM 
		trips t 
	GROUP BY
		  t.region
		, t.date
		, t.time
		, t.datasource 
)
SELECT 
	  rank_regions.region
	, rank_region_dt.datasource as latest_datasource
FROM 
	rank_regions
	INNER JOIN rank_region_dt ON rank_regions.region = rank_region_dt.region
WHERE 
	rank_regions.pos <= 2
	AND rank_region_dt.pos = 1
ORDER BY
	rank_regions.pos
;


--What regions has the "cheap_mobile" datasource appeared in
SELECT 
	t.region
FROM
	trips t 
WHERE
	datasource = 'cheap_mobile'
GROUP BY 
	t.region 
;
