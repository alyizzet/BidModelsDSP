SELECT * FROM (  
	WITH cpctable AS (
		SELECT SUM(spending)/SUM(click_count) AS avgcpc
			FROM dbo.rtb_wins_analysis AS newTable
		WHERE date BETWEEN current_date-INTERVAL '30 DAYS' AND current_date 
			AND line_item IN (113)
	)
	SELECT	domain as app_id,
			impression,
			total_spent,
			click_count,
			ROUND(cpc,2) as cpc,
			ROUND(avgcpc,2) as avgcpc,
			cpc > avgcpc AS blacklist,
			domain IN (SELECT list_item FROM dbo.rtb_whitelist WHERE list_id = '65') AS in_whitelist
	FROM (
		SELECT	app_bundle as domain, 
				SUM(impression_count) AS impression,
				SUM(spending) AS total_spent, 
				SUM(click_count) AS click_count,
				CASE SUM(click_count) WHEN 0 THEN sum(spending) ELSE sum(spending)/SUM(click_count) END AS cpc,
				(SELECT avgcpc FROM cpctable)
		FROM	"dbo".rtb_wins_analysis
		WHERE	date BETWEEN current_date-INTERVAL '7 DAYS' AND current_date
			AND line_item = 113
		GROUP BY domain
	) AS main1
) AS myTable 
WHERE	--blacklist = TRUE AND
		app_id NOT IN (SELECT list_item FROM dbo.rtb_blacklist WHERE list_id = '66') AND 
		app_id != 'UNKNOWN'
ORDER BY cpc DESC