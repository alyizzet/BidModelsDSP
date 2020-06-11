SELECT
	geo_region,
	domain,
	toFloat64(sum(win_cost_micros_usd))/ 1000000 as cost,
	sum(clicks + companion_clicks) as clicks,
	count() as impressions,
	(
	SELECT
		count(*)/ uniqExact(geo_region)
	from
		Wins_buffer
	where
		line_item_id = 98
		and bid_time < now() - INTERVAL 15 day
		and geo_region not like '') AS avg_impression,
	multiIf(clicks = 0, cost, cost / clicks) as CPC,
	today() - toDate(min(bid_time)) AS day_in_list
from
	Wins_buffer
where
	line_item_id = 98
	and geo_region not like ''
GROUP By
	geo_region,
	`domain`
having 
clicks > 0
and (impressions > 600 or day_in_list > 3)
order by
	CPC asc
