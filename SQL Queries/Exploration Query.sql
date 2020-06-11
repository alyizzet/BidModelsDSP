select
	sum(clicks + companion_clicks) as clicks,
	sum(win_cost_micros) as cost,
	model_params,
	auction_id,
	geo_region,
	app_id,
	app_bundle,
	`domain`,
	*
from
	Wins_buffer
where
	line_item_id = 98
	and bid_time > '2020-01-30 00:00:00' 
	and model_params like ''
group by *
having
	clicks > 0
order by
	cost desc
