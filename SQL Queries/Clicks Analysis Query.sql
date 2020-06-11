SELECT
geo_region as region,
domain,
sum(win_cost_micros_usd)/1000000 as cost,
sum(clicks + companion_clicks) as clicks, 
count(*) as impressions
from Wins_buffer 
where line_item_id = 98  and region not like '' 
GROUP By region,`domain`
HAVING impressions > 52
order by clicks desc
