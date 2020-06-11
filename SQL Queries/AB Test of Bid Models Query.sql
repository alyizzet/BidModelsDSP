select sum(clicks+companion_clicks) as clicks,
sum(win_cost_micros)/1000000 as Cost,
model_id 
from Wins_buffer 
where line_item_id  = 98 and bid_hour > now() - INTERVAL 3 day
group by model_id
having clicks > 0 

