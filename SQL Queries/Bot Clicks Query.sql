SELECT sum(bot_clicks),
`domain`,
win_cost_micros_usd/1000000 as cost
from Wins_buffer 
where `domain` not like '' and cost > 0 
group by `domain`,bot_clicks,win_cost_micros_usd
order by sum(bot_clicks) desc