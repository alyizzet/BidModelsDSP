#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Database Connection  - Read Logs 
import pandas as pd
from clickhouse_driver import Client
client = Client(host='34.70.65.12', user = "default", password = "", database = "")

dogshit_query = """
SELECT
    geo_region,
    domain,
    toFloat64(sum(win_cost_micros_usd))/ 1000000 as cost,
    sum(clicks + companion_clicks) as clicks,
    count() as impressions,
    cost as CPC,
    today() - toDate(min(bid_time)) AS day_in_list
from
    Wins_buffer
where
    line_item_id in (98)
GROUP By
    geo_region,
    `domain`
having 
clicks = 0 and CPC > 0.3
order by
    CPC desc
"""


result, columns = client.execute(dogshit_query, with_column_types=True)

voidu_dogshit = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])

voidu_dogshit['value'] = 0.01 
voidu_dogshit.drop(columns=['cost','clicks','impressions','CPC','day_in_list'])

programmatic_query = """ 
SELECT
	geo_region,
	domain,
	toFloat64(sum(win_cost_micros_usd))/ 1000000 as cost,
	sum(clicks + companion_clicks) as clicks,
	count() as impressions,
	multiIf(clicks = 0, cost, cost / clicks) as CPC,
	today() - toDate(min(bid_time)) AS day_in_list
from
	Wins_buffer
where
	line_item_id in (98)
GROUP By
	geo_region,
	`domain`
having 
clicks > 0
and (impressions > 600 or day_in_list > 3)
order by
	CPC asc
"""


import pandas as pd
result, columns = client.execute(programmatic_query, with_column_types=True)
programmatic = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])


programmatic.drop(columns=['cost','clicks','impressions','CPC','day_in_list'])

a = programmatic.query('CPC < 0.5')
b = programmatic.query('CPC < 1 and CPC > 0.5')
c = programmatic.query('CPC > 1 and CPC < 1.5')
d = programmatic.query('CPC > 1.5 and CPC < 2')
e = programmatic.query('CPC > 2')


a['value'] = 1.2
b['value'] = 1.1
c['value'] = 0.7 
d['value'] = 0.35 
e['value'] = 0.01 

# Higher_part = programmatic.query('CPC < 0.48')
# Lower_part = programmatic.query('CPC > 0.48 and CPC < 2')
# Dogshit_part = programmatic.query('CPC > 2')

# Higher_part['value'] = 1.3
# Dogshit_part['value'] = 0
# Lower_part['value'] = 0.7

predict_pre = pd.concat([a,b,c,d,e,voidu_dogshit])#

predict_pre

predict = predict_pre.drop(columns = ['cost','clicks','impressions','CPC','day_in_list'] ) 
predict.to_csv("Voidu98_predict", sep ='|',encoding='utf-8', index= False )


# In[ ]:


import boto3

client = boto3.client('s3', 
                      region_name='us-east-1')

client.upload_file(r'C:\Users\izzet.metin\OrionLead Bid Models\Voidu98_predict',
                   'beeswax-data-us-east-1',
                   'bid_models/improvedigital/customer_data/Voidu98_predict.csv', ExtraArgs = {'ACL': 'bucket-owner-full-control'})

