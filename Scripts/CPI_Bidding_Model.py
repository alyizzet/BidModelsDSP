#!/usr/bin/env python
# coding: utf-8

# In[6]:


# Database Connection  - Read Logs 
import pandas as pd
from clickhouse_driver import Client
client = Client(host='34.70.65.12', user = "default", password = "", database = "")


# In[17]:


# This query looks at all the install > 0 app + regions in the last 30 days and if CPI < 0.5 bids 1.3, ifelse 0.5 and 1 bids 1 else bids 0. 
rolling_window_programmatic_query ="""
SELECT
	app_id,
	geo_region,
	count() as installs,
	toFloat32(sum(win_cost_micros))/ 1000000 as cost,
	toFloat32(cost) / toFloat32(installs) as cpi,
	--today() - toDate(bid_time) as days_in_list,
	bid_hour 
FROM
	Wins_buffer any
left join (
	select
		conversion_value,
		auction_id,
		event_id
	from
		AttrConversion_buffer)
		using (auction_id)
where
	line_item_id in (27,21)
	and event_id like '1' and bid_hour > now() - interval 30 day
group by
	app_id,
	geo_region,
	event_id,
	bid_hour 
--	days_in_list
order by
	cpi desc
"""

result, columns = client.execute(rolling_window_programmatic_query, with_column_types=True)

HH_programmatic = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])


# In[19]:


# This query will look at all the non install bringing app + regions for ALL the times and ban them after they spend 0.5 dollars
Absolute_Blacklist_query = """ 
select
	app_id,
	geo_region,
	toFloat64(sum(win_cost_micros_usd)/1000000) as cost
from
	Wins_buffer
where
	line_item_id in (27,21)
	and conversions = 0 
group by
	app_id,
	geo_region
having cost > 0.5
"""

result, columns = client.execute(Absolute_Blacklist_query, with_column_types=True)
Blacklist = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])

Blacklist['value'] = 0 
Absolute_Blacklist= Blacklist.drop(columns=['cost'])


# In[20]:


programmatic = HH_programmatic.drop(columns=['cost','installs','bid_hour'])
a = programmatic.query('cpi < 0.5')
b = programmatic.query('cpi < 1 and cpi > 0.5')
c = programmatic.query('cpi > 1 and cpi < 1.5')
d = programmatic.query('cpi > 1.5 and cpi < 2')
e = programmatic.query('cpi > 2')



a['value'] = 1.5
b['value'] = 1.25
c['value'] = 0.7 
d['value'] = 0.35 
e['value'] = 0 

predict_pre = pd.concat([a,b,c,d,e,Absolute_Blacklist])

predict_pre


# In[21]:


predict = predict_pre.drop(columns = ['cpi']) 
predict.to_csv("HH27,21_predict", sep ='|',encoding='utf-8', index= False )


# In[22]:


features = list(predict.drop(['value'], axis=1).columns)

S3_predictions_path = 's3://beeswax-data-us-east-1/bid_models/improvedigital/customer_data/HH27,21_predict.csv'

manifest = {
    'model_predictions': [   
        S3_predictions_path  
    ],  
    'metadata': {
        'fields': features
    }
}

import json

def manifest_file(i):
    json_name = i + '_manifest.json'
    with open( json_name , 'w') as f:
        json.dump(manifest, f)
        
manifest_file('HH27,21')  


# In[25]:


import boto3

client = boto3.client('s3', 
                      region_name='us-east-1')

client.upload_file(r'C:\Users\izzet.metin\OrionLead Bid Models\HH27,21_predict',
                   'beeswax-data-us-east-1',
                   'bid_models/improvedigital/customer_data/HH27,21_predict.csv', ExtraArgs = {'ACL': 'bucket-owner-full-control'})


# In[24]:


client.upload_file(r'C:\Users\izzet.metin\OrionLead Bid Models\HH27,21_manifest.json',
                   'beeswax-data-us-east-1',
                   'bid_models/improvedigital/customer_manifests/HH27,21_manifest.json', ExtraArgs = {'ACL': 'bucket-owner-full-control'})

