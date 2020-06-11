#!/usr/bin/env python
# coding: utf-8

# In[1]:


from clickhouse_driver import Client
client = Client(host='34.70.65.12', user = "default", password = "", database = "")

import pandas as pd

query = """ 
select sum(clicks+companion_clicks) as clicks,
sum(win_cost_micros)/1000000 as Cost,
model_id 
from Wins_buffer
where line_item_id  = 98 and bid_time > '2020-01-30 00:00:00' 
group by model_id
having clicks > 0
""" 

result, columns = client.execute(query, with_column_types=True)

CPC_Bid_Model = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])

CPC_Bid_Model

CPC_Column = CPC_Bid_Model['Cost'] / CPC_Bid_Model['clicks'] 
AB = pd.concat([CPC_Bid_Model,CPC_Column], axis= 1)

Proper = AB.rename(columns = {0: 'Cost Per Click'})

Proper

Proper.to_html('table.html')

Lift_Rate = (Proper.iloc[0,3] - Proper.iloc[1,3]) /(Proper.iloc[0,3]) * 100

print("The percentage lift rate is " + str((Lift_Rate)))


# In[2]:


Proper


# In[7]:


"""
BID MODEL DETAILS 
"""

query = """select sum(clicks+companion_clicks) as clicks, sum(win_cost_micros) as Cost, model_params from Wins_buffer 
where line_item_id  = 98 and bid_time > now() - INTERVAL 1 day
group by model_params order by Cost desc""" 

result, columns = client.execute(query, with_column_types=True)

Bid_Model_Details = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])

Bid_Model_Detail


# In[ ]:


"""
Bid Model Performance Live Graph
"""

