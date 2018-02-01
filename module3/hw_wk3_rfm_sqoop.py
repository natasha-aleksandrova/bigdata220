
# coding: utf-8

# # Sqoop Command
# 
# command:
# sqoop --options-file /root/sq_options.txt
# 
# 
# contents of sq_options.txt:
# 

# In[ ]:


import

--connect
jdbc:sqlserver://bigdata220w18.database.windows.net:1433;database=week3

--username
bigdata

--password
5Zgv\6;8rM

--compress

--compression-codec
snappy 

--as-parquetfile 

--target-dir 
sqoop(/wk3_hw_denormal_data)

--num-mappers
1

--query
select 	transaction_data.household_key, 	homeowner_desc, 	day, 	basket_id, 	sales_value \  
from hh_demographic join transaction_data 	on transaction_data.household_key = hh_demographic.household_key join product 	on product.product_id = transaction_data.product_id WHERE $CONDITIONS


# # Load Parquet File

# In[1]:


df = sqlContext.read.parquet("hdfs://sandbox.hortonworks.com:8020/sqoop/wk3_hw_denormal_data/*")
df.show()


# # Run Queries
# 
# ## All RFM Metrics
# 

# In[7]:


df.registerTempTable("household_transaction_product")

sql_stmt = """
select 
  household_key, 
  max(day) as recency,
  count(distinct(basket_id)) as frequency,
  round(sum(sales_value), 2) as monetary
from household_transaction_product
group by household_key
order by recency desc, frequency desc, monetary desc
"""

rfm_metric_df = sqlContext.sql(sql_stmt)
rfm_metric_df.show(20)

rfm_metric_df.write.csv('hdfs://sandbox.hortonworks.com:8020/course2/rfm_metrics.csv')


# ## Homeowners RFM Metrics

# In[8]:


sql_stmt = """
select 
  household_key, 
  max(day) as recency,
  count(distinct(basket_id)) as frequency,
  round(sum(sales_value), 2) as monetary
from household_transaction_product
where lower(homeowner_desc) = 'homeowner'
group by household_key
order by recency desc, frequency desc, monetary desc
"""

rfm_metric_df = sqlContext.sql(sql_stmt)
rfm_metric_df.show(20)

rfm_metric_df.write.csv('hdfs://sandbox.hortonworks.com:8020/course2/rfm_metrics_homeowners.csv')

