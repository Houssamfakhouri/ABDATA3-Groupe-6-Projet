#!/usr/bin/env python
# coding: utf-8

# In[45]:


import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession


# In[46]:


spark = SparkSession.builder   .appName('SparkCassandraApp')   .config('spark.cassandra.connection.host', '51.79.101.40')   .config('spark.cassandra.connection.port', '9042')   .config("spark.cassandra.auth.username","cassandra")  .config("spark.cassandra.auth.password","cassandra")  .master('local[2]')   .getOrCreate()


# In[47]:


dataset = "file:/home/fitec/Documents/projet.csv"


# In[48]:


pd_dataset = pd.read_csv(dataset, header = 0)
pd_dataset


# In[38]:


melt_projet = pd.pivot(pd_dataset,index = 'Année' , columns = 'Valeur')['Capacité de consommation'].reset_index()
melt_projet.columns.name = None 
melt_projet


# In[50]:


df_dataset = spark.createDataFrame(melt_projet)
df_dataset.show()


# In[51]:


df_test = df_dataset.select(col('(Memo items) International Aviation').alias('InterAvia'))
df_test.show()


# In[ ]:




