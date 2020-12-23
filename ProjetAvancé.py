#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import datetime


# In[2]:


from pyspark.sql.types import LongType
from pyspark.sql.types import StringType


# In[3]:


spark = SparkSession.builder   .appName('SparkCassandraApp')   .config('spark.cassandra.connection.host', '51.79.101.40')   .config('spark.cassandra.connection.port', '9042')   .config("spark.cassandra.auth.username","cassandra")  .config("spark.cassandra.auth.password","cassandra")  .master('local[2]')   .getOrCreate()


# In[4]:


dataset = "file:/home/fitec/Documents/Project.csv"


# In[5]:


pd_dataset = pd.read_csv(dataset, header = 0)


# In[6]:


dataset = pd_dataset.drop(['Notation','PublicationDate','DataSource'], axis = 1)


# In[7]:


dataset = pd_dataset.drop(['Country_code','Format_name'], axis = 1)


# In[8]:


melt_projet = pd.pivot_table(dataset,index = ['Country','Year'] , columns = 'Sector_name', values ='emissions').reset_index()
melt_projet.columns.name = None


# In[9]:


melt_projet.dtypes


# In[10]:


melt_projet['Year']= pd.to_datetime(melt_projet['Year'].astype(str), errors = 'coerce')


# melt_projet

# In[11]:


dataset = melt_projet[['Country','Year','1 - Energy','2 - Industrial Processes and Product Use','3 - Agriculture','4 - Land Use, Land-Use Change and Forestry','5 - Waste management','6 - Other Sector','Total (with LULUCF)','ind_CO2 - Indirect CO2']]


# In[12]:


dataset.dtypes


# 

# In[13]:


dataset1=dataset.rename(columns={'1 - Energy':'Energy','2 - Industrial Processes and Product Use':'Industrial_Processes_and_Product_Use','3 - Agriculture':'Agriculture','4 - Land Use, Land-Use Change and Forestry':'LULUCF','5 - Waste management':'Waste_management','6 - Other Sector':'Other_Sector'})
dataset1


# In[ ]:





# In[14]:


dataset1.mean()


# In[ ]:





# In[ ]:





# In[ ]:





# In[15]:


dataset1.describe()


# In[16]:


dataset1.to_csv('/home/fitec/Documents/data.csv', index = False, header = True)


# In[17]:


get_ipython().system('pip install PyArrow')


# In[18]:


spark.conf.set("spark.sql.execution.arrow.enabled", True)
df_dataset = spark.createDataFrame(dataset1)
df_dataset.show()


# In[19]:


df_dataset.select('Year').show()


# In[20]:


df_dataset.filter(col("Year") >= "1990-01-01").show()


# In[21]:


dataset2 = (df_dataset.filter(col("Year") >= "1990-01-01")).toPandas()
dataset2


# In[22]:


dset = dataset2.fillna(0)
dset


# In[24]:


dset.to_csv('/home/fitec/Documents/data.csv', index = False, header = True)


# In[ ]:




