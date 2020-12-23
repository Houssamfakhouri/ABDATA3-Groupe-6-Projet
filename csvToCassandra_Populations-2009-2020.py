#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 22:16:16 2020

@author: fitec ABDATA3 groupe 6
"""

# IMPORT LIBRARIES

import pandas as pd
import numpy as np

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import warnings
warnings.filterwarnings('ignore')


# DEFINE PATH FOR CSV FILE
# https://data.europa.eu/euodp/en/data/dataset/o9i1dRNBDzEuffd0I5m3A
#file_path = '/home/fitec/Bureau/Projet_fil_rouge/Data_sources/tps00001.tsv'
file_path = '/home/fitec/Bureau/Projet_fil_rouge/Data_sources/tps00001.modified4.csv'


# Creation of organized pandas dataframe
data_origin_ini = pd.read_csv(file_path, sep="\t", quotechar='"', encoding="utf-8")
#data_origin.head(20)
  
data_columns=data_origin_ini.columns
data_origin_ini.fillna(0)
 
# displaying the columns after renaming 
print(data_origin_ini)


""" Example to rename a DF column - apply to Populations ""
# defining a dictionary 
d = {"Name": ["John", "Mary", "Helen"], "Marks": [95, 75, 99], "Roll No": [12, 21, 9]} 
# creating the pandas data frame 
df = pd.DataFrame(d) 
# displaying the columns before renaming 
print(df) 
# renaming the column "A" 
df.rename(columns = {"Name": "Names"}, inplace = True) 
# displaying the columns after renaming 
print(df)
"" End of example """

#Rename first column to avoid insertion bugs in Cassandra - NOT WORKING !!
data_origin = data_origin_ini.rename(columns = {"indic_de,geo\time":"indic_geo_time"}) 
print(data_origin)


# Generation of a new column with index number
data_origin['id'] = np.arange(len(data_origin))
data_origin.shape
data_origin.head(20)

# CONNECTION TO CASSANDRA CLUSTER
# Connection to Cassandra cluster in local
CASSANDRA_HOST = ['localhost']
CASSANDRA_PORT = 9042
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

try :
    cluster = Cluster(protocol_version=3, contact_points=CASSANDRA_HOST, load_balancing_policy=None, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session =cluster.connect()
    print("Connection ok")
except ValueError :
    print("Oops! échec de connexion au cluster. Try again...")

#creation du key space
session.execute("CREATE KEYSPACE IF NOT EXISTS emissions5 WITH REPLICATION={'class':'SimpleStrategy','replication_factor':3};")
session.execute("CREATE TABLE IF NOT EXISTS emissions5.populations2009 (geo_time TEXT, pop2009 TEXT, pop2010 TEXT, pop2011 TEXT, pop2012 TEXT, pop2013 TEXT, pop2014 TEXT, pop2015 TEXT, pop2016 TEXT, pop2017 TEXT, pop2018 TEXT, pop2019 TEXT, pop2020 TEXT, id TEXT, primary key (id));")
session = cluster.connect()

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)
    #on force ici a respecter le dataframe de pandas lors de la recuperation des données
    
session.row_factory = pandas_factory
session.default_fetch_size = 1000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.

# Insertion of dataframe data into Cassandra keyspace
query_insert="INSERT INTO emissions5.populations2009 (geo_time, pop2009, pop2010, pop2011, pop2012, pop2013, pop2014, pop2015, pop2016, pop2017, pop2018, pop2019, pop2020, id) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', $${}$$);"
for ct in data_origin.index:
    CQL_query = query_insert.format(data_origin['indic_de_geo_time'][ct], data_origin['pop_2009'][ct],data_origin['pop_2010'][ct], data_origin['pop_2011'][ct], data_origin['pop_2012'][ct], data_origin['pop_2013'][ct], data_origin['pop_2014'][ct], data_origin['pop_2015'][ct], data_origin['pop_2016'][ct], data_origin['pop_2017'][ct], data_origin['pop_2018'][ct], data_origin['pop_2019'][ct], data_origin['pop_2020'][ct], data_origin['id'][ct])
    session.execute(CQL_query)

# Closing connection ??




