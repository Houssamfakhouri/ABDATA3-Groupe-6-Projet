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
file_path = '/home/fitec/Bureau/Projet_fil_rouge/Data_sources/global-average-air-temperature-anomalies-6.csv'

# Creation of organized pandas dataframe
data_origin = pd.read_csv(file_path, sep=",", quotechar='"', encoding="utf-8")
data_columns=data_origin.columns
data_origin.fillna(0)

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
session.execute("CREATE TABLE IF NOT EXISTS emissions5.temperature_anomalies (Year TEXT, HadCRUT4_mean TEXT, HadCRUT4_upper TEXT, HadCRUT4_lower TEXT, ERA_Interim TEXT, GISTEMP_v4 TEXT, NOAA_GlobalTemp_v5 TEXT, Type TEXT, id TEXT, primary key (id));")
session = cluster.connect()

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)
    #on force ici a respecter le dataframe de pandas lors de la recuperation des données
    
session.row_factory = pandas_factory
session.default_fetch_size = 1000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.

# Insertion of dataframe data into Cassandra keyspace
query_insert="INSERT INTO emissions5.temperature_anomalies (Year, HadCRUT4_mean, HadCRUT4_upper, HadCRUT4_lower, ERA_Interim, GISTEMP_v4, NOAA_GlobalTemp_v5, Type, id) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', $${}$$);"
for ct in data_origin.index:
    CQL_query = query_insert.format(data_origin['Year:number'][ct], data_origin['HadCRUT4 (mean):number'][ct],data_origin['HadCRUT4 (upper):number'][ct], data_origin['HadCRUT4 (lower):number'][ct], data_origin['ERA-Interim:number'][ct], data_origin['GISTEMP v4:number'][ct], data_origin['NOAA GlobalTemp v5:number'][ct], data_origin['Type:text'][ct], data_origin['id'][ct])
    session.execute(CQL_query)

# Closing connection ??




