#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 22:16:16 2020

@author: fitec ABDATA3 groupe 6
"""

# IMPORT LIBRARIES

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import warnings
warnings.filterwarnings('ignore')


# DEFINE PATH FOR CSV FILE
file_path = '/home/fitec/Bureau/Projet_fil_rouge/Data_sources/ETS_Database_v38.csv'

# Creation of organized pandas dataframe
data_origin = pd.read_csv(file_path, sep="\t", quotechar='"', encoding="utf-8")
data_columns=data_origin.columns
data_origin.fillna(0)


# CONNECTION TO CASSANDRA CLUSTER
# Connection to Cassandra cluster in local
CASSANDRA_HOST = ['localhost']
CASSANDRA_PORT = 9042
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)
    #on force ici a repecter le datframe de pandas lors de la recuperation des données
try :
    cluster = Cluster(protocol_version=3, contact_points=CASSANDRA_HOST, load_balancing_policy=None, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session =cluster.connect()
except ValueError :
    print("Oops! échec de connexion au cluster. Try again...")

#creation du key space
session.execute("CREATE KEYSPACE IF NOT EXISTS emissions WITH REPLICATION={'class':'SimpleStrategy','replication_factor':3};")
session.execute("CREATE TABLE IF NOT EXISTS emissions.v38 (country TEXT, country_code TEXT, ets_information TEXT, main_activity_sector_name TEXT, unit TEXT, value FLOAT, year TEXT, primary key (country));")

session = cluster.connect()
session.row_factory = pandas_factory
session.default_fetch_size = 1000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.

# Insertion of dataframe data into Cassandra keyspace
query_insert="INSERT INTO emissions.v38 (country, country_code, ets_information, main_activity_sector_name, unit, value, year) VALUES ($${}$$, '{}', '{}', '{}', '{}', {}, '{}' );"
for ct in data_origin.index:
    CQL_query = query_insert.format(data_origin['country'][ct], data_origin['country_code'][ct],data_origin['ETS information'][ct], data_origin['main activity sector name'][ct], data_origin['unit'][ct], data_origin['value'][ct], data_origin['year'][ct])
    session.execute(CQL_query)

# Closing connection ??




