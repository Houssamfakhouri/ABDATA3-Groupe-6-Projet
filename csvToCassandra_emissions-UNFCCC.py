#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 14 22:16:16 2020

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
file_path = '/home/fitec/Bureau/Projet_fil_rouge/Data_sources/UNFCCC_v23.csv'

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
session.execute("CREATE TABLE IF NOT EXISTS emissions5.emissions_unfccc (country_code TEXT, country TEXT, format_name TEXT, pollutant_name TEXT, sector_code TEXT, sector_name TEXT, parent_sector_code TEXT, unit TEXT, year TEXT, emissions TEXT, notation TEXT, publicationDate TEXT, dataSource TEXT, id TEXT, primary key (id));")
session = cluster.connect()

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)
    #on force ici a respecter le dataframe de pandas lors de la recuperation des données
    
session.row_factory = pandas_factory
session.default_fetch_size = 1000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.

# Insertion of dataframe data into Cassandra keyspace
query_insert="INSERT INTO emissions5.emissions_unfccc (country_code, country, format_name, pollutant_name, sector_code, sector_name, parent_sector_code, unit, year, emissions, notation, publicationDate, dataSource, id) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', $${}$$);"
for ct in data_origin.index:
    CQL_query = query_insert.format(data_origin['Country_code'][ct], data_origin['Country'][ct],data_origin['Format_name'][ct], data_origin['Pollutant_name'][ct], data_origin['Sector_code'][ct], data_origin['Sector_name'][ct], data_origin['Parent_sector_code'][ct], data_origin['Unit'][ct], data_origin['Year'][ct], data_origin['emissions'][ct],data_origin['Notation'][ct], data_origin['PublicationDate'][ct], data_origin['DataSource'][ct], data_origin['id'][ct])
    session.execute(CQL_query)


# Closing connection ??



