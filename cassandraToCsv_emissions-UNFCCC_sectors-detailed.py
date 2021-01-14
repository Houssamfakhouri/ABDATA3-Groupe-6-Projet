#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 22 22:09:01 2020

@author: fitec ABDATA3 groupe 6
"""


# IMPORT LIBRARIES

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import warnings
warnings.filterwarnings('ignore')

# CONNECTION TO CASSANDRA CLUSTER
# Connection to Cassandra cluster in local
CASSANDRA_HOST = ['localhost']
CASSANDRA_PORT = 9042
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

try :
    cluster = Cluster(protocol_version=3, contact_points=CASSANDRA_HOST, load_balancing_policy=None, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect()
    print('Connection ok')

except ValueError :
    print("Oops! échec de connexion au cluster. Try again...")


# Retrieve data from Cassandra: economic sectors within Europe 27 countries
#CQL_query = "select year, hadcrut4_mean, hadcrut4_lower, hadcrut4_upper from emissions5.temperature_anomalies WHERE type='European annual' ALLOW FILTERING;"
query_select ="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE country='{}' AND pollutant_name ='{}' ALLOW FILTERING;"
CQL_query = query_select.format('EU-27 (2020)', 'All greenhouse gases - (CO2 equivalent)')
rowsResult = session.execute(CQL_query)
resultDF1 = rowsResult._current_rows
exportDF = pd.DataFrame(resultDF1)

# Concatenation of partial dataframes, and export in a global CSV
#exportDF = pd.concat([exportDF1, exportDF2, exportDF3, exportDF4, exportDF5, exportDF6])
exportDF.head(15)
exportDF.to_csv(r'/home/fitec/export_data-emissions-unfccc_sector-details-01.csv', index = False) # place 'r' before the path name to avoid any errors in the path

# FOR TABLEAU: filter YEAR since 1990, remove sectors parent 1-5 and Total(s)

