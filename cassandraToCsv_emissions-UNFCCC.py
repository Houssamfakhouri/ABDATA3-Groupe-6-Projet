#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 22:09:01 2020

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

"""
# Retrieve data from Cassandra: ALL
rowsData = session.execute("SELECT * FROM emissions5.v38;")
data_Cass = rowsData._current_rows
print(data_Cass)
"""

# Retrieve data from Cassandra: Temperatures within europe
#CQL_query = "select year, hadcrut4_mean, hadcrut4_lower, hadcrut4_upper from emissions5.temperature_anomalies WHERE type='European annual' ALLOW FILTERING;"
query_select="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE pollutant_name='{}' AND sector_name ='{}' ALLOW FILTERING;"
CQL_query = query_select.format('All greenhouse gases - (CO2 equivalent)', 'Total (without LULUCF)')
rowsResult = session.execute(CQL_query)
resultDF = rowsResult._current_rows
print(resultDF)

exportDF = pd.DataFrame(resultDF)
exportDF.head(15)
exportDF.to_csv(r'/home/fitec/export_data-emissions-unfccc-01.csv', index = False) # place 'r' before the path name to avoid any errors in the path



