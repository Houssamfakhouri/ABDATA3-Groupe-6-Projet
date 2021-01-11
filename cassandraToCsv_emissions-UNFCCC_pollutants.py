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

# Retrieve data from Cassandra: pollutant names within Europe 27 countries
#CQL_query = "select year, hadcrut4_mean, hadcrut4_lower, hadcrut4_upper from emissions5.temperature_anomalies WHERE type='European annual' ALLOW FILTERING;"
query_select ="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE country='{}' AND sector_name  = '{}' AND pollutant_name = '{}' ALLOW FILTERING;"
CQL_query = query_select.format('EU-27 (2020)', 'Total (without LULUCF)', 'CH4')
rowsResult = session.execute(CQL_query)
resultDF1 = rowsResult._current_rows
exportDF1 = pd.DataFrame(resultDF1)

query_select ="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE country='{}' AND sector_name  = '{}' AND pollutant_name = '{}' ALLOW FILTERING;"
CQL_query = query_select.format('EU-27 (2020)', 'Total (without LULUCF)', 'CO2')
rowsResult = session.execute(CQL_query)
resultDF2 = rowsResult._current_rows
exportDF2 = pd.DataFrame(resultDF2)

query_select ="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE country='{}' AND sector_name  = '{}' AND pollutant_name = '{}' ALLOW FILTERING;"
CQL_query = query_select.format('EU-27 (2020)', 'Total (without LULUCF)', 'HFCs - (CO2 equivalent)')
rowsResult = session.execute(CQL_query)
resultDF3 = rowsResult._current_rows
exportDF3 = pd.DataFrame(resultDF3)

query_select ="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE country='{}' AND sector_name  = '{}' AND pollutant_name = '{}' ALLOW FILTERING;"
CQL_query = query_select.format('EU-27 (2020)', 'Total (without LULUCF)', 'N2O')
rowsResult = session.execute(CQL_query)
resultDF4 = rowsResult._current_rows
exportDF4 = pd.DataFrame(resultDF4)

query_select ="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE country='{}' AND sector_name  = '{}' AND pollutant_name = '{}' ALLOW FILTERING;"
CQL_query = query_select.format('EU-27 (2020)', 'Total (without LULUCF)', 'NF3 - (CO2 equivalent)')
rowsResult = session.execute(CQL_query)
resultDF5 = rowsResult._current_rows
exportDF5 = pd.DataFrame(resultDF5)

query_select ="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE country='{}' AND sector_name  = '{}' AND pollutant_name = '{}' ALLOW FILTERING;"
CQL_query = query_select.format('EU-27 (2020)', 'Total (without LULUCF)', 'PFCs - (CO2 equivalent)')
rowsResult = session.execute(CQL_query)
resultDF6 = rowsResult._current_rows
exportDF6 = pd.DataFrame(resultDF6)

query_select ="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE country='{}' AND sector_name  = '{}' AND pollutant_name = '{}' ALLOW FILTERING;"
CQL_query = query_select.format('EU-27 (2020)', 'Total (without LULUCF)', 'SF6 - (CO2 equivalent)')
rowsResult = session.execute(CQL_query)
resultDF7 = rowsResult._current_rows
exportDF7 = pd.DataFrame(resultDF7)

query_select ="SELECT country, pollutant_name, sector_name, unit, year, emissions FROM emissions5.emissions_unfccc WHERE country='{}' AND sector_name  = '{}' AND pollutant_name = '{}' ALLOW FILTERING;"
CQL_query = query_select.format('EU-27 (2020)', 'Total (without LULUCF)', 'Unspecified mix of HFCs and PFCs - (CO2 equivalent)')
rowsResult = session.execute(CQL_query)
resultDF8 = rowsResult._current_rows
exportDF8 = pd.DataFrame(resultDF8)


# Concatenation of results in one dataframe and export to CSV

exportDF = pd.concat([exportDF1, exportDF2, exportDF3, exportDF4, exportDF5, exportDF6, exportDF7, exportDF8])
exportDF.head(15)
exportDF.to_csv(r'/home/fitec/export_data-emissions-unfccc_pollutants-18.csv', index = False) # place 'r' before the path name to avoid any errors in the path



