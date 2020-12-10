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

# CONNECTION TO CASSANDRA CLUSTER
# Connection to Cassandra cluster in local
CASSANDRA_HOST = ['localhost']
CASSANDRA_PORT = 9042
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

try :
    cluster = Cluster(protocol_version=3, contact_points=CASSANDRA_HOST, load_balancing_policy=None, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect()
    print('ok')

except ValueError :
    print("Oops! échec de connexion au cluster. Try again...")


# Retrieve data from Cassandra
rowsData = session.execute("SELECT * FROM test14.emissionsv38;")
exportCsvDF = rowsData._current_rows
print(exportCsvDF)


exportDF = pd.DataFrame(exportCsvDF)
exportDF.head(15)
exportDF.to_csv(r'/home/fitec/export_data-01.csv', index = False) # place 'r' before the path name to avoid any errors in the path

"""
# EXPORT TO CSV file
#query_export = "SELECT * FROM test14.emissionsv38;"
query_csvExport="SELECT * FROM test14.emissionsv38;"
queryCsvExport = pd.read_sql_query(query_csvExport, session)
print(queryCsvExport)



session.execute(queryCsvExport)



# DEFINE PATH FOR CSV FILE
file_path = '/home/fitec/Bureau/Projet_fil_rouge/Data_sources/ETS_Database_v38.csv'

"""

