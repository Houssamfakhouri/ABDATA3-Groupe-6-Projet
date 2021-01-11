#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 14 22:16:16 2020
@author: fitec ABDATA3 groupe 6
"""

# IMPORT LIBRARIES

import pandas as pd
import numpy as np
import zipfile

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import warnings
warnings.filterwarnings('ignore')

print("Lancer l'importation des données (3 fichiers: émissions de CO2, températures et populations) dans Cassandra (attention, près d'un million de données, compter 10-20mn pour ce processus)")
responseStart = input("Taper 'oui' pour commencer, 'non' pour sortir : ")
#print(responseStart)

if responseStart == "oui" :
    
    # CONNECTION TO CASSANDRA CLUSTER
    # Connection to Cassandra cluster in local
    CASSANDRA_HOST = ['localhost']
    CASSANDRA_PORT = 9042
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    
    try :
        cluster = Cluster(protocol_version=3, contact_points=CASSANDRA_HOST, load_balancing_policy=None, port=CASSANDRA_PORT, auth_provider=auth_provider)
        session =cluster.connect()
        print("Connection à CassandraDB ok")
    except ValueError :
        print("Oops! échec de connexion au cluster. Try again...")
    
    # *** CO2 EMISSIONS DATA ***
    # Unzip original file
    try :
        #zip = zipfile.ZipFile('/home/fitec/Bureau/Projet_fil_rouge/code/ABDATA3-Groupe-6-Projet/UNFCCC_v23.csv2.zip')
        zip = zipfile.ZipFile('/home/groupe6/UNFCCC_v23.csv.zip')
        zip.extractall()
        print("Fichier des données CO2 extrait")
    except :    
        print("Problème: le fichier des données CO2 n'a pas pu s'extraire.")
    
    # DEFINE PATH FOR CSV FILE
    #file_path = '/home/fitec/Bureau/Projet_fil_rouge/Data_sources/UNFCCC_v23.csv'
    file_path = '/home/groupe6/UNFCCC_v23.csv'
    
    # Creation of organized pandas dataframe
    data_origin = pd.read_csv(file_path, sep=",", quotechar='"', encoding="utf-8")
    data_columns=data_origin.columns
    data_origin.fillna(0)
    
    # Generation of a new column with index number
    data_origin['id'] = np.arange(len(data_origin))
    data_origin.shape
    data_origin.head(20)
    
    # Creation du key space
    session.execute("CREATE KEYSPACE IF NOT EXISTS emissions5 WITH REPLICATION={'class':'SimpleStrategy','replication_factor':3};")
    session.execute("CREATE TABLE IF NOT EXISTS emissions5.emissions_unfccc (country_code TEXT, country TEXT, format_name TEXT, pollutant_name TEXT, sector_code TEXT, sector_name TEXT, parent_sector_code TEXT, unit TEXT, year TEXT, emissions TEXT, notation TEXT, publicationDate TEXT, dataSource TEXT, id TEXT, primary key (id));")
    session = cluster.connect()
    
    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
        #on force ici a respecter le dataframe de pandas lors de la recuperation des données
        
    session.row_factory = pandas_factory
    session.default_fetch_size = 1000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.
     
    # Insertion of dataframe data into Cassandra keyspace
    query_insert="INSERT INTO emissions5.emissions_unfccc (country_code, country, format_name, pollutant_name, sector_code, sector_name, parent_sector_code, unit, year, emissions, notation, publicationDate, dataSource, id) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', $${}$$);"
    for ct in data_origin.index:
        CQL_query = query_insert.format(data_origin['Country_code'][ct], data_origin['Country'][ct],data_origin['Format_name'][ct], data_origin['Pollutant_name'][ct], data_origin['Sector_code'][ct], data_origin['Sector_name'][ct], data_origin['Parent_sector_code'][ct], data_origin['Unit'][ct], data_origin['Year'][ct], data_origin['emissions'][ct],data_origin['Notation'][ct], data_origin['PublicationDate'][ct], data_origin['DataSource'][ct], data_origin['id'][ct])
        session.execute(CQL_query)
    
    print("Ingestion des données d'émissions CO2 réalisée")
    
    
    # *** TEMPERATURES DATA ***
    # DEFINE PATH FOR CSV FILE
    #file_path = '/home/fitec/Bureau/Projet_fil_rouge/Data_sources/global-average-air-temperature-anomalies-6.csv'
    file_path = '/home/groupe6/global-average-air-temperature-anomalies-6.csv'
    
    # Creation of organized pandas dataframe
    data_origin = pd.read_csv(file_path, sep=",", quotechar='"', encoding="utf-8")
    data_columns=data_origin.columns
    data_origin.fillna(0)
    
    # Generation of a new column with index number
    data_origin['id'] = np.arange(len(data_origin))
    data_origin.shape
    data_origin.head(20)
    
    # Creation du key space
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

    print("Ingestion des données de températures réalisée")


    # *** POPULATION DATA ***

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

    #Rename first column to avoid insertion bugs in Cassandra - NOT WORKING !!
    data_origin = data_origin_ini.rename(columns = {"indic_de,geo\time":"indic_geo_time"}) 
    print(data_origin)
    
    # Generation of a new column with index number
    data_origin['id'] = np.arange(len(data_origin))
    data_origin.shape
    data_origin.head(20)

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

    print("Ingestion des données de population réalisée")
    
    print("Fin du processus d'injection, vous pouvez maintenant utiliser les données depuis Cassandra.")
# Closing connection ??

else:
    print("Sans exécution, au revoir.")



