# -*- coding: utf-8 -*-
"""
Created on Sat Apr 29 19:02:17 2023

@author: aparajitha
"""
# git_url = "https://github.com/PHonepe/pulse"
repo_dir = "PhonePePulseData"
# from git import Repo
# Repo.clone_from(git_url, repo_dir)
import pandas as pd
import json
import os
import mysql.connector as mysql
from sqlalchemy import create_engine
from mysql.connector import Error

def get_aggregated_transaction_table(path) :
    state_list=os.listdir(path)
    #Extracting data from directories
    col={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
    for s in state_list:
        p_s=path+s+"/"
        yr_list=os.listdir(p_s)    
        #print(s,*yr_list)
        for y in yr_list:
            p_s_y=p_s+y+"/"
            #print(p_s_y)
            q_list=os.listdir(p_s_y)  
            # print(s,y,*q_list)
            for j in q_list:
                if j.endswith(".json") :
                     p_j=p_s_y+j
                     # print(p_j)
                     JsonData=open(p_j,'r')
                     D=json.load(JsonData)
                     for z in D['data']['transactionData']:
                        name=z['name']
                        cnt=z['paymentInstruments'][0]['count']
                        amt=z['paymentInstruments'][0]['amount']
                        col['Transaction_type'].append(name)
                        col['Transaction_count'].append(cnt)
                        col['Transaction_amount'].append(amt)
                        col['State'].append(s)
                        col['Year'].append(y)
                        col['Quarter'].append(int(j.strip('.json')))
                        
    df = pd.DataFrame(col)
    try:
        conn = mysql.connect(host='localhost', user='root',
                            password='Raji@19')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePe_Pulse_db_test3;")
            print("PhonePe_Pulse_db_test3 database is created")
            #cursor.execute("SELECT PhonePe_Pulse_db_test3();")
            #record = cursor.fetchone()
            cursor.execute("USE PhonePe_Pulse_db_test3;")
            cursor.execute('''CREATE TABLE IF NOT EXISTS Aggregated_Transaction (
                          State TEXT,
                        Year int,
                        Quarter int,
                        Transaction_type TEXT,
                        Transaction_count int,
                        Transaction_amount float
                        )''')
            for i,row in df.iterrows():
                sql = "INSERT INTO PhonePe_Pulse_db_test3.Aggregated_Transaction VALUES (%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))                        
                conn.commit()
            print("PhonePe_Pulse_db_test3.Aggregated_Transaction table created and updated")
    except Error as e:
        print("Error while connecting to MySQL", e)



def get_aggregated_user_table(path) :
    state_list=os.listdir(path)
    #Extracting data from directories
    col={'State':[], 'Year':[],'Quarter':[],'brand_name':[], 'brand_count':[], 'brand_percentage':[]}
    for s in state_list:
        p_s=path+s+"/"
        yr_list=os.listdir(p_s)    
        # print(s,*yr_list)
        for y in yr_list:
            p_s_y=p_s+y+"/"
            #print(p_s_y)
            q_list=os.listdir(p_s_y)  
            # print(s,y,*q_list)
            for j in q_list:
                if j.endswith(".json") :
                     p_j=p_s_y+j
                     # print(p_j)
                     JsonData=open(p_j,'r')
                     D=json.load(JsonData)
                     #print(type(D))
                     if (D['data']['usersByDevice'] != None):
                         for z in D['data']['usersByDevice']:
                            name=z['brand']
                            cnt=z['count']
                            amt=z['percentage']
                            col['brand_name'].append(name)
                            col['brand_count'].append(cnt)
                            col['brand_percentage'].append(amt)
                            col['State'].append(s)
                            col['Year'].append(y)
                            col['Quarter'].append(int(j.strip('.json')))
                        
    df = pd.DataFrame(col)
    try:
        conn = mysql.connect(host='localhost', user='root',
                            password='Raji@19')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePe_Pulse_db_test3;")
            print("PhonePe_Pulse_db_test3 database is created")
            #cursor.execute("SELECT PhonePe_Pulse_db_test3();")
            #record = cursor.fetchone()
            cursor.execute("USE PhonePe_Pulse_db_test3;")
            cursor.execute('''CREATE TABLE IF NOT EXISTS Aggregated_user (
                          State TEXT,
                        Year int,
                        Quarter int,
                        Brand_type TEXT,
                        Brand_count int,
                        Brand_percentage float
                        )''')
            for i,row in df.iterrows():
                sql = "INSERT INTO PhonePe_Pulse_db_test3.Aggregated_user VALUES (%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))                        
                conn.commit()
            print("PhonePe_Pulse_db_test3.Aggregated_user Table is created and updated")
    except Error as e:
        print("Error while connecting to MySQL", e)

def get_map_transaction_table(path) :
    state_list=os.listdir(path)
    #Extracting data from directories
    col={'State':[], 'Year':[],'Quarter':[],'district_name':[], 'HoverDataMetric_type':[], 'HoverDataMetric_count':[],'HoverDataMetric_amount':[]}
    for s in state_list:
        p_s=path+s+"/"
        yr_list=os.listdir(p_s)    
        # print(s,*yr_list)
        for y in yr_list:
            p_s_y=p_s+y+"/"
            #print(p_s_y)
            q_list=os.listdir(p_s_y)  
            # print(s,y,*q_list)
            for j in q_list:
                if j.endswith(".json") :
                     p_j=p_s_y+j
                     # print(p_j)
                     JsonData=open(p_j,'r')
                     D=json.load(JsonData)
                     mtype_t = ""
                     cnttotal=0
                     amttotal=0.0
                     #print(type(D))
                     if (D['data']['hoverDataList'] != None):
                         for z in D['data']['hoverDataList']:
                            name=z['name']
                            mtype=z['metric'][0]['type']
                            cnt=z['metric'][0]['count']
                            amt=z['metric'][0]['amount']
                            col['district_name'].append(name)
                            col['HoverDataMetric_type'].append(mtype)
                            col['HoverDataMetric_count'].append(cnt)
                            col['HoverDataMetric_amount'].append(amt)
                            col['State'].append(s)
                            col['Year'].append(y)
                            col['Quarter'].append(int(j.strip('.json')))
                            cnttotal+=cnt
                            amttotal+=amt
                            mtype_t=mtype
                     col['district_name'].append(s)
                     col['HoverDataMetric_type'].append(mtype_t)
                     col['HoverDataMetric_count'].append(cnttotal)
                     col['HoverDataMetric_amount'].append(amttotal)
                     col['State'].append('india')
                     col['Year'].append(y)
                     col['Quarter'].append(int(j.strip('.json')))
                    
                        
    df = pd.DataFrame(col)
    try:
        conn = mysql.connect(host='localhost', user='root',
                            password='Raji@19')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePe_Pulse_db_test3;")
            print("PhonePe_Pulse_db_test3 database is created")
            #cursor.execute("SELECT PhonePe_Pulse_db_test3();")
            #record = cursor.fetchone()
            cursor.execute("USE PhonePe_Pulse_db_test3;")
            cursor.execute('''CREATE TABLE IF NOT EXISTS map_transaction(
                          State TEXT,
                        Year int,
                        Quarter int,
                        district_name TEXT,
                        HoverDataMetric_type TEXT,
                        HoverDataMetric_count int,
                        HoverDataMetric_amount float
                        )''')
            for i,row in df.iterrows():
                sql = "INSERT INTO PhonePe_Pulse_db_test3.map_transaction VALUES (%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))                        
                conn.commit()
            print("PhonePe_Pulse_db_test3.map_transaction Table is created and updated")
    except Error as e:
        print("Error while connecting to MySQL", e)

def get_map_user_table(path) :
    state_list=os.listdir(path)
    #Extracting data from directories
    col={'State':[], 'Year':[],'Quarter':[],'district_name':[], 'Registered_users':[], 'Appopens':[]}
    for s in state_list:
        p_s=path+s+"/"
        yr_list=os.listdir(p_s)    
        # print(s,*yr_list)
        for y in yr_list:
            p_s_y=p_s+y+"/"
            #print(p_s_y)
            q_list=os.listdir(p_s_y)  
            # print(s,y,*q_list)
            for j in q_list:
                if j.endswith(".json") :
                     p_j=p_s_y+j
                     # print(p_j)
                     JsonData=open(p_j,'r')
                     D=json.load(JsonData)
                     cnttotal=0
                     appopenstotal=0
                     #print(type(D))
                     if (D['data']['hoverData'] != None):
                         for z in D['data']['hoverData']:
                             distname = z
                             cnt = D['data']['hoverData'][z]['registeredUsers']
                             appopens = D['data']['hoverData'][z]['appOpens']
                             col['district_name'].append(distname)
                             col['Registered_users'].append(cnt)
                             col['Appopens'].append(appopens)
                             col['State'].append(s)
                             col['Year'].append(y)
                             col['Quarter'].append(int(j.strip('.json')))
                             cnttotal+=cnt
                             appopenstotal+=appopens

                     col['district_name'].append(s)
                     col['Registered_users'].append(cnttotal)
                     col['Appopens'].append(appopenstotal)
                     col['State'].append('india')
                     col['Year'].append(y)
                     col['Quarter'].append(int(j.strip('.json')))
                        
    df = pd.DataFrame(col)
    try:
        conn = mysql.connect(host='localhost', user='root',
                            password='Raji@19')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePe_Pulse_db_test3;")
            print("PhonePe_Pulse_db_test3 database is created")
            #cursor.execute("SELECT PhonePe_Pulse_db_test3();")
            #record = cursor.fetchone()
            cursor.execute("USE PhonePe_Pulse_db_test3;")
            cursor.execute('''CREATE TABLE IF NOT EXISTS map_user(
                          State TEXT,
                        Year int,
                        Quarter int,
                        district_name TEXT,
                        Registered_users int,
                        Appopens BIGINT
                        )''')
            for i,row in df.iterrows():
                sql = "INSERT INTO PhonePe_Pulse_db_test3.map_user VALUES (%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))                        
                conn.commit()
            print("PhonePe_Pulse_db_test3.map_user Table is created and updated")
    except Error as e:
        print("Error while connecting to MySQL", e)

def get_top_transaction_tables(path) :
    state_list=os.listdir(path)
    #Extracting data from directories
    col={'State':[], 'Year':[],'Quarter':[],'district_name':[], 'metric_type':[], 'metric_count':[], 'metric_amount':[]}
    col1={'State':[], 'Year':[],'Quarter':[],'pincode':[], 'metric_type':[], 'metric_count':[], 'metric_amount':[]}
    for s in state_list:
        p_s=path+s+"/"
        yr_list=os.listdir(p_s)    
        # print(s,*yr_list)
        for y in yr_list:
            p_s_y=p_s+y+"/"
            #print(p_s_y)
            q_list=os.listdir(p_s_y)  
            # print(s,y,*q_list)
            for j in q_list:
                if j.endswith(".json") :
                     p_j=p_s_y+j
                     # print(p_j)
                     JsonData=open(p_j,'r')
                     D=json.load(JsonData)
                     #print(type(D))
                     if (D['data']['districts'] != None):
                         for z in D['data']['districts']:
                             distname = z['entityName']
                             typ=z['metric']['type']
                             cnt=z['metric']['count']
                             amt=z['metric']['amount']
                             col['district_name'].append(distname)
                             col['metric_type'].append(typ)
                             col['metric_count'].append(cnt)
                             col['metric_amount'].append(amt)
                             col['State'].append(s)
                             col['Year'].append(y)
                             col['Quarter'].append(int(j.strip('.json')))
                     if (D['data']['pincodes'] != None):
                        for z in D['data']['pincodes']:
                             pincode = z['entityName']
                             typ=z['metric']['type']
                             cnt=z['metric']['count']
                             amt=z['metric']['amount']
                             col1['pincode'].append(pincode)
                             col1['metric_type'].append(typ)
                             col1['metric_count'].append(cnt)
                             col1['metric_amount'].append(amt)
                             col1['State'].append(s)
                             col1['Year'].append(y)
                             col1['Quarter'].append(int(j.strip('.json')))
                        
    df = pd.DataFrame(col)
    df1 = pd.DataFrame(col1)
    try:
        conn = mysql.connect(host='localhost', user='root',
                            password='Raji@19')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePe_Pulse_db_test3;")
            print("PhonePe_Pulse_db_test3 database is created")
            #cursor.execute("SELECT PhonePe_Pulse_db_test3();")
            #record = cursor.fetchone()
            cursor.execute("USE PhonePe_Pulse_db_test3;")
            cursor.execute('''CREATE TABLE IF NOT EXISTS top_transaction_district(
                          State TEXT,
                        Year int,
                        Quarter int,
                        district_name TEXT,
                        metric_type TEXT,
                        metric_count int,
                        metric_amount float
                        )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS top_transaction_pincode(
                          State TEXT,
                        Year int,
                        Quarter int,
                        pincode int,
                        metric_type TEXT,
                        metric_count int,
                        metric_amount float
                        )''')
            for i,row in df.iterrows():
               # print("DEBUG1",i,row)
                sql = "INSERT INTO PhonePe_Pulse_db_test3.top_transaction_district VALUES (%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))                        
                #conn.commit()
            print("PhonePe_Pulse_db_test3.top_transaction_district Table is created and updated")
            for i,row in df1.iterrows():
               # print("DEBUG2",i,row)
                sql = "INSERT INTO PhonePe_Pulse_db_test3.top_transaction_pincode VALUES (%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))                        
            conn.commit()
            print("PhonePe_Pulse_db_test3.top_transaction_pincode Table is created and updated")
    except Error as e:
        print("Error while connecting to MySQL", e)
  
def get_top_user_tables(path) :
    state_list=os.listdir(path)
    #Extracting data from directories
    col={'State':[], 'Year':[],'Quarter':[],'district_name':[], 'registered_users': []}
    col1={'State':[], 'Year':[],'Quarter':[],'pincode':[], 'registered_users': []}
    for s in state_list:
        p_s=path+s+"/"
        yr_list=os.listdir(p_s)    
        # print(s,*yr_list)
        for y in yr_list:
            p_s_y=p_s+y+"/"
            #print(p_s_y)
            q_list=os.listdir(p_s_y)  
            # print(s,y,*q_list)
            for j in q_list:
                if j.endswith(".json") :
                     p_j=p_s_y+j
                     # print(p_j)
                     JsonData=open(p_j,'r')
                     D=json.load(JsonData)
                     #print(type(D))
                     if (D['data']['districts'] != None):
                         for z in D['data']['districts']:
                             distname = z['name']
                             ucnt=z['registeredUsers']
                             # print(distname,ucnt)
                             col['district_name'].append(distname)
                             col['registered_users'].append(ucnt)
                             col['State'].append(s)
                             col['Year'].append(y)
                             col['Quarter'].append(int(j.strip('.json')))
                     if (D['data']['pincodes'] != None):
                        for z in D['data']['pincodes']:
                             pincode = z['name']
                             ucnt = z['registeredUsers']
                             # print("DEBUG2:",pincode,ucnt)
                             col1['pincode'].append(pincode)
                             col1['registered_users'].append(ucnt)
                             col1['State'].append(s)
                             col1['Year'].append(y)
                             col1['Quarter'].append(int(j.strip('.json')))
                        
    df = pd.DataFrame(col)
    df1 = pd.DataFrame(col1)
    try:
        conn = mysql.connect(host='localhost', user='root',
                            password='Raji@19')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePe_Pulse_db_test3;")
            print("PhonePe_Pulse_db_test3 database is created")
            #cursor.execute("SELECT PhonePe_Pulse_db_test3();")
            #record = cursor.fetchone()
            cursor.execute("USE PhonePe_Pulse_db_test3;")
            cursor.execute('''CREATE TABLE IF NOT EXISTS top_user_district(
                          State TEXT,
                        Year int,
                        Quarter int,
                        district_name TEXT,
                        registered_users int
                        )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS top_user_pincode(
                          State TEXT,
                        Year int,
                        Quater int,
                        pincode int,
                        registered_users int
                        )''')
            for i,row in df.iterrows():
               # print("DEBUG1",i,row)
                sql = "INSERT INTO PhonePe_Pulse_db_test3.top_user_district VALUES (%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))                        
                #conn.commit()
            print("PhonePe_Pulse_db_test3.top_user_district Table is created and updated")
            for i,row in df1.iterrows():
               # print("DEBUG2",i,row)
                sql = "INSERT INTO PhonePe_Pulse_db_test3.top_user_pincode VALUES (%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))                        
            conn.commit()
            print("PhonePe_Pulse_db_test3.top_user_pincode Table is created and updated")
    except Error as e:
        print("Error while connecting to MySQL", e)
  
    
path=repo_dir+"/data/aggregated/transaction/country/india/state/"
get_aggregated_transaction_table(path)
path=repo_dir+"/data/aggregated/user/country/india/state/"
get_aggregated_user_table(path)
path=repo_dir+"/data/map/transaction/hover/country/india/state/"
get_map_transaction_table(path)
path=repo_dir+"/data/map/user/hover/country/india/state/"
get_map_user_table(path)
path=repo_dir+"/data/top/transaction/country/india/state/"
get_top_transaction_tables(path)
path=repo_dir+"/data/top/user/country/india/state/"
get_top_user_tables(path)

#Inserting dataframe into mysql db





# # # #create a table in the database

# insert_query = "INSERT INTO transaction_data VALUES (%s,%s,%s,%s,%s,%s)"
# records_to_insert = [(col['State'], col['Year'], col['Quarter'], col['Transaction_type'], col['Transaction_count'], col['Transaction_amount'])]
   
# #cursor.executemany(insert_query, records_to_insert)
#     # connection.commit()
#     # print(cursor.rowcount, "Record inserted successfully into table")
# # # #insert the All data into the table
# c.executemany(insert_query, records_to_insert)
# connection.commit()
