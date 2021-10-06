#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  6 18:24:26 2021

@author: adityan_manick
"""

#importing pandas a python library to work with datas
import pandas as pd
#importing sqlalchemy a library used to connect to mysql database
import sqlalchemy
import numpy as np
from datetime import datetime
#importing boto3 is a python library to fetch the aws s3 bucket details
import boto3
#apscheduler is used to schedule the function at particular interval
from apscheduler.schedulers.blocking import BlockingScheduler

#Getting my s3 details 
s3 = boto3.resource('s3')

#Connecting to mysql database
name='root' 
password=''
database_name='ETL'

engine=sqlalchemy.create_engine('mysql+pymysql://'+name+':'+password+'@localhost:3306/'+database_name)

def incremental_extract():
    #Getting data from the table
    cus_df = pd.read_sql('select * from customer',engine)
    cus_temp_df = pd.read_sql('select * from cust_temp where s_c_id in (select max(s_c_id) from cust_temp group by c_id);',engine)

    #Changing the name of the column
    cus_temp_df = cus_temp_df.rename(columns={'c_id':'c_id_y'})

    #Merging the two dataframes
    m_df = pd.merge(cus_df, cus_temp_df, how = 'left', left_on = 'c_id', right_on = 'c_id_y')

    #Setting the flag which we have to work with
    m_df['flag'] = np.where((m_df.c_id == m_df.c_id_y) & (m_df.c_name_x == m_df.c_name_y) & 
                              (m_df.c_city_x == m_df.c_city_y),'F','T')

    #Getting the current datetime using now()
    now = datetime.now()
    #Loading the 'T' flaged rows to a dataframe
    df3 = m_df.loc[m_df['flag']=='T']

    #Reseting the index
    df3 = df3.reset_index()

    #Iterating rows from dataframe and load that to a table cust_temp
    n = 0
    for i, row in df3.iterrows():
        ins_query = 'insert into cust_temp (s_c_id,c_id,c_name,c_city,crt_date) values (%d,%d,"%s","%s","%s")'%(0,df3['c_id'][n],
                                                                                                      df3['c_name_x'][n],
                                                                                                     df3['c_city_x'][n],
                                                                                                    now.strftime('%Y-%m-%d %H:%M:%S'))
        engine.execute(ins_query)
        n+=1
    
    #Loading data from cust_temp with updated data
    u_df = pd.read_sql('select * from cust_temp',engine)
    #Converting the dataframe into a csv file
    u_df.to_csv('cust_temp.csv', index = False)
    #Uploading the csv file to aws s3 bucket
    s3.meta.client.upload_file('cust_temp.csv','incremental-extract','cust_temp.csv')
    print('File uploaded to s3....')

#Starting the schedular
print('Hello, Please Wait...')
print()
scheduler = BlockingScheduler()
scheduler.add_job(incremental_extract, 'interval', Hours=1)
scheduler.start()