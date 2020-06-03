#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys

import getopt
from datetime import datetime

import pandas as pd

from sqlalchemy import create_engine

if __name__ == "__main__":

    #Задаём входные параметры
    unixOptions = "s:e"  
    gnuOptions = ["start_dt=", "end_dt="] 

    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:]    #excluding script name

    try:
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except getopt.error as err:
        print (str(err))
        sys.exit(2)
    
    start_dt = ''
    end_dt = ''
    for currentArgument, currentValue in arguments:
        if currentArgument in ("-s", "--start_dt"):
            start_dt = currentValue
        elif currentArgument in ("-e", "--end_dt"):
            end_dt = currentValue

    db_config = {'user': 'my_user',
				  'pwd': 'my_user_password',
				  'host': 'localhost',
				  'port': 5432,
				  'db': 'zen'} 
    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'], 
                                                             db_config['pwd'], 
                                                             db_config['host'], 
                                                             db_config['port'], 
                                                             db_config['db'])
    engine = create_engine(connection_string)
    
    query = ''' SELECT * , TO_TIMESTAMP (ts / 1000) AT TIME ZONE 'Etc/UTC' AS dt
                FROM log_raw
                WHERE TO_TIMESTAMP (ts / 1000) AT TIME ZONE 'Etc/UTC' BETWEEN '{}' AND '{}' '''.format(start_dt, end_dt)
    

    #Выполним запрос и сохраним результат в DataFrame


    log_raw = pd.io.sql.read_sql(query, con = engine, index_col = 'event_id').reset_index()

    
    columns_numeric = ['event_id', 'item_id', 'source_id', 'user_id']
    columns_datetime = ['dt']

    for column in columns_numeric: log_raw[column] = pd.to_numeric(log_raw[column], errors='coerce')
    for column in columns_datetime: log_raw[column] = pd.to_datetime(log_raw[column]).dt.round('min') 
   
   #Подготовим агрегирующие таблицы
    dash_visits = log_raw.groupby(['item_topic', 'source_topic', 'age_segment', 'dt']).agg({'user_id':'count'}).reset_index()
    dash_visits = dash_visits.rename(columns = {'user_id':'visits'})

    dash_engagement = log_raw.groupby(['event', 'item_topic', 'age_segment', 'dt']).agg({'user_id':'nunique'}).reset_index()
    dash_engagement = dash_engagement.rename(columns = {'user_id':'unique_users'})

    # Сохраним в таблицу dash_visits и dash_engagement нашей базы zen
    dash_visits.to_sql(name = 'dash_visits', con = engine, if_exists = 'append', index = False)
    dash_engagement.to_sql(name = 'dash_engagement', con = engine, if_exists = 'append', index = False)
    print(dash_engagement)







