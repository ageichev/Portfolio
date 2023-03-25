#!/usr/bin/env python
# coding: utf-8

# In[ ]:
import requests
# from zipfile import ZipFile
# from io import BytesIO
import pandas as pd
# import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
# import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
# TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {
    'owner': 'v-kolegov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 26),
    'schedule_interval': '0 12 * * *'
}

# CHAT_ID = -620798068
# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''

# def send_message(context):
#     date = context['ds']
#     dag_id = context['dag'].dag_id
#     message = f'Huge success! Dag {dag_id} completed on {date}'
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

login = 'v-kolegov-22'
year = 1994 +  hash('{login}') % 23 

@dag(default_args=default_args, catchup=False)
def games_vkolegov22():
    @task(retries=4)
    def get_data():
#         top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
#         zipfile = ZipFile(BytesIO(top_doms.content))
#         top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
#         return top_data
        top_games = pd.read_csv('vgsales.csv')
        df = top_games.query('Year == @year')
        return df

    @task(retries=4)
    def get_popular_in_world(df):
#         top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
#         top_data_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
#         return top_data_ru.to_csv(index=False)
        popular_in_world = df.groupby('Name').agg('Global_Sales').sum().reset_index().sort_values('Global_Sales', ascending=False).head(1).iloc[0,0]
        return popular_in_world
    
    
#     @task()
#     def get_stat_ru(top_data_ru):
#         ru_df = pd.read_csv(StringIO(top_data_ru))
#         ru_avg = int(ru_df['rank'].aggregate(np.mean))
#         ru_median = int(ru_df['rank'].aggregate(np.median))
#         return {'ru_avg': ru_avg, 'ru_median': ru_median}

    @task(retries=4)
    def get_genre_in_eu(df):
#         top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
#         top_data_com = top_data_df[top_data_df['domain'].str.endswith('.com')]
#         return top_data_com.to_csv(index=False)

        genre_in_eu = df.groupby('Genre').agg('EU_Sales').sum().reset_index().sort_values('EU_Sales', ascending=False).head(1).iloc[0,0]
        return genre_in_eu
    
    
#     @task()
#     def get_stat_com(top_data_com):
#         com_df = pd.read_csv(StringIO(top_data_com))
#         com_avg = int(com_df['rank'].aggregate(np.mean))
#         com_median = int(com_df['rank'].aggregate(np.median))
#         return {'com_avg': com_avg, 'com_median': com_median}

    @task(retries=4)
    def get_platform_na(df):
        platform_na = df.query('NA_Sales > 1').groupby('Platform').agg('NA_Sales').sum().reset_index().sort_values('NA_Sales', ascending=False).head(1).iloc[0,0]
        return platform_na

    @task(retries=4)
    def get_publisher_jp(df):
        publisher_jp = df.groupby('Publisher').agg('JP_Sales').mean().reset_index().sort_values('JP_Sales', ascending=False).head(1).iloc[0,0]
        return publisher_jp
     
    @task(retries=4)
    def get_eu_than_jp(df):
        eu_than_jp = df.groupby('Name').agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).reset_index().query('EU_Sales > JP_Sales').shape[0]
        return eu_than_jp
    

    @task(retries=4)
#     def print_data(ru_stat, com_stat):
    def print_data(popular_in_world, genre_in_eu, platform_na, publisher_jp,eu_than_jp):    

        context = get_current_context()
        date = context['ds']

#         ru_avg, ru_median = ru_stat['ru_avg'], ru_stat['ru_median']
#         com_avg, com_median = com_stat['com_avg'], com_stat['com_median']

#         print(f'''Data from .RU for {date}
#                   Avg rank: {ru_avg}
#                   Median rank: {ru_median}''')

#         print(f'''Data from .COM for {date}
#                           Avg rank: {com_avg}
#                           Median rank: {com_median}''')
        print(f'The most popular in world {date}')
        print(popular_in_world)

        print(f'The most popular genre in EU {date}')
        print(genre_in_eu)
        
        print(f'The most popular platform in NA {date}')
        print(platform_na)
        
        print(f'The most popular publisher in Japan {date}')
        print(publisher_jp)
        
        print(f'How many games sold better in EU rather than in Japan {date}')
        print(eu_than_jp)
        
        
#     top_data = get_data()

#     top_data_ru = get_table_ru(top_data)
#     ru_data = get_stat_ru(top_data_ru)

#     top_data_com = get_table_com(top_data)
#     com_data = get_stat_com(top_data_com)

#     print_data(ru_data, com_data)

    df = get_data()
    
    popular_in_world = get_popular_in_world(df)
    genre_in_eu = get_genre_in_eu(df)
    platform_na = get_platform_na(df)
    publisher_jp = get_publisher_jp(df)
    eu_than_jp = get_eu_than_jp(df)

    print_data(popular_in_world, genre_in_eu, platform_na, publisher_jp, eu_than_jp)

games_vkolegov22 = games_vkolegov22()



