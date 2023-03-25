#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[2]:


login = 'n-vindilovich'
year=1994 + hash(f'{login}') % 23
df_vgsales=pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv').query('Year==@year')


# In[6]:


default_args = {
    'owner': 'n-vindilovich',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 21),
    'schedule_interval': '0 11 * * *'
}


# In[13]:


@dag(default_args=default_args, catchup=False)
def vindilovich_create_own_dag():
    @task(retries=3)
    def get_data():
        login = 'n-vindilovich'
        year=1994 + hash(f'{login}') % 23
        df_vgsales = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv').query('Year==@year')
        return df_vgsales
    
    @task(retries=4, retry_delay=timedelta(10))
    def get_popular_game(df_vgsales):
        popular_game=df_vgsales.query('Global_Sales == @df_vgsales.Global_Sales.max()')['Name'].iloc[0]
        return popular_game
    
    @task()
    def get_popular_genre(df_vgsales):
        popular_genre=df_vgsales.query('EU_Sales == @df_vgsales.EU_Sales.max()')['Genre'].iloc[0]
        return popular_genre
    
    @task()
    def get_popular_platform(df_vgsales):
        popular_platform=df_vgsales.query('NA_Sales > 1').groupby('Platform', as_index = False)         .agg({'Name': 'count'}).sort_values('Name', ascending = False)['Platform'].iloc[0]
        return popular_platform
    
    @task()
    def get_popular_publisher(df_vgsales):
        popular_publisher=df_vgsales.groupby('Publisher', as_index = False).agg({'JP_Sales': 'mean'})         .sort_values('JP_Sales', ascending = False)['Publisher'].iloc[0]
        return popular_publisher
    
    @task()
    def get_eu_more_jp(df_vgsales):
        eu_more_jp=df_vgsales.query('EU_Sales > JP_Sales')['Name'].nunique()
        return eu_more_jp
    
    @task()
    def print_data(popular_game, popular_genre, popular_platform, popular_publisher, eu_more_jp):
        print (f'My year is {year}')
        print (f'The most popular game is {popular_game}')
        print (f'The most popular genre in EU is {popular_genre}')
        print (f'The most popular platform in NA is {popular_platform}')
        print (f'The publisher with the highest average sales in JP is {popular_publisher}')
        print (f'Amount of games which are more popular in EU then in JP is {eu_more_jp}')   
        
    df_vgsales = get_data()
    popular_game = get_popular_game(df_vgsales)
    popular_genre = get_popular_genre(df_vgsales)
    popular_platform = get_popular_platform(df_vgsales)
    popular_publisher = get_popular_publisher(df_vgsales)
    eu_more_jp = get_eu_more_jp(df_vgsales)
    print_data(popular_game, popular_genre, popular_platform, popular_publisher, eu_more_jp)

vindilovich_create_own_dag = vindilovich_create_own_dag()