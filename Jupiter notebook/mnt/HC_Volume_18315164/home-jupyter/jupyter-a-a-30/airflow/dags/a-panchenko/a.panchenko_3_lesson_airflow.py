#!/usr/bin/env python
# coding: utf-8

# In[12]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable



default_args = {
    'owner': 'a-panchenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 2, 12),
    'schedule_interval': '0 12 * * *'
}



@dag(default_args = default_args, catchup = False)
def a_panchenko_less3():
    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        year = 1994 + hash('a-panchenko') % 23
        df = df[df['Year'] == year]
        return df


    @task()
    def top_sale_game(df):
        return df.groupby('Name', as_index=False).agg({'Global_Sales':'sum'}).sort_values('Global_Sales',ascending=False).Name.iloc[0]


    @task()
    def top_sale_genre(df):
        top_genre=df.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'}).sort_values('EU_Sales',ascending=False).Genre.iloc[0]
        return top_genre


    @task()
    def top_platforn_sale(df):
        top_platform=df.query('NA_Sales>1').groupby('Platform',as_index=False).agg({'Name':'count'})\
        .sort_values('Name',ascending=False).Platform.iloc[0]
        return top_platform


    @task()
    def top_sale_publisher(df):
        top_Publisher=df.groupby('Publisher', as_index=False)\
        .agg({'JP_Sales':'mean'}).sort_values('JP_Sales',ascending=False).Publisher.iloc[0]
        return top_Publisher

    @task()
    def eu_japan(df):
        return data.query("EU_Sales > JP_Sales").Name.count()


    @task()
    def print_results(top_game, top_genre,top_platform ,top_publisher,eu):
        print(f'Данные за {date}')
        print(f'Самой продаваемой игрой в этом году во всем мире: {top_game}')
        print(f'Жанр игр, которые были самыми продаваемыми в Европе: {top_genre}')
        print(f'Платформа с cамым большим числом игр, которые продались более чем миллионным тиражом в Северной Америке: {top_platform}')
        print(f'Издатель с самыми высокими средними продажами в Японии: {top_publisher}')
        print(f'Количество игр, которые продались лучше в Европе, чем в Японии: {eu}')
    df = get_data()
    top_game = top_sale_game(df)
    top_genre = top_sale_genre(df)
    top_platform = top_platforn_sale(df)
    top_publisher = top_sale_publisher(df)
    eu = eu_japan(df)
    print_results(top_game, top_genre,top_platform ,top_publisher,eu)


a_panchenko_less3=a_panchenko_less3()

