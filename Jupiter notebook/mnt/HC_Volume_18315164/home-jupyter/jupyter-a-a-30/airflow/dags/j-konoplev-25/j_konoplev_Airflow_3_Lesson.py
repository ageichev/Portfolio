#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from io import StringIO

VGsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
VGsales_file = 'vgsales.csv'


default_args = {
    'owner': 'j-konoplev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 9),
}
schedule_interval = '55 21 * * *'


@dag(default_args=default_args, catchup=False)
def j_konoplev_3_lesson_airflow():
    @task()
    def get_data():
        data = pd.read_csv(VGsales)
        return data

    @task()
    def get_top_1_game_sales(data):
        data = pd.read_csv(StringIO(data))
        year = 1994 + hash(f'j-konoplev-25') % 23
        top_game_sales = data[data.Year == year].groupby('Name').agg({'Global_Sales':'sum'})
        top_1_game_sales = top_game_sales[top_game_sales.Global_Sales == top_game_sales.Global_Sales.max()]
        return top_1_game_sales.to_csv(index=True)

    @task()
    def get_top_1_eu_sales(data):
        data = pd.read_csv(StringIO(data))
        year = 1994 + hash(f'j-konoplev-25') % 23
        top_eu_sales = data[data.Year == year].groupby('Genre').agg({'EU_Sales':'sum'})
        top_1_eu_sales = top_eu_sales[top_eu_sales.EU_Sales == top_eu_sales.EU_Sales.max()]
        return top_1_eu_sales.to_csv(index=True)

    @task()
    def get_top_more_1_ml(data):
        data = pd.read_csv(StringIO(data))
        year = 1994 + hash(f'j-konoplev-25') % 23
        more_1_ml = data[data.Year == year].groupby('Platform').agg({'NA_Sales':'sum'})
        top_more_1_ml = more_1_ml.query('NA_Sales > 1').sort_values('NA_Sales', ascending=False)
        return top_more_1_ml.to_csv(index=True)       

    @task()        
    def get_top_jp_publisher(data):
        data = pd.read_csv(StringIO(data))
        top_jp_mean_publisher = data[data.Year == year].groupby('Publisher').agg({'JP_Sales':'mean'})
        top_jp_publisher = top_jp_mean_publisher[top_jp_mean_publisher.JP_Sales == top_jp_mean_publisher.JP_Sales.max()]
        return top_jp_publisher.to_csv(index=True)

    @task()       
    def get_eu_better_jp(data):
        data = pd.read_csv(StringIO(data))
        year = 1994 + hash(f'j-konoplev-25') % 23
        data['EU_JP_Sales_diff'] = data['EU_Sales'] - data['JP_Sales']
        eu_better_jp = data[data.Year == year].query('EU_JP_Sales_diff > 0').Name.nunique()
        return eu_better_jp.to_csv(index=False)        



    @task()
    def print_data(top_1_game_sales, top_1_eu_sales, top_more_1_ml, top_jp_publisher, eu_better_jp):

        date = ''

        print(f'Самая продаваемая игра в мире {date}')
        print(top_1_game_sales)

        print(f'Самый продаваемый игровой жанр в Европе {date}')
        print(top_1_eu_sales)

        print(f'Платформы с миллионными тиражами {date}')
        print(top_more_1_ml)

        print(f'Самый успешный издатель в Японии {date}')
        print(top_jp_publisher)

        print(f'Количество игр с продажами в Европе лучше, чем в Японии {date}')
        print(eu_better_jp)

        
        data = get_data()
        top_1_game_sales = get_top_1_game_sales(data)
        top_1_eu_sales = get_top_1_eu_sales(data)
        top_more_1_ml = get_top_more_1_ml(data)
        top_jp_publisher = get_top_jp_publisher(data)
        eu_better_jp = get_eu_better_jp(data)
        print_data(top_1_game_sales, top_1_eu_sales, top_more_1_ml, top_jp_publisher, eu_better_jp)
        
j_konoplev_3_lesson_airflow = j_konoplev_3_lesson_airflow()

