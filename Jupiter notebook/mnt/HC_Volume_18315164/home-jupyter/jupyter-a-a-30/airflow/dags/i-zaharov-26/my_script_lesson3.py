import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable



login = 'i-zaharov-26'
year = 1994 + hash(f'{login}') % 23
path_to_file = '/var/lib/airflow/airflow.git/dags/a.kosheleva-14/vgsales-1.csv'

default_args = {
    'owner': 'i-zaharov-26',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 10, 18),
    'schedule_interval': '00 6 * * *'
}


@dag(default_args=default_args, catchup=False)
def i_zaharov_lesson3():
    @task(retries=3, retry_delay=timedelta(10))
    def i_zaharov_get_data():
        data = pd.read_csv(path_to_file)
        data = data.query('Year == @year')
        return data


    @task()
    def i_zaharov_top_game(data):
        top_game = data.sort_values('Global_Sales', ascending=False) \
                       .iloc[0]['Name']
        return top_game


    @task()
    def i_zaharov_top_genre(data):
        top_genre_1 = data.groupby('Genre', as_index = False) \
                                .agg({'EU_Sales':'sum'}) 
        top_genre_loc = top_genre_1.sort_values(by='EU_Sales', ascending=False).iloc[0]['EU_Sales']
        top_genre = top_genre_1[top_genre_1['EU_Sales']==top_genre_loc].Genre.values
        return top_genre


    @task()
    def i_zaharov_top_platform_in_NA(data):
        top_platform_NA = data.query('NA_Sales > 1') \
                                .groupby('Platform', as_index = False) \
                                .agg({'Name':'count'}) 
        top_platform_loc = top_platform_NA.sort_values(by='Name', ascending=False).iloc[0]['Name']
        top_platform_NA = top_platform_NA[top_platform_NA['Name'] == top_platform_loc].Platform.values                  
        return top_platform_NA


    @task()
    def i_zaharov_top_JP_publisher(data):
        top_JP_publisher = data.groupby('Publisher', as_index = False) \
                                .agg({'JP_Sales':'sum'}) 
        top_JPsales_loc = top_JP_publisher.sort_values('JP_Sales', ascending = False).iloc[0]['JP_Sales']
        top_JP_publisher = top_JP_publisher[top_JP_publisher['JP_Sales'] == top_JPsales_loc].Publisher.values
        return top_JP_publisher


    @task()
    def i_zaharov_eu_more_than_jp_sales(data):
        eu_more_than_jp = data.query('EU_Sales > JP_Sales')['Name'].nunique()
        return eu_more_than_jp


    @task()
    def i_zaharov_print_data(top_game, top_genre, top_platform_NA, top_JP_publisher, eu_more_than_jp):

        context = get_current_context()
        date = context['ds']

        print(f'''Game sales data in {year} for {date}:
        TOP-game according to global sales:
            {top_game}
        TOP genre in Europe:
            {top_genre}
        TOP Platform in North America with over 1 million game copies sold:
            {top_platform_NA}
        TOP Publisher in Japan (Highest Average Game Sales):
            {top_JP_publisher}
        How many games were sold better in Europe than in Japan:
            {eu_more_than_jp}''')
        

    data = i_zaharov_get_data()
    top_game = i_zaharov_top_game(data)
    top_genre = i_zaharov_top_genre(data)
    top_platform_NA = i_zaharov_top_platform_in_NA(data)
    top_JP_publisher = i_zaharov_top_JP_publisher(data)
    eu_more_than_jp = i_zaharov_eu_more_than_jp_sales(data)
    i_zaharov_print_data(top_game, top_genre, top_platform_NA, top_JP_publisher, eu_more_than_jp)
        
i_zaharov_lesson3 = i_zaharov_lesson3()
