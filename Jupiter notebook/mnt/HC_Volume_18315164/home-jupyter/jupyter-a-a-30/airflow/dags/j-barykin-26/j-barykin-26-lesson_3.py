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


data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'j-barykin-26') % 23

default_args = {
    'owner': 'j-barykin-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 11, 29),
    'schedule_interval': '00 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def lesson_3():

    @task()
    def get_data():
        df = pd.read_csv(data)
        df = df.dropna(subset = ['Year'])
        df['Year'] = df['Year'].apply(lambda x: int(x))
        df_year = df.query('Year == @year')
        return df_year

    @task()
    def get_top_game(df_year):
        top = df_year.groupby('Name', as_index = False).agg({'Global_Sales': 'sum'}) \
            .sort_values('Global_Sales', ascending = False)
        top = top['Name'].iloc[0]
        return top

    @task()
    def get_eu_top(df_year):
        eu = df_year.groupby('Genre', as_index = False).agg({'EU_Sales': 'sum'}) \
            .sort_values('EU_Sales', ascending = False)
        max_eu = eu.EU_Sales.iloc[0]
        eu = eu.query('EU_Sales == @max_eu').Genre.tolist()
        return eu
    
    @task()
    def get_na_top(df_year):
        na = df_year.query('NA_Sales > 1').groupby('Platform', as_index = False).agg({'Name': 'count'}) \
                .sort_values('Name', ascending = False)
        max_na = na.Name.iloc[0]
        na = na.query('Name == @max_na').Platform.tolist()
        return na
    
    @task()
    def get_jp_top(df_year):
        jp = df_year.groupby('Publisher', as_index = False).agg({'JP_Sales': 'mean'}) \
                .sort_values('JP_Sales', ascending = False)
        max_jp = jp.JP_Sales.iloc[0]
        jp = jp.query('JP_Sales == @max_jp').Publisher.tolist()
        return jp
    
    @task()
    def get_eu_and_jp(df_year):
        ej = df_year.groupby('Name', as_index = False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        ej = ej.query('EU_Sales > JP_Sales').shape[0]
        return ej
    

    @task()
    def print_data(t1, t2, t3, t4, t5):

        context = get_current_context()
        date = context['ds']

        print(f'The most sold game of the year {year}: {t1}')
        print(f'The most popular genres in Europe in year {year}: {t2}')
        print(f'NA platforms with the biggest number of bestsellers in year {year}: {t3}')
        print(f'The best publisher in Japan by avarage sales in year {year}: {t4}')
        print(f'The number of games with higher sales in EU than in JP in year {year}: {t5}')

    df_year = get_data()
    
    t1 = get_top_game(df_year)
    t2 = get_eu_top(df_year)
    t3 = get_na_top(df_year)
    t4 = get_jp_top(df_year)
    t5 = get_eu_and_jp(df_year)

    print_data(t1, t2, t3, t4, t5)

lesson_3 = lesson_3()
