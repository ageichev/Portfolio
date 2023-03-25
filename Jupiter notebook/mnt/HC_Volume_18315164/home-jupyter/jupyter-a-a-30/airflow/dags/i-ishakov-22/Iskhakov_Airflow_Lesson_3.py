import requests
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'i-ishakov-22'
my_year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'i-ishakov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 27),
    'schedule_interval': '* 12 * * *'
}

@dag(default_args=default_args, catchup=False)

def i_ishakov_22_airflow_lesson3():
    
    @task()
    def get_data():        
        df = pd.read_csv(file)
        df = df.query('Year == @my_year')
        return df
    
    @task()
    def get_top_sale_game(df):
        top_sale_game = df.groupby('Name', as_index=False) \
                          .agg({'Global_Sales': 'sum'}) \
                          .sort_values('Global_Sales', ascending=False)
        return top_sale_game
    
    @task()
    def get_top_EU_genre(df):
        top_EU_genre = df.groupby('Genre', as_index=False) \
                         .agg({'EU_Sales': 'sum'}) \
                         .sort_values('EU_Sales', ascending=False)
        top_EU_genre = top_EU_genre.query('EU_Sales == EU_Sales.max()').Genre \
                                   .values[0] 
        return top_EU_genre
    
    @task()
    def get_NA_1m_sales(df):
        NA_1m_sales = df.query('NA_Sales > 1') \
                        .groupby('Platform', as_index=False) \
                        .agg({'Publisher': 'count'}) \
                        .sort_values('Publisher', ascending=False) \
                        .rename(columns={'Publisher': 'quantity'})

        NA_1m_sales = NA_1m_sales.query('quantity == quantity.max()').Platform.values[0]
        return NA_1m_sales
    
    @task()    
    def get_Japan_sales(df):
        Japan_sales = df.groupby('Publisher', as_index=False) \
                        .agg({'JP_Sales': 'mean'}) \
                        .sort_values('JP_Sales', ascending=False) \
                        .rename(columns={'JP_Sales': 'avg_sales'})

        Japan_sales = Japan_sales.query('avg_sales == avg_sales.max()').Publisher.values[0]
        return Japan_sales
    
    @task()    
    def get_EU_count(df):
        EU_count = df.query('EU_Sales > JP_Sales').Name.count()
        return EU_count
    
    @task()
    def print_data(top_sale_game, top_EU_genre, NA_1m_sales, Japan_sales, EU_count):

        context = get_current_context()
        date = context['ds']

        print(f'''Data for {date}
                  Top_sale_game: {top_sale_game}
                  Top_EU_genre: {top_EU_genre}
                  Platforms_with_a_million copies in NA: {NA_1m_sales}
                  TOP_Japan_avg_sales: {Japan_sales}
                  Games_in_EU_that_sold_better_than_in_Japan: {EU_count}''')
        
    df = get_data()
    top_sale_game = get_top_sale_game(df)
    top_EU_genre = get_top_EU_genre(df)
    NA_1m_sales = get_NA_1m_sales(df)
    Japan_sales = get_Japan_sales(df)
    EU_count = get_EU_count(df)

    print_data(top_sale_game, top_EU_genre, NA_1m_sales, Japan_sales, EU_count)

i_ishakov_22_airflow_lesson3 = i_ishakov_22_airflow_lesson3()
