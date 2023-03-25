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


top_games_data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'a-antonova') % 23

default_args = {
    'owner': 'a-antonova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 30),
    'schedule_interval': '11 11 * * *'
}

@dag(default_args=default_args, catchup=False)
def top_games_ant_ls_3():

    @task()
    def get_data():
        df = pd.read_csv(top_games_data)
        return df

    @task()
    def filter_data(df):
        df = df.dropna(subset = ['Year'])
        df.Year = df.Year.astype(int, copy = False)
        df_year = df.query('Year == @year')
        return df_year

    @task()
    def get_top_game(df_year):
        top_global = df_year.groupby('Name', as_index = False) \
                    .agg({'Global_Sales': 'sum'}) \
                    .sort_values('Global_Sales', ascending = False)
        top_global_game = top_global.Name.iloc[0]
        return {'top_game': top_global_game}

    @task()
    def get_eu_top(df_year):
        top_eu = df_year.groupby('Genre', as_index = False) \
                .agg({'EU_Sales': 'sum'}) \
                .sort_values('EU_Sales', ascending = False)
        max_eu_sales = top_eu.EU_Sales.max()
        max_eu = top_eu.query('EU_Sales == @max_eu_sales').Genre.tolist()
        return {'top_game_eu': max_eu}
    
    @task()
    def get_na_top(df_year):
        na_top = df_year.query('NA_Sales > 1') \
                .groupby('Platform', as_index = False) \
                .agg({'Name': 'count'}) \
                .sort_values('Name', ascending = False)
        max_na_sales = na_top.Name.max()
        max_na = na_top.query('Name == @max_na_sales').Platform.tolist()
        return {'top_sales_na': max_na}
    
    @task()
    def get_jp_top(df_year):
        top_jp = df_year.groupby('Publisher', as_index = False) \
                .agg({'JP_Sales': 'mean'}) \
                .sort_values('JP_Sales', ascending = False)
        max_jp_sales = top_jp.JP_Sales.max()
        max_jp = top_jp.query('JP_Sales == @max_jp_sales').Publisher.tolist()
        return {'top_pub_jp': max_jp}
    
    @task()
    def get_jp_eu(df_year):
        game_sales = df_year.groupby('Name', as_index = False) \
                    .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        eu_vs_jp = len(game_sales.query('EU_Sales > JP_Sales'))
        return {'eu_jp_sales': eu_vs_jp}
    

    @task()
    def print_data(q1, q2, q3, q4, q5):

        context = get_current_context()
        

        top_game = q1['top_game']
        top_game_eu = q2['top_game_eu']
        top_sales_na = q3['top_sales_na']
        top_pub_jp = q4['top_pub_jp']
        eu_jp_sales = q5['eu_jp_sales']

        print(f'''Most popular game in {year}:
                  {top_game}''')

        print(f'''Most popular genres in Europe in {year}:
                  {top_game_eu}''')
        
        print(f'''Top sales in North America in {year}:
                  {top_sales_na}''')
        
        print(f'''Top publisher in Japan in {year}:
                  {top_pub_jp}''')
        
        print(f'''Games with higher sales in EU in {year}:
                  {eu_jp_sales}''')

    df = get_data()
    df_year = filter_data(df)
    q_1 = get_top_game(df_year)
    q_2 = get_eu_top(df_year)
    q_3 = get_na_top(df_year)
    q_4 = get_jp_top(df_year)
    q_5 = get_jp_eu(df_year)

    print_data(q_1, q_2, q_3, q_4, q_5)

top_games_ant_ls_3 = top_games_ant_ls_3()
