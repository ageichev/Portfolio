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

# TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
GAMES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
# GAMES = '/mnt/HC_Volume_18315164/home-jupyter/jupyter-d-gatiatullin/airflow/dags/d-gatiatullin/vgsales.csv'
# TOP_1M_DOMAINS_FILE = 'top-1m.csv'


default_args = {
    'owner': 'd.gatiatullin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 11, 24),
    'schedule_interval': '@daily'
    
}

year_start = 1994 + hash(f'd.gatiatullin') % 23


@dag(default_args=default_args, catchup=False)
def dgati_dag2():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(GAMES)
        df = df.query("Year== @year_start")
        return df
    
    @task()
    def top_sales(df):
        top_sales_game = df.groupby('Name', as_index=False) \
                           .agg({'Global_Sales': 'sum'}) \
                           .sort_values('Global_Sales', ascending=False) \
                           .head(1)['Name'].to_list()
        top_sales_game = ', '.join(str(e) for e in top_sales_game)
        return {'top_sales_res': top_sales_game}
    
    @task
    def top_sales_genres_eu(df):
        df2 = df.groupby('Genre', as_index=False) \
                .agg({'EU_Sales': 'sum'})
        top_sales_genres_eu_list = df2[df2['EU_Sales'] == df2['EU_Sales'].max()].Genre.to_list()
        top_sales_genres_eu_list = ', '.join(str(e) for e in top_sales_genres_eu_list)
        return {'top_sales_genres_eu_res': top_sales_genres_eu_list}
    
    @task
    def top_sales_platform(df):
        df3 = df.query("NA_Sales > 1") \
                .groupby('Platform', as_index=False) \
                .agg({'Name': 'count'}) \
                .rename(columns={'Name': 'Count'})
        top_sales_platform_list = df3[df3['Count'] == df3['Count'].max()].Platform.to_list()
        top_sales_platform_list = ', '.join(str(e) for e in top_sales_platform_list)

        return {'top_sales_platform_res': top_sales_platform_list}
    
    @task()
    def top_avg_sales_jp(df):
        df4 = df.groupby('Publisher', as_index=False) \
                .agg({'JP_Sales': 'mean'}) \
                .rename(columns={'JP_Sales': 'JP_Sales_mean'})
        top_avg_sales_jp_list = df4[df4['JP_Sales_mean'] == df4['JP_Sales_mean'].max()].Publisher.to_list()
        top_avg_sales_jp_list = ', '.join(str(e) for e in top_avg_sales_jp_list)
        return {'top_avg_sales_jp_res': top_avg_sales_jp_list}
    
    @task()
    def eu_jp(df):
        eu_jp_res = df.groupby('Name', as_index=False) \
                      .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}) \
                      .query("EU_Sales > JP_Sales") \
                      .shape[0]
        return {'eu_jp_res': eu_jp_res}

    
    @task()
    def print_data(a, b, c, d, e):
        first_answer = a['top_sales_res']
        second_answer = b['top_sales_genres_eu_res']
        third_answer = c['top_sales_platform_res']
        fourth_answer = d['top_avg_sales_jp_res']
        fifth_answer = e['eu_jp_res']
        
        print(f'Best-selling game in {year_start} is {first_answer}')
        print(f'List of best-selling genres in Europe: {second_answer}')
        print(f'List of platforms with more than a million copies in NA: {third_answer}')
        print(f'List of publishers with the highest average sales in Japan: {fourth_answer}')
        print(f'Number of games that sold better in Europe than in Japan: {fifth_answer}')
    

    df = get_data()
    aa = top_sales(df)
    bb = top_sales_genres_eu(df)
    cc = top_sales_platform(df)
    dd = top_avg_sales_jp(df)
    ee = eu_jp(df)

    print_data(aa, bb, cc, dd, ee)

dgati_dag2 = dgati_dag2()
