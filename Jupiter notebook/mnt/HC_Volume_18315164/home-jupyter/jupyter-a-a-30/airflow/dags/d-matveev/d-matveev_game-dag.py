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



default_args = {
    'owner': 'd-matveev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 17),
    'schedule_interval': '0 12 * * *'}



@dag(default_args=default_args, catchup=False)
def d_matveev_game_dag():
    
    @task()
    def get_data():
        year = 1994 + hash(f'{"d-matveev"}') % 23
        data = pd.read_csv('vgsales.csv').query("Year == @year")
        return data

    @task()
    def global_sales_game(data):
        GS_game = data.sort_values(by ='Global_Sales', ascending = False)['Name'].head(1)
        return GS_game
    
    @task()
    def eu_games_sale(data):
        eu_gs = data.groupby('Genre', as_index = False)['EU_Sales'].sum().sort_values(by='EU_Sales', ascending = False)['Genre'][:3]
        return eu_gs
    
    @task()
    def NA_plaltforms(data):
        platforms = data.query('NA_Sales > 1').groupby('Platform', as_index = False).agg({'Name': 'count'}) \
.sort_values(by='Name', ascending = False)['Platform'].head(5)
        return platforms
    
    @task()
    def publisher(data):
        publ = data.groupby('Publisher', as_index = False)[['JP_Sales']].mean().sort_values('JP_Sales', ascending =False)\
['Publisher'].head(5)
        return publ
    
    @task()
    def eujp_sales(data):
        EU_JP_Sales = data.groupby('Name', as_index = False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        EU_JP_Sales['diff'] = EU_JP_Sales['EU_Sales'] - EU_JP_Sales['JP_Sales']
        c = EU_JP_Sales.query('diff > 0')['Name'].count()
        return c
   

    data = get_data()
    top_global_sales_game = global_sales_game(data)
    top_eu_games_sale = eu_games_sale(data)
    top_NA_plaltforms = NA_plaltforms(data)
    top_publisher = publisher(data)
    top_eujp_sales = eujp_sales(data)
    
    print(top_global_sales_game, top_eu_games_sale, top_NA_plaltforms, top_publisher, top_eujp_sales)

d_matveev_game_dag = d_matveev_game_dag()
