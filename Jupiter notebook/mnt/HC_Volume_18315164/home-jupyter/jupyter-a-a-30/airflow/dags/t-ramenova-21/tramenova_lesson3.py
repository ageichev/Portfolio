import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import json
from urllib.parse import urlencode

DATA_LOCAL= "https://gist.githubusercontent.com/zhonglism/f146a9423e2c975de8d03c26451f841e/raw/f79e190df4225caed58bf360d8e20a9fa872b4ac/vgsales.csv" 

CHAT_ID = 515022905
BOT_TOKEN = '5268572096:AAHWkxrLPn9bhbLkjpN5jGO3r0247x_oSQU'


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    params = {'chat_id': CHAT_ID, 'text': message}
    base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    resp = requests.get(url)
    

default_args = {
    'owner': 't.ramenova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 30),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def tramenova_task3():
    @task(retries=3)
    def get_data(my_year):
        sales    = pd.read_csv(DATA_LOCAL)
        sales    = sales[sales.Year == my_year]
        vgsales  = sales.to_csv(index=False)
        return (vgsales)
     
    @task()
    def get_best_sell_game(vgsales):
        df_vgsales =  pd.read_csv(StringIO(vgsales))
        best_sell_game = df_vgsales.sort_values('Global_Sales', ascending=False)['Name'].values[0]
        return(best_sell_game)
    
    @task()
    def get_EU_best_sell_genre(vgsales):
        df_vgsales =  pd.read_csv(StringIO(vgsales))
        EU_best_sell_genre = df_vgsales.groupby('Genre', as_index = False)\
                            .agg({'EU_Sales':'sum'})
        max_sales = EU_best_sell_genre.EU_Sales.max()
        EU_best_sell_genre = EU_best_sell_genre[EU_best_sell_genre.EU_Sales == max_sales]['Genre'].values
        EU_best_sell_genre = ", ".join(map(str,EU_best_sell_genre))
        return (EU_best_sell_genre)
    
    @task()
    def get_platform_for_NA(vgsales):
        df_vgsales =  pd.read_csv(StringIO(vgsales))
        df_vgsales =  df_vgsales[df_vgsales.NA_Sales >=1]
        best_platform_for_NA = df_vgsales.groupby('Platform', as_index = False)\
                            .agg({'Name':'count'})\
                            .sort_values('Name', ascending = False)
        max_platform_games = best_platform_for_NA.Name.max()
        best_platform_for_NA = best_platform_for_NA[best_platform_for_NA.Name == max_platform_games]['Platform'].values
        best_platform_for_NA = ", ".join(map(str,best_platform_for_NA))
        return(best_platform_for_NA)
    
    @task()
    def get_max_JP_sales_publisher(vgsales):
        df_vgsales =  pd.read_csv(StringIO(vgsales))
        max_JP_sales_publisher = df_vgsales.groupby('Publisher', as_index = False)\
                                .agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending = False)
        max_meansales =  max_JP_sales_publisher.JP_Sales.max()
        max_JP_sales_publisher = max_JP_sales_publisher[max_JP_sales_publisher.JP_Sales == max_meansales]['Publisher'].values
        max_JP_sales_publisher = ", ".join(map(str,max_JP_sales_publisher))
        return(max_JP_sales_publisher)
    
    @task()
    def get_games_EU_better_JP(vgsales):
        df_vgsales =  pd.read_csv(StringIO(vgsales))
        games_count =  df_vgsales[ df_vgsales.EU_Sales >  df_vgsales.JP_Sales].Name.count()
        return (games_count) 
    
    @task(on_success_callback=send_message)
    def print_data(bs_game, UE_bs_genre, NA_bs_platform, JP_publ, EU_vs_JP_games,my_year):
        context = get_current_context()
        print (f'Best selling game of the {my_year} year worldwide is {bs_game}')
        print (f'Top selling genre in {my_year} in Europe is {UE_bs_genre}')
        print (f'Platform with the most games sold over a million in North America in {my_year} is {NA_bs_platform}')
        print (f'Publisher with the highest sales in japan in {my_year} is {JP_publ}')
        print (f'Number of games that sold better in Europe than in Japan in {my_year} is {EU_vs_JP_games}')
        
        
    login = 't-ramenova-21'
    my_year = 1994 + hash(f'{login}') % 23
    vgsales =  get_data(my_year)
    
    best_sell_game         = get_best_sell_game(vgsales)
    EU_best_sell_genre     = get_EU_best_sell_genre(vgsales)
    platform_for_NA        = get_platform_for_NA(vgsales)
    max_JP_sales_publisher = get_max_JP_sales_publisher(vgsales)
    games_EU_better_JP     = get_games_EU_better_JP(vgsales)
    
    print_data(best_sell_game, EU_best_sell_genre, platform_for_NA, max_JP_sales_publisher, games_EU_better_JP, my_year)
        
        
tramenova_task3 = tramenova_task3()
   