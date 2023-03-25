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

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'v-molahov'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'v-molahov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '30 9 * * *'
}

CHAT_ID = '392403759'
BOT_TOKEN = '5559148434:AAGK-6LfIG-T3Unlg-AHHrtJgFiqhdltF78'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def homework_3_airflow_v_molahov():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(path)
        df = df.query('Year == @year')
        return df

    @task()
    def get_top_game(df):
        top_game = df.sort_values('Global_Sales', ascending=False).head(1).Name.values[0]
        return top_game

    @task()
    def get_top_genres_in_EU(df):
        top_genres_in_EU = df.groupby('Genre', as_index=False).EU_Sales.sum().sort_values('EU_Sales', ascending=False).head(3).Genre.to_list()
        return top_genres_in_EU

    @task()
    def get_top_platform_in_NA(df):
        top_platform_in_NA = df.query('NA_Sales > 1').groupby('Platform', as_index=False).Name.nunique()\
    .sort_values('Name', ascending=False).head(1).Platform.values[0]
        return top_platform_in_NA

    @task()
    def get_top_publisher_in_JP(df):
        top_publisher_in_JP = df.groupby('Publisher', as_index=False).JP_Sales.mean()\
    .sort_values('JP_Sales', ascending=False).head(1).Publisher.values[0]
        return top_publisher_in_JP
    
    @task()
    def get_EU_vs_JP(df):
        EU_vs_JP = df.query('EU_Sales > JP_Sales').shape[0]
        return EU_vs_JP

    @task(on_success_callback=send_message)
    def print_data(top_game, top_genres_in_EU, top_platform_in_NA, top_publisher_in_JP, EU_vs_JP):

        context = get_current_context()
        date = context['ds']

        
        print(f'Top game in {year} worldwide - {top_game}')
        
        print(f'Top genres in {year} in EU - {top_genres_in_EU}')
        
        print(f'Top platform in {year} in North America - {top_platform_in_NA}')
        
        print(f'Top publisher in {year} in Japan - {top_publisher_in_JP}')
        
        print(f'Number of games with better sales in {year} in EU than in Japan  - {EU_vs_JP}')


    df = get_data()
    
    top_game = get_top_game(df)
    top_genres_in_EU = get_top_genres_in_EU(df)
    top_platform_in_NA = get_top_platform_in_NA(df)
    top_publisher_in_JP = get_top_publisher_in_JP(df)
    EU_vs_JP = get_EU_vs_JP(df)
    
    print_data(top_game, 
               top_genres_in_EU, 
               top_platform_in_NA, 
               top_publisher_in_JP, 
               EU_vs_JP)
    
    
    

homework_3_airflow_v_molahov = homework_3_airflow_v_molahov()
