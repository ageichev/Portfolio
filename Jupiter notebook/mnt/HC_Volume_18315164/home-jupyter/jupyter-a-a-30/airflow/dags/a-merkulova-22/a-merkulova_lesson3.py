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

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'{"a-merkulova-22"}') % 23

default_args = {
    'owner': 'a.merkulova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 8),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 413978302
try:
    BOT_TOKEN = '5225557645:AAEvGGsHOb-NW8qYITDC8t2atS3FsV9DsdY'
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def a_merkulova_22_lesson_3():
    
    @task()
    def get_data():
        games = pd.read_csv(vgsales).query('Year == @year')
        return games

    @task()
    def get_best_seller(games):
        best_seller = games.groupby('Name').agg({'Global_Sales': 'sum'}).Global_Sales.idxmax()
        return best_seller

    @task()
    def get_best_EU(games):
        best_EU = games.groupby('Genre').agg({'EU_Sales': 'sum'}).EU_Sales.idxmax()
        return best_EU

    @task()
    def get_best_platform(games):
        best_platform = games.query('NA_Sales > 1').groupby('Platform').agg({'Name': 'count'}).Name.idxmax()
        return best_platform
        
    @task()
    def get_best_publisher(games):
        best_publisher = games.groupby('Publisher').agg({'JP_Sales': 'mean'}).JP_Sales.idxmax()
        return best_publisher
        
    @task()
    def get_best_EU_JP(games):
        best_EU_JP = games.query('EU_Sales > JP_Sales').shape[0]
        return best_EU_JP


    @task(on_success_callback=send_message)
    def print_data(best_seller, best_EU, best_platform, best_publisher, best_EU_JP):

        context = get_current_context()

        print(f'The best selling game in {year} worldwide is {best_seller}')
        
        print(f'The best selling game genre in Europe in {year} is {best_EU}')
        
        print(f'The platform that had the most games that sold over a million units in North America in {year} is  {best_platform}')
        
        print(f'The publisher with the highest average sales in Japan in {year} is {best_publisher}')
            
        print(f'Number of games sold better in Europe than in Japan in {year} - {best_EU_JP}') 
        
        
        context = get_current_context()
        date = context['ds']

    games = get_data()
    
    best_seller = get_best_seller(games)
    best_EU = get_best_EU(games)
    best_platform = get_best_platform(games)
    best_publisher = get_best_publisher(games)
    best_EU_JP = get_best_EU_JP(games)

    print_data(best_seller, best_EU, best_platform, best_publisher, best_EU_JP)

a_merkulova_22_lesson_3 = a_merkulova_22_lesson_3()