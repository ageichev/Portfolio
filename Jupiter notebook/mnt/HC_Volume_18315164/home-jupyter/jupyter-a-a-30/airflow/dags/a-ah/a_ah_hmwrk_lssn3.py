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

year = 1994 + hash(f'a_ah') % 23
vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a-ah',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 3),
    'schedule_interval': '0 5 * * *'
}

CHAT_ID = -54131360
try:
    BOT_TOKEN = Variable.get('5747740219:AAFjx2j-Fm2fseQKyYbkfmfevpmyH7HLdFU')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Working! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass 

@dag(default_args=default_args, catchup=False)
def a_ah_lssn3():
    @task(retries=3)
    def get_data():
        vgsales_df = pd.read_csv(vgsales)
        return vgsales_df

    @task()
    def top_sales_game(vgsales_df):
        task_1 = vgsales_df.query('Year == @year').groupby('Name',as_index = False)\
        .agg({'Global_Sales':'sum'})\
        .sort_values('Global_Sales',ascending=False).head(1)
        return task_1

    @task()
    def top_sales_genre(vgsales_df):
        task_2 = vgsales_df.query('Year == @year').groupby('Genre',as_index = False)\
        .agg({'EU_Sales':'sum'})\
        .sort_values('EU_Sales',ascending=False).head(3)
        return task_2

    @task()
    def top_NA_platform(vgsales_df):
        task_3 = vgsales_df.query('Year == @year').groupby('Platform',as_index = False)\
        .agg({'NA_Sales':'sum'})\
        .sort_values('NA_Sales',ascending=False).head(3)
        return task_3
    
    @task()
    def top_JP_publisher(vgsales_df):
        task_4 = vgsales_df.query('Year == @year').groupby('Publisher',as_index = False)\
        .agg({'JP_Sales':'mean'})\
        .sort_values('JP_Sales',ascending=False).head(3)
        return task_4
    
    @task()
    def top_EU_sales(vgsales_df):
        task_5 = vgsales_df.query('Year == 2016').groupby('Name',as_index = False)\
        .agg({'JP_Sales':'sum','EU_Sales':'sum'})
        task_5['delta'] = task_5.EU_Sales-task_5.JP_Sales
        task_5 = task_5.query('delta >0').agg({'Name':'count'})
        return task_5

    @task(on_success_callback=send_message)
    def print_data(top_sales_game, top_sales_genre,top_NA_platform, top_JP_publisher,top_EU_sales):

        context = get_current_context()
        date = context['ds']

        print(f'The top selling game in {year} worldwide')
        print (top_sales_game)
        print(f'The top selling genre in {year} in Europe is {task_2}')
        print(f'The top sales platform in {year} in Nothern America is {task_3}')
        print(f'The top sales publisher in {year} in Japan is {task_4}')
        print(f'The top count of sales games Europe vs Japan is {task_5}')        
                 
    vgsales_df = get_data()
    task_1 = top_sales_game(vgsales_df)
    task_2 = top_sales_genre(vgsales_df)
    task_3 = top_NA_platform(vgsales_df)
    task_4 = top_JP_publisher(vgsales_df)
    task_5 = top_EU_sales(vgsales_df)
 
    print_data(top_sales_game, top_sales_genre,top_NA_platform, top_JP_publisher,top_EU_sales)
a_ah_lssn3 = a_ah_lssn3()
