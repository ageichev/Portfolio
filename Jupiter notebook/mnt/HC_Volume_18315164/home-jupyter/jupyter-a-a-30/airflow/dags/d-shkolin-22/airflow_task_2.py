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

path='/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year=1994+hash(f'd-shkolin-22')%23
data=pd.read_csv(path)

default_args = {
    'owner': 'd-shkolin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 23),
    'schedule_interval': '0 23 * * *'}

CHAT_ID=-151259664
try:
    BOT_TOKEN=Variable.get('telegram_secret')
except:
    BOT_TOKEN=''
def send_message(context):
    date=context['ds']
    dag_id=context['dag'].dag_id
    mmessage=f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN!='':
        bot=telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID,text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def airflow_shkolin_2():
    @task(retries=3)
    def get_data():
        data=pd.read_csv(path)
        return data

    @task(retries=2, retry_delay=timedelta(10))
    def function_top_sold(data):
        top_sold=list(data.groupby(['Name'],as_index=False)\
        .agg({'Global_Sales':'sum'})\
        .sort_values(by='Global_Sales',ascending=False)\
        .head(1)['Name'])[0]
        return top_sold

    @task()
    def function_top_genre(data):
        top_genre=data.groupby(['Genre'],as_index=False)\
        .agg({'EU_Sales':'sum'})\
        .sort_values(by='EU_Sales', ascending=False)
        top_sales=list(top_genre.head(1)['EU_Sales'])[0]
        top_genre=top_genre.query('EU_Sales==@top_sales')
        top_genre=top_genre['Genre'].to_list()
        return top_genre
        
    @task()
    def function_top_platforms(data):
        top_platforms=data.query('NA_Sales>1')\
        .groupby(['Platform'],as_index=False).agg({'NA_Sales':'count'})\
        .sort_values(by='NA_Sales',ascending=False)
        top_platforms=top_platforms['Platform'].to_list()
        return top_platforms

    @task()
    def function_top_japan(data):
        top_japan=data.groupby(['Publisher'],as_index=False)\
        .agg({'JP_Sales':'mean'})\
        .sort_values(by='JP_Sales',ascending=False)
        top_japan_average=list(top_japan.head(1)['JP_Sales'])[0]
        top_japan=top_japan.query('JP_Sales==@top_japan_average')
        top_japan=top_japan['Publisher'].to_list()
        return top_japan
    
    @task()
    def function_evsj(data):
        evsj=len(data.query('EU_Sales>JP_Sales'))
        return evsj

    @task(on_success_callback=send_message)
    def print_data(function_top_sold, function_top_genre,\
                   function_top_platforms,function_top_japan,\
                  function_evsj):
        context=get_current_context()
        date=context['ds']
        print('Game most sold: {}'.format(top_sold))
        print('Top genres in Europe: {}'.format(top_genre))
        print('Top NA platforms: {}'.format(top_platforms))
        print('Top Japan pubslisher {}'.format(top_japan))
        print('The nummber of games better sold in EU than in Japan: {}'.format(evsj))
    
    data=get_data()
    top_sold=function_top_sold(data)
    top_genre=function_top_genre(data)
    top_platforms=function_top_platforms(data)
    top_japan=function_top_japan(data)
    evsj=function_evsj(data)
    print_data(top_sold, top_genre,top_platforms,top_japan,evsj)                          
    
airflow_shkolin_2=airflow_shkolin_2()   

