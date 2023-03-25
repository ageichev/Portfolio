#!/usr/bin/env python
# coding: utf-8

# In[108]:
import json
from urllib.parse import urlencode
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram
from airflow.decorators import dag, task #импорт декораторов, штуки позволяющие по другому дизайнить даги
from airflow.operators.python import get_current_context #библа позволяющая подключить глобальные переменные аирфлоу, типа даты
from airflow.models import Variable 


CHAT_ID = -662838083
BOT_TOKEN = '5492518498:AAG11s6Cshh4LtEzPtlUZp9cOBFkwPaEMcY'


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'DAG {dag_id} completed on {date}. Success!!!'
    params = {'chat_id': CHAT_ID, 'text': message}
    base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    resp = requests.get(url)

default_args = {
    'owner': 'v-maslov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 16),
    'schedule_interval': '0 12 * * *'
}

iyear = 1994 + hash(f'v-maslov-20') % 23


# In[109]:


@dag(default_args=default_args, catchup=False)
def maslov_my_dag():
    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        return df
    
    @task()
    def best_game(df):
        bestgame = list(df.query("Year == @iyear").groupby('Name', as_index = False)['Global_Sales'].max()        .sort_values('Global_Sales', ascending = False).head(1)['Name'].to_dict().values())[0] 
        return bestgame
        #самая продаваемая игра в 2012м году в мире.
        
    @task()
    def best_genre(df):
        bestgenre = list(df.query("Year == @iyear").groupby('Genre', as_index = False)['EU_Sales'].max().sort_values('EU_Sales', ascending = False)        .head(1)['Genre'].to_dict().values())[0]
        return bestgenre
        #самый продаваемый жанр игр в Европе
        
    @task()
    def best_platform(df):
        bestplatform = list(df.query("Year == @iyear & NA_Sales >= 1").groupby('Platform', as_index = False)['Name'].count().sort_values('Name',ascending = False).head(1)['Platform']        .to_dict().values())[0]
        return bestplatform
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    
    @task()
    def jp_sales(df):
        jpsales = list(df.query("Year == @iyear").groupby("Publisher", as_index = False).agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending = False)        .head(1)['Publisher'].to_dict().values())[0]
        return jpsales
    #У какого издателя самые высокие средние продажи в Японии?
    
    @task()
    def eu_jp(df):
        eudf = df.query("Year == @iyear").groupby('Name', as_index = False).agg({'JP_Sales':'sum', 'EU_Sales':'sum'})
        eudf['EUmore'] = eudf.apply(lambda x: 1 if x.EU_Sales > x.JP_Sales else 0, axis = 1)
        eujp=eudf.EUmore.sum()
        return eujp
    #Сколько игр продались лучше в Европе, чем в Японии?
    
    @task(on_success_callback=send_message)
    def print_data(bestgame, bestgenre, bestplatform, jpsales, eujp):
        print(f'''Для года: {iyear}
    Самая продаваемая игра: {bestgame},
    Самый продаваемый жанр игр в Европе: {bestgenre},
    На платформе {bestplatform} было продано больше всего игр с более чем миллионым тиражом в Северной Америке,
    У издателя {jpsales} самые высокие средние продажи,
    {eujp} игр продались лучше в Европе, чем в Японии.''')
        
    mydata = get_data()
    bestgame = best_game(mydata)
    bestgenre = best_genre(mydata)
    bestplatform = best_platform(mydata)
    jpsales = jp_sales(mydata)
    eujp = eu_jp(mydata)
    
    print_data(bestgame, bestgenre, bestplatform, jpsales, eujp)

maslov_my_dag = maslov_my_dag()

