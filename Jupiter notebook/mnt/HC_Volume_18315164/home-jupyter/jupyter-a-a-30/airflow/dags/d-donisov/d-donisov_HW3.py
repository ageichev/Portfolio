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

TOP_1M_DOMAINS_FILE = 'vgsales.csv'
year= 1994 + hash(f'd-donisov') % 23

default_args = {
    'owner': 'd-donisov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 5),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = -620798068
try:
    BOT_TOKEN = Variable.get('telegram_secret')
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
def DAG_ddonisov_2():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(TOP_1M_DOMAINS_FILE).query('Year == @year')
        return df
    
    @task()
    def get_top_game_Global(df):
        top_game_Global = df.groupby('Name')\
                .agg({'Global_Sales':'sum'})\
                .Global_Sales.idxmax()
        return {'top_game_Global' : top_game_Global}
    
    @task()
    def get_top_ganres_EU(df):
        quantile75 = int(df.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'}).quantile(.75))
        top_ganres_EU = df.groupby('Genre')\
                .agg({'EU_Sales': 'sum'})\
                .query('EU_Sales > @quantile75')\
                .sort_values('EU_Sales', ascending=False)\
                .index.tolist()
        return {'top_ganres_EU' : top_ganres_EU}
    
    @task()
    def get_top_platforms_NA(df):
        top_platforms_NA = df.query('NA_Sales > 1')\
                .groupby('Platform')\
                .agg({'Name':'count'})\
                .sort_values('Name', ascending=False)\
                .index.tolist()
        return {'top_platforms_NA' : top_platforms_NA}
    
    @task()
    def get_top_publisher_JP(df):
        top_publisher_JP = df.groupby('Publisher')\
                .agg({'JP_Sales':'mean'})\
                .JP_Sales.idxmax()
        return {'top_publisher_JP' : top_publisher_JP}
    
    @task()
    def get_EUvsJP(df):
        EUvsJP = df.groupby('Name')\
                .agg({'EU_Sales': 'sum', 'JP_Sales':'sum'})\
                .query('EU_Sales > JP_Sales')\
                .shape[0]
        return {'EUvsJP' : EUvsJP}
    
    @task(on_success_callback=send_message)
    def print_data(t2, t3, t4, t5, t6):
        
        context = get_current_context()
        date = context['ds']
        
        game_Global, ganres_EU, platforms_NA = t2['top_game_Global'], t3['top_ganres_EU'], t4['top_platforms_NA']
        publisher_JP, EU_JP = t5['top_publisher_JP'], t6['EUvsJP']
        
        print(f'''Данные за {year} год:
                  Самая продоваемая игра в Мире: {game_Global}
                  Жанры самых продоваемых игр в Европе: {ganres_EU}
                  Платформы самых продоваемых игр тиражом свыше 1М в Северной Америке: {platforms_NA}
                  Издатель с самыми высокими средними продажами в Японии: {publisher_JP}
                  Количество игр, которое продалось лучше в Европе, чем в Японии: {EU_JP}''')
        
    df = get_data()
    t2 = get_top_game_Global(df)
    t3 = get_top_ganres_EU(df)
    t4 = get_top_platforms_NA(df)
    t5 = get_top_publisher_JP(df)
    t6 = get_EUvsJP(df)
    
    print_data(t2, t3, t4, t5, t6)
    
DAG_ddonisov_2 = DAG_ddonisov_2()