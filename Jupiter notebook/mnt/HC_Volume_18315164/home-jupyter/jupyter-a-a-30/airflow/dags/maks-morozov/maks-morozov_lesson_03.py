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

df_0 = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'maks-morozov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 27),
    'schedule_interval': '0 12 * * *'
}

year = 1994 + hash(f'maks-morozov') % 23

CHAT_ID = 400105923
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Done! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        print('Бот протестирован, токен удалён')


@dag(default_args=default_args, catchup=False)
def dag_maks_morozov():
    
    @task()
    def df_0_maks_morozov_1995():
        df_0_1995 = pd.read_csv(df_0)
        df_0_1995 = df_0_1995.fillna(0)
        df_0_1995['Name'] = df_0_1995['Name'].drop_duplicates()
        df_0_1995 = df_0_1995.query('Year == @year')
        return df_0_1995

    @task()
    def df_1_maks_morozov(df_0_1995):
        df_1 = df_0_1995
        df_1 = df_1.groupby('Name').agg({'Global_Sales':'sum'}).sort_values('Global_Sales', ascending = False).idxmax()[0]
        return df_1

    @task()
    def df_2_maks_morozov(df_0_1995):
        df_2 = df_0_1995
        df_2 = df_2.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'})
        max_df_2 = df_2.EU_Sales.max()
        df_2 = df_2.query('EU_Sales == @max_df_2').Genre.tolist()
        return df_2

    @task()
    def df_3_maks_morozov(df_0_1995):
        df_3 = df_0_1995
        df_3 = df_3.query('NA_Sales >= 1').groupby('Platform', as_index=False).agg({'Name':'count'})
        max_df_3 = df_3.Name.max()
        df_3 = df_3.query('Name == @max_df_3').Platform.tolist()
        return df_3
    
    @task()
    def df_4_maks_morozov(df_0_1995):
        df_4 = df_0_1995
        df_4 = df_4.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'})
        max_df_4 = df_4.JP_Sales.max()
        df_4 = df_4.query('JP_Sales == @max_df_4').Publisher.tolist()
        return df_4
    
    @task()
    def df_5_maks_morozov(df_0_1995):
        df_5 = df_0_1995
        df_5 = df_5.query('EU_Sales > JP_Sales')
        df_5 = df_5.Name.count()
        return df_5   

    @task(on_success_callback=send_message)
    def print_maks_morozov(df_1_end, df_2_end, df_3_end, df_4_end, df_5_end):

        print(f'''The best-selling game in {year}: {df_1_end}
                  Genre of the best-selling game in EU in {year}: {df_2_end}
                  The platform that has sold the most games in NA with a million copies or more in {year}: {df_3_end}
                  The publisher with the highest average sales in JP in {year}: {df_4_end}
                  The number of games that sold better in EU than in JP in {year}: {df_5_end}''')

    df_0_1995 = df_0_maks_morozov_1995()
    
    df_1_end = df_1_maks_morozov(df_0_1995)    
    df_2_end = df_2_maks_morozov(df_0_1995)    
    df_3_end = df_3_maks_morozov(df_0_1995)    
    df_4_end = df_4_maks_morozov(df_0_1995)    
    df_5_end = df_5_maks_morozov(df_0_1995)
    
    print_maks_morozov(df_1_end, df_2_end, df_3_end, df_4_end, df_5_end)

dag_maks_morozov = dag_maks_morozov()