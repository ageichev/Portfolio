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

my_year = 1994 + hash(f'{"m-pozdeev-23"}') % 23

CHAT_ID = 451895522
try:
    BOT_TOKEN = '5393401977:AAEgVojlEsVnVLGFbgAC15xCXtFSBFPkEv4'
except:
    BOT_TOKEN = ''

default_args = {
    'owner': 'm-pozdeev-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 4),
    'schedule_interval': '0 18 * * *'
}


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
def m_pozdeev_23_lesson_3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(vgsales)
        df = df.query('Year == @my_year')
        return df.to_csv(index = False)

    @task()
    def top_sales(df):
        df = pd.read_csv(StringIO(df))
        top_sales_my_year = df.sort_values('Global_Sales', ascending=False) \
        .head(1).Name.to_list()[0]
        return top_sales_my_year

    @task()
    def top_EU_Genres(df):
        df = pd.read_csv(StringIO(df))
        top_EU_Genre_Sales = float(df.groupby('Genre') \
                         .agg({'EU_Sales' : 'sum'}) \
                         .sort_values('EU_Sales', ascending=False) \
                         .head(1).EU_Sales)
        top_EU_Genres = df.groupby('Genre') \
                         .agg({'EU_Sales' : 'sum'}) \
                         .sort_values('EU_Sales', ascending=False) \
                         .query('EU_Sales == @top_EU_Genre_Sales')
        return top_EU_Genres

    @task()
    def top_NA_Platforms(df):
        df = pd.read_csv(StringIO(df))
        top_NA_Platform_Sales = float(df.query('NA_Sales > 1.0') \
                                      .groupby('Platform').agg({'NA_Sales': 'sum'}) \
                                      .sort_values('NA_Sales', ascending=False) \
                                      .head(1).NA_Sales)
        top_NA_Platforms = df.query('NA_Sales > 1.0') \
                           .groupby('Platform').agg({'NA_Sales': 'sum'}) \
                           .sort_values('NA_Sales', ascending=False) \
                           .query('NA_Sales == @top_NA_Platform_Sales')
        return top_NA_Platforms

    @task()
    def mean_JP_Sales(df):
        df = pd.read_csv(StringIO(df))
        mean_JP_Sales = float(df.groupby('Publisher', as_index=False).agg({'JP_Sales' : 'mean'}) \
                        .sort_values('JP_Sales', ascending = False) \
                        .head(1).JP_Sales)
        top_mean_JP_Sales = df.groupby('Publisher', as_index=False).agg({'JP_Sales' : 'mean'}) \
                        .sort_values('JP_Sales', ascending = False) \
                        .query('JP_Sales == @mean_JP_Sales')
        return top_mean_JP_Sales

    @task()
    def better_in_EU(df):
        df = pd.read_csv(StringIO(df))
        Better_in_EU = df.query('EU_Sales > JP_Sales').Name.count()
        return Better_in_EU

    @task(on_success_callback=send_message)
    def log(sales, top_EU, top_NA, mean_JP, better_EU):

        context = get_current_context()
        date = context['ds']

        print('My year is:', my_year)
        print('Top sales of the year:', sales, '\n')
        print('Top sales in EU (genre):', top_EU, '\n')
        print('Top platforms in NA:', top_NA, '\n')
        print('Best mean sales in JP (publisher):', mean_JP, '\n')
        print('Better saled in EU, than JP (number):', better_EU)

    df = get_data()
    sales = top_sales(df)
    top_EU = top_EU_Genres(df)
    top_NA = top_NA_Platforms(df)
    mean_JP = mean_JP_Sales(df)
    better_EU = better_in_EU(df)
    log(sales, top_EU, top_NA, mean_JP, better_EU)

m_pozdeev_23_lesson_3 = m_pozdeev_23_lesson_3()







