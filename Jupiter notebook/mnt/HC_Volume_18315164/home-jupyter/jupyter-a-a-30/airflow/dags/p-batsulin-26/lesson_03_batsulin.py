import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

VGSALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

year = 1994 + hash(f'p-batsulin-26') % 23


default_args = {
    'owner': 'p.batsulin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 17),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 188468759
try:
    BOT_TOKEN = '5228530540:AAHCKdcr_x_zFXvpuX5Unf5tCdfQS9naB00'
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Все успешно! Dag {dag_id} завершен {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def pbatsulin_top_games():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(VGSALES).query('Year == @year')
        return df.to_csv(index=False)


    @task(retries=2, retry_delay=timedelta(10))
    def top_sales_global(df):
        df = pd.read_csv(StringIO(df))
        top_sales = df[df.Global_Sales == df.Global_Sales.max()].values[0][1]
        return top_sales

    @task()
    def get_genre_eu(df):
        df = pd.read_csv(StringIO(df))
        top_genre_eu_sum = df.groupby('Genre').EU_Sales.sum()
        top_genre_eu = top_genre_eu_sum[top_genre_eu_sum == top_genre_eu_sum.max()].index.to_list()
        return top_genre_eu

    @task()
    def get_platform_na(df):
        df = pd.read_csv(StringIO(df))
        na_platform_count = df.query('NA_Sales > 1').groupby('Platform').Name.count()
        na_platform = na_platform_count[na_platform_count == na_platform_count.max()].index.to_list()
        return na_platform

    @task()
    def get_publisher_jp(df):
        df = pd.read_csv(StringIO(df))
        japan_publisher_mean = df.groupby('Publisher').JP_Sales.mean().sort_values(ascending=False)
        japan_publisher = japan_publisher_mean[japan_publisher_mean == japan_publisher_mean.max()].index.to_list()
        return japan_publisher
    
    @task()
    def get_eu_better_jp(df):
        df = pd.read_csv(StringIO(df))
        eu_jp_games = len(df.query('EU_Sales > JP_Sales'))
        return eu_jp_games

    @task(on_success_callback=send_message)
    def print_data(top_sales, top_genre_eu, na_platform, japan_publisher, eu_jp_games):

        context = get_current_context()
        date = context['ds']

        top_sales = top_sales
        top_genre_eu = ", ".join(top_genre_eu)
        na_platform = ", ".join(na_platform)
        japan_publisher = ", ".join(japan_publisher)
        eu_jp_games = eu_jp_games


        print(f'Самая продаваемая игра во всем мире в {year} году — {top_sales}.')
        print(f'Самыми продаваевыми играми в Европе в {year} году были игры жанра/ов {top_genre_eu}.')
        print(f'''Больше всего игр, продавшихся более чем миллионным тиражом в Северной Америке, в {year}
        году было выпущено на платформе — {na_platform}.''')
        print(f'''Издатели с самыми высокими средними продажами в Японии в {year} году — {japan_publisher}.''')
        print(f'''В {year} году {eu_jp_games} игр продавались в Европе лучше, чем в Японии.''')


    df = get_data()
    top_sales = top_sales_global(df)
    top_genre_eu = get_genre_eu(df)
    na_platform = get_platform_na(df)
    japan_publisher = get_publisher_jp(df)
    eu_jp_games = get_eu_better_jp(df)

    
    print_data(top_sales, top_genre_eu, na_platform, japan_publisher, eu_jp_games)

pbatsulin_top_games = pbatsulin_top_games()
