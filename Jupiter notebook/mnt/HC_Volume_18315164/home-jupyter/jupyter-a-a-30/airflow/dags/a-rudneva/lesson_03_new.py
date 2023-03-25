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

TOP_1M_DOMAINS = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
TOP_1M_DOMAINS_FILE = 'vgsales.csv'

default_args = {
    'owner': 'a-rudneva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 27),
}
schedule_interval = '0 12 * * *'

CHAT_ID = 884968080
try:
    BOT_TOKEN = '5899731109:AAHYBqV0gKm-QRmfe__oXz2066Ia3C_B4PY'
except:
    BOT_TOKEN = ''


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, message=message)
    else:
        pass

@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def rudneva_airflow_3():
    @task(retries=3)
    def get_data():
        # Получаем данные и фильтруем год для получения статистики
        top_doms = pd.read_csv(TOP_1M_DOMAINS)
        top_doms = top_doms.dropna()
        top_doms['Year'] = top_doms['Year'].astype(int)
        year = 1994 + hash(f"{'a-rudneva'}") % 23
        top_doms = top_doms[top_doms.Year == year]
        top_data = top_doms.to_csv(index=False)
        return top_data

    @task(retries=4, retry_delay=timedelta(10))
    #Какая игра была самой продаваемой в этом году во всем мире?
    def get_top_game(top_data):
        top_data_df = pd.read_csv(StringIO(top_data))
        top_game = top_data_df.sort_values('Global_Sales', ascending=False)
        max = top_game.reset_index().loc[0, 'Name']
        top_game = top_game[top_game.Global_Sales == max]
        return top_game

    @task()
    #Игры какого жанра были самыми продаваемыми в Европе?
    def get_EU_Genre(top_data):
        df = pd.read_csv(StringIO(top_data))
        top_EU_Genre = df.groupby('Genre',as_index=False)\
            .agg({'EU_Sales':'sum'})\
            .sort_values('EU_Sales', ascending=False)
        max = top_EU_Genre.reset_index().loc[0, 'EU_Sales']
        top_EU_Genre = top_EU_Genre[top_EU_Genre['EU_Sales']== max]
        return top_EU_Genre

    @task()
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def get_million_platforms(top_data):
        top_data_df = pd.read_csv(StringIO(top_data))
        d3 = top_data_df[top_data_df.NA_Sales > 1]
        d3 = d3.groupby('Platform', as_index=False).agg({'Name':'count'}).sort_values('Name', ascending=False)
        max = d3.reset_index().loc[0,'Name']
        top_Platform = d3[d3.Name == max]
        return top_Platform

    @task()
    #У какого издателя самые высокие средние продажи в Японии?
    def get_JP_Publisher(top_data):
        df = pd.read_csv(StringIO(top_data))
        df = df.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending=False)
        max = df.reset_index().loc[0,'JP_Sales']
        top_JP_Publisher = df[df.JP_Sales == max]
        return top_JP_Publisher

    @task()
    #Сколько игр продались лучше в Европе, чем в Японии
    def get_EU_better_JP(top_data):
        df = pd.read_csv(StringIO(top_data))
        d5 = df[df.EU_Sales>df.JP_Sales]
        EU_better_JP = d5.shape[0]
        return EU_better_JP

    @task(on_success_callback=send_message)
    def print_data(top_game, top_EU_Genre, top_Platform_NA, top_JP_Publisher, EU_better_JP):

        context = get_current_context()
        date = context['ds']
        print(f'''Data for {date}
                  Top game: {top_game}
                  Top genre in EU: {top_EU_Genre}
                  Top million platform in NA: {top_Platform_NA}
                  Top JP publisher by avg sales: {top_JP_Publisher}
                  Games with sales in EU bigger then in JP: {EU_better_JP}''')

    top_data = get_data()
    top_game = get_top_game(top_data)
    top_EU_Genre = get_EU_Genre(top_data)
    top_Platform_NA = get_million_platforms(top_data)
    top_JP_Publisher = get_JP_Publisher(top_data)
    EU_better_JP = get_EU_better_JP(top_data)

    print_data(top_game, top_EU_Genre, top_Platform_NA, top_JP_Publisher, EU_better_JP)

rudneva_airflow_3 = rudneva_airflow_3()