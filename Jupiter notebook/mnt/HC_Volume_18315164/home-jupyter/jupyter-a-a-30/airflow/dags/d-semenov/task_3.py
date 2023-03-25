import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
import numpy as np
from airflow.models import Variable


default_args = {
    'owner': 'd-semenov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 1),
    'schedule_interval': '40 21 * * *'
}


CHAT_ID = -626204802
BOT_TOKEN = Variable.get('telegram_secret')

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f"Well Done! Dag {dag_id} completed on {date}. It's fine."
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)


@dag(default_args=default_args, catchup=False)
def d_semenov_task_3():
    # Получаем данные
    @task()
    def get_data():
        path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        year = 1994 + hash('d-semenov') % 23
        df = pd.read_csv(path)
        data = df.query("Year == @year")
        return data

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_World_top_1_game(data):
        World_top_1_game = data.head(1)['Name']
        return World_top_1_game

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_World_top_1_genre(data):
        EU_Sale_top = data.groupby('Genre', as_index=False).agg({'EU_Sales': 'mean'}).sort_values('EU_Sales', ascending=False)
        TOP_EU_Genre = EU_Sale_top.query('EU_Sales == @EU_Sale_top.EU_Sales.max()')['Genre']
        return TOP_EU_Genre

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task()
    def get_platform_1m_copies(data):
        TOP_Platform_NA = data.query('NA_Sales > 1').groupby("Platform", as_index=False).agg({'Name': 'count'}).sort_values('Name', ascending=False)
        Platform_1M = TOP_Platform_NA.query('Name == @TOP_Platform_NA.Name.max()')['Platform']
        return Platform_1M

    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_JP_TOP_1_Publisher(data):
        Top_Publisher_JP = data.groupby("Publisher", as_index=False).agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False)
        TOP_1_Publisher_JP = Top_Publisher_JP.query('JP_Sales == @Top_Publisher_JP.JP_Sales.max()')['Publisher']
        return TOP_1_Publisher_JP

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_count_games_EU_than_JP(data):
        Number_EU_better_than_JP = data.query('EU_Sales > JP_Sales').shape[0]
        return Number_EU_better_than_JP


    @task(on_success_callback=send_message)
    def print_data(World_top_1_game, TOP_EU_Genre, Platform_1M, TOP_1_Publisher_JP, Number_EU_better_than_JP): # передаем переменные с прошлых функций
            context = get_current_context()
            date = context['ds']
            year = 1994 + hash('d-semenov') % 23

            print(f'The most popular game in {year} year for {date}')
            print(World_top_1_game)

            print(f'The most popular genre(s) in {year} year for {date}')
            print(TOP_EU_Genre)

            print(f'The most popular platform(s) with more than 1M copies in NA in {year} year for {date}')
            print(Platform_1M)

            print(f'The most popular Publisher(s) with best sales in Japan in {year} year for {date}')
            print(TOP_1_Publisher_JP)

            print(f'Number of games with better sales in Europe than in Japan in {year} year for {date}')
            print(Number_EU_better_than_JP)
        
    data = get_data()
    World_top_1_game = get_World_top_1_game(data)
    TOP_EU_Genre = get_World_top_1_genre(data)
    Platform_1M = get_platform_1m_copies(data)
    TOP_1_Publisher_JP = get_JP_TOP_1_Publisher(data)
    Number_EU_better_than_JP = get_count_games_EU_than_JP(data)
    print_data(World_top_1_game, TOP_EU_Genre, Platform_1M, TOP_1_Publisher_JP, Number_EU_better_than_JP)

d_semenov_task_3 = d_semenov_task_3()