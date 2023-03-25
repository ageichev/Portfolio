import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
# import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'n-redkina'

year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'n-redkina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 19)
}


# CHAT_ID = -620798068
# try:
# BOT_TOKEN = Variable.get('telegram_secret')
# except:
# BOT_TOKEN = ''

# def send_message(context):
# date = context['ds']
# dag_id = context['dag'].dag_id
# message = f'Huge success! Dag {dag_id} completed on {date}'
# if BOT_TOKEN != '':
# bot = telegram.Bot(token=BOT_TOKEN)
# bot.send_message(chat_id=CHAT_ID, message=message)
# else:
# pass

# функция запускающая все другие

@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def vgsales_redkina():
    # Считали и вернули таблицу
    @task(retries=3)
    def get_data():
        df = pd.read_csv(file)
        df = df.query("Year == @year")
        return df

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=3, retry_delay=timedelta(1))
    def bestseller_game(df):
        bestseller_game_res = df.groupby(['Name'], as_index=False) \
            .agg({'Global_Sales': 'sum'}) \
            .rename(columns={'Global_Sales': 'Total_Sales'}) \
            .sort_values(by='Total_Sales', ascending=False) \
            .iloc[0].Name
        return bestseller_game_res

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def bestseller_games_euro(df):
        result = df.groupby(["Genre"], as_index=False).agg({"EU_Sales": "sum"}).sort_values("EU_Sales", ascending=False)
        bestseller_games_euro_res = ', '.join(result.Genre.tolist())
        return bestseller_games_euro_res

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько

    @task()
    def million_platform_na(df):
        df['MoreThan1M'] = df.NA_Sales > 1.0
        million_platform_na_res = df.groupby(["Platform"], as_index=False)['NA_Sales'] \
            .agg({"MoreThan1M": "sum"}).sort_values("MoreThan1M", ascending=False)
        return million_platform_na_res

    # У какого издателя самые высокие средние продажи в Японии?
    # Перечислить все, если их несколько
    @task()
    def best_publisher_jp(df):
        best_publisher_jp = df.groupby(["Publisher"], as_index=False).agg({"JP_Sales": "mean"}).sort_values("JP_Sales",
                                                                                                            ascending=False)
        best_publisher_jp_res = best_publisher_jp.query("JP_Sales > 0")
        return best_publisher_jp_res

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def games_eur_vs_jp(df):
        df['EurMoreThanJp'] = df.EU_Sales > df.JP_Sales
        games_eur_vs_jp_res = df.EurMoreThanJp.count()
        return games_eur_vs_jp_res

    @task()
    def print_data(bestseller_game_res, bestseller_games_euro_res, million_platform_na_res, best_publisher_jp_res,
                   games_eur_vs_jp_res):
        context = get_current_context()
        date = context['ds']

        print('----------------')
        print(f' Какая игра была самой продаваемой в {year}г. во всем мире?')
        print(bestseller_game_res)
        print('----------------')
        print(f' Игры какого жанра были самыми продаваемыми в {year}г. в Европе?')
        print(bestseller_games_euro_res)
        print('----------------')
        print(
            f' На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в {year}г. в Северной Америке?')
        print(million_platform_na_res)
        print('----------------')
        print(f' У какого издателя самые высокие средние продажи в Японии за {year}г.?')
        print(best_publisher_jp_res)
        print('----------------')
        print(f' Сколько игр продались лучше в Европе, чем в Японии за {year}г.?')
        print(games_eur_vs_jp_res)
        print('----------------')

    top_data = get_data()
    best_game_RES = bestseller_game(top_data)
    bestseller_games_euro_RES = bestseller_games_euro(top_data)
    million_platform_na_RES = million_platform_na(top_data)
    best_publisher_jp_RES = best_publisher_jp(top_data)
    games_eur_vs_jp_RES = games_eur_vs_jp(top_data)

    print_data(best_game_RES, bestseller_games_euro_RES, million_platform_na_RES, best_publisher_jp_RES,
               games_eur_vs_jp_RES)


vgsales_redkina = vgsales_redkina()