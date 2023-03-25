#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

import telegram

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'n-lobachev-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = -672690205
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
def n_lobachev_18_lesson3():
    @task(retries=3)
    def get_data():
        login = 'n-lobachev-18'
        my_year = 1994 + hash(f'{login}') % 23
        games_data = pd.read_csv(vgsales)
        games_data = games_data.query('Year == @my_year')
        return games_data

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=3)
    def best_selling_game(games_data):
        b_s_g = games_data.groupby('Name').agg({'Global_Sales': 'sum'}).rename(columns={'Global_Sales': 'All_Sales'}).sort_values(by='All_Sales', ascending=False).reset_index().head(1)
        return b_s_g

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task(retries=3)
    def top_EU_genres(games_data):
        t_eu_g = games_data.groupby('Genre').agg({'EU_Sales': 'sum'}).sort_values(by='EU_Sales',ascending=False).reset_index()
        return t_eu_g

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько.
    @task(retries=3)
    def NA_platforms(games_data):
        na_p = games_data.groupby('Platform').agg({'NA_Sales': 'sum'}).sort_values(by='NA_Sales',ascending=False).reset_index().query('NA_Sales > 1')
        return na_p

    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task(retries=3)
    def top_JP_publisher(games_data):
        t_jp_p = games_data.groupby('Publisher').agg({'JP_Sales': 'mean'}).sort_values(by='JP_Sales',ascending=False).reset_index().head(1)
        return t_jp_p

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=3)
    def EU_vs_JP(games_data):
        EU_vs_JP = games_data.groupby('Name').agg({'JP_Sales': 'sum', 'EU_Sales': 'sum'}).reset_index()

        EU_vs_JP['delta'] = EU_vs_JP['EU_Sales'] - EU_vs_JP['JP_Sales']
        delta_EU_vs_JP = EU_vs_JP.query('delta > 0').delta.count()
        return delta_EU_vs_JP

    @task(on_success_callback=send_message)
    def print_data(best_selling_game, top_EU_genres, NA_platforms,
                   top_JP_publisher, EU_vs_JP):
        context = get_current_context()
        date = context['ds']

        print('1.Best selling game worldwide:', '<', best_selling_game.Name[0], '>')

        print('2.Top selling genres in Europe:', '\n\n', top_EU_genres)

        print('3.Platforms with over millions of sales:', '\n\n', NA_platforms)

        print('4.Publisher with the highest average sales in Japan:', '\n\n', top_JP_publisher.Publisher[0])

        print('5.', EU_vs_JP, 'games sold better in Europe than in Japan')

    games_data = get_data()

    best_selling_game = best_selling_game(games_data)
    top_EU_genres = top_EU_genres(games_data)
    NA_platforms = NA_platforms(games_data)
    top_JP_publisher = top_JP_publisher(games_data)
    EU_vs_JP = EU_vs_JP(games_data)

    print_data(best_selling_game, top_EU_genres, NA_platforms, top_JP_publisher, EU_vs_JP)


n_lobachev_18_lesson3 = n_lobachev_18_lesson3()