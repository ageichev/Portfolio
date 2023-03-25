import pandas as pd

from datetime import timedelta
from datetime import datetime

import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

my_year = 1994 + hash(f'a-bajagantaev-21') % 23

default_args = {
    'owner': 'a-bajagantaev-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 7),
    'schedule_interval': '0 21 * * *'
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
def a_bajagantaev_21_hw3():
    @task()
    # Собираем все данные за необходимый год
    def get_all_data():
        all_data = pd.read_csv(vgsales) \
                 .query('Year == @my_year')
        return all_data


    @task()
    # Какая игра была самой продаваемой в этом году во всем мире?
    def get_bestseller_game(all_data):
        bestseller_game = all_data[all_data.Global_Sales == all_data.Global_Sales.max()] \
                              .Name.values[0]
        return bestseller_game


    @task()
    # Игры какого жанра были самыми продаваемыми в Европе?
    def get_top_Europe_Genre(all_data):
        top_Europe_Genre = all_data.groupby('Genre', as_index=False) \
                           .agg({'EU_Sales': 'sum'})
        top_Europe_Genre = top_Europe_Genre[top_Europe_Genre.EU_Sales == top_Europe_Genre.EU_Sales.max()] \
                                   .Genre.to_list()
        return top_Europe_Genre


    @task()
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def get_top_NA_Platform(all_data):
        top_NA_Platform = all_data.query('NA_Sales > 1') \
                              .groupby('Platform', as_index=False) \
                              .agg({'Name': 'count'}) \
                              .rename(columns={'Name': 'Number'})
        top_NA_Platform = top_NA_Platform[top_NA_Platform.Number == top_NA_Platform.Number.max()] \
                                         .Platform.to_list()
        return top_NA_Platform


    @task()
    # У какого издателя самые высокие средние продажи в Японии?
    def get_top_JP_Publisher(all_data):
        top_JP_Publisher = all_data.groupby('Publisher', as_index=False) \
                               .agg({'JP_Sales': 'mean'})
        top_JP_Publisher = top_JP_Publisher[top_JP_Publisher.JP_Sales == top_JP_Publisher.JP_Sales.max()] \
                                           .Publisher.to_list()
        return top_JP_Publisher


    @task
    # Сколько игр продались лучше в Европе, чем в Японии?
    def get_EU_more_JP(all_data):
        EU_more_JP =  all_data.query('EU_Sales > JP_Sales').shape[0]
        return EU_more_JP

    @task(on_success_callback=send_message)
    # Вывод на печать
    def print_data(bestseller_game, top_Europe_Genre, top_NA_Platform, top_JP_Publisher, EU_more_JP):

        context = get_current_context()
        date = context['ds']

        print(f'''Data for {my_year} for {date}
                  Bestseller worldwide game: {bestseller_game}
                  Top Europe genres: {top_Europe_Genre}
                  Top North America platforms: {top_NA_Platform}
                  Top Japan publishers: {top_JP_Publisher}
                  The number of games which sales is greater in Europe than in Japan: {EU_more_JP}''')


    all_data = get_all_data()

    bestseller_game = get_bestseller_game(all_data)
    top_Europe_Genre = get_top_Europe_Genre(all_data)
    top_NA_Platform = get_top_NA_Platform(all_data)
    top_JP_Publisher = get_top_JP_Publisher(all_data)
    EU_more_JP = get_EU_more_JP(all_data)

    print_data(bestseller_game, top_Europe_Genre, top_NA_Platform, top_JP_Publisher, EU_more_JP)

a_bajagantaev_21_hw3 = a_bajagantaev_21_hw3()
