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


login = 'p-zabelin-20'
year = 1994 + hash(f'login') % 23


default_args = {
    'owner': 'p-zabelin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 3),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 478445325
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
def p_zabelin_dag_lesson3():
    @task()
    def get_data():
        vgsales = pd.read_csv('vgsales.csv')
        vgsales = vgsales.fillna(0)
        vgsales['Year'] = vgsales['Year'].apply(lambda x: int(x))
        vgsales_year = vgsales.query('Year == @year')
        return vgsales_year

    @task()
    def get_top_game(vgsales_year):
# Какая игра была самой продаваемой в этом году во всем мире?
        return vgsales_year.groupby('Name').sum().Global_Sales.idxmax()

    @task()
    def get_top_Genre_EU(vgsales_year):
#         Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
        return vgsales_year.groupby('Genre').sum().EU_Sales.idxmax()

    @task()
    def get_top_Platform_NA(vgsales_year):
#         На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
# Перечислить все, если их несколько
        return vgsales_year.query('NA_Sales > 1').groupby('Platform').sum().NA_Sales.idxmax()

    @task()
    def get_top_Publisher_JP(vgsales_year):
#         У какого издателя самые высокие средние продажи в Японии?
# Перечислить все, если их несколько
        return vgsales_year.groupby('Publisher').mean().JP_Sales.idxmax()

    @task()
    def get_EU_vs_JP(vgsales_year):
#         Сколько игр продались лучше в Европе, чем в Японии?
        return vgsales_year.query('EU_Sales > JP_Sales').count().Name

    @task(on_success_callback=send_message)
    def print_data(get_top_game, get_top_Genre_EU, get_top_Platform_NA, get_top_Publisher_JP, get_EU_vs_JP):

        context = get_current_context()
        date = context['ds']

        print(f'''Data for                                                       {year} 
                  The most popular video game:                                   {get_top_game} 
                  The most popular genre in Europe:                              {get_top_Genre_EU}
                  Top platform in North America:                                 {get_top_Platform_NA}
                  Top publisher in Japan:                                        {get_top_Publisher_JP}
                  Number of games that are more popular in Europe than in Japan: {get_EU_vs_JP}''')


    vgsales_year = get_data()
    get_top_game = get_top_game(vgsales_year)
    get_top_Genre_EU = get_top_Genre_EU(vgsales_year)
    get_top_Platform_NA = get_top_Platform_NA(vgsales_year)
    get_top_Publisher_JP = get_top_Publisher_JP(vgsales_year)
    get_EU_vs_JP = get_EU_vs_JP(vgsales_year)
    

    print_data(get_top_game, get_top_Genre_EU, get_top_Platform_NA, get_top_Publisher_JP, get_EU_vs_JP)
     

p_zabelin_dag_lesson3 = p_zabelin_dag_lesson3()
