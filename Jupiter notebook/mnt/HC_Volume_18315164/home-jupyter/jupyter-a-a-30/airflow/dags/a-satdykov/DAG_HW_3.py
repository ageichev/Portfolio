import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable 
import telegram 



# зададим изначальные показатели

GAMES_SALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'a-satdykov'
YEAR = 1994 + hash(f'{login}') % 23
    
# зададим дефолтные параметры

default_args = {
    'owner': 'a-satdykov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 20),
    'schedule_interval': '0 25 * * 4'
}

# создаём бот для отчётов в тг

CHAT_ID = -897432063   
try: 
    BOT_TOKEN = Variable.get('telegram_secret')
except: 
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    
    if BOT_TOKEN != '':
        bot = telegram.Bot(token = BOT_TOKEN)
        bot.send_message(chat_id = CHAT_ID, text = message)
    else:
        pass
    
# Зададим даг через функцию

@dag(default_args = default_args, catchup = False)
def a_satdykov_HW_3_DAG():
    
    # первый таск - забирает таблицу

    @task(retries = 2) 
    def get_df_vgsales():
        vgsales = pd.read_csv(GAMES_SALES)
        vgsales.columns = vgsales.columns.str.lower()
        df_vgsales = vgsales.query('year == @YEAR')

        return df_vgsales
    
    # второй таск выполняет задание 1 - самая продаваемая игра в этом году
    
    @task(retries = 2) 
    def best_sales_game(df_vgsales):
        best_sales = df_vgsales.groupby('name', as_index = False) \
            .agg({'global_sales':'sum'}) \
            .sort_values('global_sales', ascending = False) \
            .head(1) \
            .iloc[0,0]

        return best_sales
    
    # третий таск выполняет задание 2 - Игры какого жанра были самыми продаваемыми в Европе?
    
    @task(retries = 2) 
    def get_best_genre_in_eu(df_vgsales):
        df_best_genre_in_eu = df_vgsales.groupby('genre', as_index = False) \
            .agg({'eu_sales' : 'sum'}) \
            .sort_values('eu_sales', ascending = False)
        best_genre_in_eu = df_best_genre_in_eu.loc[df_best_genre_in_eu.eu_sales == df_best_genre_in_eu.eu_sales.max()].genre.to_list()

        return best_genre_in_eu
    
    #  четвёртый таск выполняет задание 3 - На какой платформе было больше всего игр, которые продались 
    #  более чем миллионным тиражом в Северной Америке?
    
    @task(retries = 2) 
    def get_best_platform(df_vgsales):
        df_na_sales = df_vgsales.groupby('name', as_index = False) \
            .agg({'na_sales' : 'sum'}) \
            .query('na_sales > 1')
        na_games = df_na_sales.name.to_list()
        df_platform = df_vgsales.query('name == @na_games') \
            .groupby('platform', as_index = False) \
            .agg({'name':'count'})
        best_platform = df_platform.loc[df_platform.name == df_platform.name.max()].platform.to_list()

        return best_platform

    # пятый таск - У какого издателя самые высокие средние продажи в Японии?
    
    @task(retries = 2) 
    def get_best_publisher_jp (df_vgsales):
        df_jp_pub = df_vgsales.groupby('publisher', as_index = False) \
            .agg({'jp_sales': 'mean'})
        best_publisher_jp = df_jp_pub.loc[df_jp_pub.jp_sales == df_jp_pub.jp_sales.max()].publisher.to_list()

        return best_publisher_jp
    
    # шестой таск - Сколько игр продались лучше в Европе, чем в Японии
    
    @task(retries = 2) 
    def get_game_sales_eu_jp(df_vgsales):
        eu_jp_sales = df_vgsales.groupby('name', as_index = False).sum()
        eu_jp_sales = eu_jp_sales.loc[eu_jp_sales.eu_sales > eu_jp_sales.jp_sales]
        game_sales_eu_jp = eu_jp_sales.name.nunique()

        return game_sales_eu_jp
    
    # последний таск - печатаем полученные нами результаты
    
    @task(on_success_callback = send_message)
    def print_data(best_sales, best_genre_in_eu, best_platform, best_publisher_jp, game_sales_eu_jp):
        
        context = get_current_context()
        date = context['ds']
    
        print(f'For date {date} the best selling game in {YEAR} was')
        print (best_sales)
        
        print(f'For date {date} the best selling games in {YEAR} in Europe were genre')
        print (best_genre_in_eu)
        
        print(f'For date {date} the most preferred platform in {YEAR} for games sold in North America more than a million each was')
        print (best_platform)
        
        print(f'For date {date} the publisher sold the most games in Japan in {YEAR} was')
        print (best_publisher_jp)
        
        print(f'For date {date} the amount of games sold more in EU than in Japan in {YEAR} was')
        print (game_sales_eu_jp)
        
        
# и задаём последовательность тасков
    
    df_vgsales = get_df_vgsales()
    best_sales = best_sales_game(df_vgsales)
    best_genre_in_eu = get_best_genre_in_eu(df_vgsales)
    best_platform = get_best_platform(df_vgsales)
    best_publisher_jp = get_best_publisher_jp(df_vgsales)
    game_sales_eu_jp = get_game_sales_eu_jp(df_vgsales)
    print_data(best_sales, best_genre_in_eu, best_platform, best_publisher_jp, game_sales_eu_jp)
    
    
# запускаем наш даг

a_satdykov_HW_3_DAG = a_satdykov_HW_3_DAG()
        
        

    
    
    
    