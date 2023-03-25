# DAG в парадигме Airflow 2.0

import requests
import json
from urllib.parse import urlencode

import pandas as pd
import numpy as np

from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


LINK_FOR_DF = 'vgsales.csv'
YEAR_FOR_ANALYSIS = 1994 + hash('ro.shokalo-14') % 23


default_args = {
    'owner': 'r.shokalo',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 13)
}

schedule_interval = '*/5 11 * * *'


def our_telegram_api(my_message):
    
    token = '5375665808:AAE4hypsZh0bI7W1kHLGg4Yl4J5cJzUaTbg'  # a token for my telegram bot
    chat_id = -655053148  # a chat id for a chat with me and my telegram bot
    
    message = my_message  # a phrase which will be sent if the dag succeed
    
    params = {'chat_id': chat_id, 'text': message}
    
    base_url = f'https://api.telegram.org/bot{token}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    
    resp = requests.get(url)
    
    
def success_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    our_telegram_api(f'Successfully done! Dag {dag_id} completed on {date}')
    
    
def failure_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    our_telegram_api(f'Something gone wrong! Dag {dag_id} failed on {date}')


@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def rshokalo_lesson3_dag():
    # Таск 1: Получаем данные
    @task(retries=5)
    def get_data():
        df = pd.read_csv(LINK_FOR_DF)
        df_games = df[df['Year'] == YEAR_FOR_ANALYSIS]
        return df_games
    
    # Таск 2: Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_best_selling_game(df_games):
        best_selling_game = df_games[df_games['Global_Sales'] == df_games['Global_Sales'].max()]['Name'].to_list()
        return best_selling_game
        
    # Таск 3: Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько (если продажи игр равны)
    @task()
    def get_best_selling_genre_eu(df_games):
        best_selling_genre_eu = df_games.nlargest(1, 'EU_Sales', keep='all')['Genre'].unique().tolist()
        return best_selling_genre_eu
    
    # Таск 4: На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    # Перечислить все, если их несколько
    @task()
    def get_top_platforms_na(df_games):
        top_platforms_na = df_games \
                            .query('NA_Sales > 1') \
                            .groupby('Platform') \
                            .agg({'NA_Sales': 'count'}) \
                            ['NA_Sales'] \
                            .nlargest(1, keep='all') \
                            .index.to_list()
        return top_platforms_na
    
    # Таск 5: У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_top_mean_sales(df_games):
        top_mean_sales = df_games \
                            .groupby('Publisher', as_index=False) \
                            .agg({'JP_Sales': 'mean'}) \
                            .sort_values(by='JP_Sales', ascending=False) \
                            .round(2) \
                            .nlargest(1, 'JP_Sales', keep='all') \
                            ['Publisher'] \
                            .to_list()
        return top_mean_sales
    
    # Таск 6: Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_eu_sales_more_jp(df_games):
        eu_sales_more_jp = df_games \
                            .groupby('Name') \
                            .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}) \
                            .reset_index() \
                            .query('EU_Sales > JP_Sales') \
                            ['Name'] \
                            .count()
        return eu_sales_more_jp
    
    # Таск 7: Печатаем ответы на вопросы
    @task(on_success_callback=success_message, on_failure_callback=failure_message)
    def print_answers(best_selling_game,
                      best_selling_genre_eu,
                      top_platforms_na,
                      top_mean_sales,
                      eu_sales_more_jp,
                     ):
        
        context = get_current_context()
        date = context['ds']
        
        print(
            f"Данные по видеоиграм представлены для {YEAR_FOR_ANALYSIS} года",
            f"Самая продаваемая игра в мире: {', '.join(best_selling_game)}",
            f"Жанр, игры которого были самыми продаваемыми в Европе: {', '.join(best_selling_genre_eu)}",
            f"Платформа, на которой было больше всего игр, проданных более чем миллионным тиражом в Северной Америке: {', '.join(top_platforms_na)}",
            f"Издатель с самыми высокими средними продажами в Японии: {', '.join(top_mean_sales)}",
            f"Кол-во игр, которые продались лучше в Европе, чем в Японии: {eu_sales_more_jp}",
            sep='\n'
        )
        
    df_games = get_data()
    
    best_selling_game = get_best_selling_game(df_games)
    best_selling_genre_eu = get_best_selling_genre_eu(df_games)
    top_platforms_na = get_top_platforms_na(df_games)
    top_mean_sales = get_top_mean_sales(df_games)
    eu_sales_more_jp = get_eu_sales_more_jp(df_games)
    
    print_answers(best_selling_game,
                  best_selling_genre_eu,
                  top_platforms_na,
                  top_mean_sales,
                  eu_sales_more_jp)
    
    
rshokalo_lesson3_dag = rshokalo_lesson3_dag()
