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

default_args = {
    'owner': 'r-burehin-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 11),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 416830384
BOT_TOKEN = '1601804271:AAHVVVmPgjkyzM2q5v97dOtqCk9dC7OHJaw'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Great!!! Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass
    
@dag(default_args=default_args, catchup=False, tags= ["bur_train"])
def games_br_v5():
    @task(retries=3)
    def get_data():
        
        data_source = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        data = pd.read_csv(data_source)

        login = 'r-burehin-21'
        year = 1994 + hash(f'{login}') % 23
        
        data = data.query("Year == @year")
        
        return data.to_csv(index=False)

    @task(retries=4, retry_delay=timedelta(10))
    def most_popular_game(data):
        data = pd.read_csv(StringIO(data))
        game = (data
                 .groupby('Name')
                 .agg({'Global_Sales': 'sum'})
                 .sort_values('Global_Sales', ascending = False)
                 .idxmax()
                 .to_list()[0]
                )
        
        return {'most_pop_game': game}
    
    
    @task(retries=4, retry_delay=timedelta(10))
    def most_popular_genre_eu(data):
        data = pd.read_csv(StringIO(data))
        eu_most_popular_genre = (data
                                   .groupby('Genre')
                                   .agg({'EU_Sales':'sum'})
                                   .sort_values('EU_Sales', ascending = False)
                                   .idxmax()
                                   .to_list()
                                  )
        return {'eu_most_popular_genre': eu_most_popular_genre}

    @task(retries=4, retry_delay=timedelta(10))
    def most_popular_platform_na(data):
        data = pd.read_csv(StringIO(data))
        games_NA_sales_more_1mln = \
        (data
         .groupby('Name')
         .agg({'NA_Sales': 'sum'})
         .query("NA_Sales > 1")
         .index
         .to_list()
        )

        na_most_popular_platform = (
            data
            .query("Name.isin(@games_NA_sales_more_1mln)")
            .groupby('Platform')
            .agg({'Name': 'count'})
            .sort_values('Name', ascending = False)
            .idxmax()
            .to_list()
        )
        return {'na_most_popular_platform': na_most_popular_platform}
    
    @task(retries=4, retry_delay=timedelta(10))
    def highest_avg_jap_sales_publisher(data):
        data = pd.read_csv(StringIO(data))
        highest_avg_jap_sales_publisher = (data
                                     .groupby('Publisher')
                                     .agg({'JP_Sales': 'mean'})
                                     .sort_values('JP_Sales', ascending = False)
                                     .idxmax()
                                     .to_list()
                                    )
        return {'highest_avg_jap_sales_publisher': highest_avg_jap_sales_publisher}
    
    @task(retries=4, retry_delay=timedelta(10))
    def number_games_sold_better_in_eu_vs_jap(data):
        data = pd.read_csv(StringIO(data))
        number_games_sold_better_in_eu_vs_jap = (data
                                                 .groupby('Name')
                                                 .agg({'EU_Sales': 'sum',
                                                       'JP_Sales': 'sum', 
                                                      })
                                                 .query("EU_Sales > JP_Sales")
                                                 .shape[0]
                                                )
        return {'number_games_sold_better_in_eu_vs_jap': number_games_sold_better_in_eu_vs_jap}


    @task(on_success_callback=send_message)
    def print_data(stat_game, stat_genre, stat_platform, stat_publisher, stat_qnt_games):

        context = get_current_context()
        date = context['ds']

        game = stat_game['most_pop_game']
        genre = stat_genre['eu_most_popular_genre']
        platform = stat_platform['na_most_popular_platform']
        publisher = stat_publisher['highest_avg_jap_sales_publisher']
        qnt_games = stat_qnt_games['number_games_sold_better_in_eu_vs_jap']

        print(f'''Date: {date}''')
        print(f'''1. The most popular game: {game}''')
        print(f'''2. The most popular EU genre: {genre}''')
        print(f'''3. The most popular NA platform among games with edition more than 1 mln: {platform}''')
        print(f'''4. The highest average sales in Japan among publisher: {publisher}''')
        print(f'''5. Number games sold better in EU than in Japan: {qnt_games}''')


    data = get_data()
    stat_game = most_popular_game(data)
    stat_genre = most_popular_genre_eu(data)
    stat_platform = most_popular_platform_na(data)
    stat_publisher = highest_avg_jap_sales_publisher(data)
    stat_qnt_games = number_games_sold_better_in_eu_vs_jap(data)
    

    print_data(stat_game, stat_genre, stat_platform, stat_publisher, stat_qnt_games)

games_br_v5 = games_br_v5()    