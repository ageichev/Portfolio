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

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'j-molchanova-21') % 23

default_args = {
    'owner': 'J.Molchanova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 10),
    'schedule_interval': '0 20 * * *'
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
def j_molchanova_lesson3():
    @task()
    def get_data():
        sales_data = pd.read_csv(vgsales)
        sales_data = sales_data.query('Year == @year')
        return sales_data

    @task()
    def game_n_one(sales_data):
        game_best = sales_data.groupby('Name').agg({'Global_Sales': 'sum'}).sort_values('Global_Sales', ascending=False).head(1).index[0]
        return game_best

    @task()
    def best_genres_eu(sales_data):
        top_genres_EU = sales_data.groupby('Genre').agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending=False).\
                        EU_Sales.idxmax()
        return top_genres_EU

    @task()
    def pltf_top_na(sales_data):
        pltf_NA = sales_data.groupby(['Platform']).agg({'NA_Sales': 'sum', 'Name': 'count'}).\
                    sort_values(['NA_Sales'], ascending=False).query('NA_Sales > 1').Name.idxmax()
        return pltf_NA

    @task()
    def best_publ_jp(sales_data):
        publ_JP = sales_data.groupby('Publisher').agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False).JP_Sales.idxmax()
        return publ_JP
    
    @task()
    def eu_vs_jp(sales_data):
        diff_eu_jp = sales_data.groupby('Name').agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).\
                        query('EU_Sales > JP_Sales').shape[0]
        return diff_eu_jp

    @task(on_success_callback=send_message)
    def print_data(game_best, top_genres_EU, pltf_NA, publ_JP, diff_eu_jp):

        context = get_current_context()
        date = context['ds']

        print(f'For {date} the most salable game on {year} is')
        print(game_best)
        
        print(f'For {date} the most salable genres in EU on {year} is')
        print(top_genres_EU)
        
        print(f'For {date} platform for 1mln game sales on {year} is')
        print(pltf_NA)
        
        print(f'For {date} publisher with best sales in Jp on {year} is')
        print(publ_JP)
        
        print(f'For {date} the number of games which were sold better in Eu than in Jp on {year} is')
        print(diff_eu_jp)
        


    sales_data = get_data()
    game = game_n_one(sales_data)
    genres = best_genres_eu(sales_data)
    platform = pltf_top_na(sales_data) 
    publisher = best_publ_jp(sales_data)
    sales = eu_vs_jp(sales_data)
    print_data(game, genres, platform, publisher, sales)

    
j_molchanova_lesson3 = j_molchanova_lesson3()