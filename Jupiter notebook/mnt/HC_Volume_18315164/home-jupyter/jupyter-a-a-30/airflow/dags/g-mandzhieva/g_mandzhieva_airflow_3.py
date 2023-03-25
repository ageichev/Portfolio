import requests
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import json
from urllib.parse import urlencode 

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


games = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
select_year = 1994 + hash('g-mandzhieva') % 23

default_args = {
    'owner': 'g.mandzhieva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 10),
    'schedule_interval': '30 18 * * *'
}

CHAT_ID = 1294312318
try:
    BOT_TOKEN = '5447614459:AAF4oPjqMFCv-pptu0Wiua7G0IqirIFVdPQ'
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        params = {'chat_id': CHAT_ID, 'text': message}
        base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
        url = base_url + 'sendMessage?' + urlencode(params)  
        resp = requests.get(url)
    else:
        pass

@dag(default_args=default_args, catchup = False)
def games_sales_lesson_3_g_mandzhieva():
    @task(retries=3)
    def get_data():
        games_df = pd.read_csv(games)
        games_data = games_df.query('Year == @select_year')
        return games_data.to_csv(index = False)

    @task()
    def get_global_selling_game(games_data):
        games_data_df = pd.read_csv(StringIO(games_data))
        best_selling_game = games_data_df.groupby('Name')['Global_Sales'].sum().idxmax()       
        return best_selling_game

    @task()
    def get_best_selling_genre_eu(games_data):
        games_data_df = pd.read_csv(StringIO(games_data))
        max_genre = games_data_df.groupby('Genre')['EU_Sales'].sum().max()
        best_selling_genre = games_data_df.groupby('Genre', as_index=False)['EU_Sales'].sum()\
            .query('EU_Sales == @max_genre')
        return best_selling_genre.to_csv(index = False)
      
    @task()
    def get_platform_na(games_data):
        games_data_df = pd.read_csv(StringIO(games_data))
        max_platform = games_data_df.groupby(['Platform','Name'], as_index=False)['NA_Sales'].sum()\
            .query('NA_Sales>1')\
            .groupby('Platform')['Name'].count().max()
        platforms = games_data_df.groupby(['Platform','Name'], as_index=False)['NA_Sales'].sum()\
            .query('NA_Sales>1')\
            .groupby('Platform', as_index = False)['Name'].count()\
            .rename(columns={"Name" : "Amount_of_games"})\
            .query('Amount_of_games == @max_platform')
        return platforms.to_csv(index=False)

    @task()
    def get_max_mean_sales_jp(games_data):
        games_data_df = pd.read_csv(StringIO(games_data))
        max_mean = games_data_df.groupby('Publisher')['JP_Sales'].mean().max()
        publishers = games_data_df.groupby('Publisher', as_index = False)['JP_Sales'].mean()\
            .rename(columns = {'JP_Sales' : 'JP_Mean_sales'})\
            .query('JP_Mean_sales == @max_mean')
        return publishers.to_csv(index=False)
    
    @task()
    def get_games_eu_jp(games_data):
        games_data_df = pd.read_csv(StringIO(games_data))
        eu_jp_sales = games_data_df.groupby('Name', as_index=False)[['EU_Sales', 'JP_Sales']].sum()\
            .query('EU_Sales > JP_Sales')['Name'].nunique()
        return eu_jp_sales

    @task(on_success_callback=send_message)
    def print_data(best_selling_game, best_selling_genre, platforms, publishers, eu_jp_sales):

        print(f'1. The best-selling game in {select_year} year worldwide is {best_selling_game}')
        print(f'2. The best-selling genre of games in Europe in {select_year} year')
        print(best_selling_genre)
        print(f'3. The platforms which has the most games that sold more than a million copies in North America in {select_year} year')
        print(platforms)
        print(f'4. The publishers which has the highest average sales in Japan in {select_year} year')
        print(publishers)
        print(f'5. {eu_jp_sales} games sold better in Europe than in Japan in {select_year} year')

     
    games_data = get_data()
    best_selling_game = get_global_selling_game(games_data)
    best_selling_genre = get_best_selling_genre_eu(games_data)
    platforms = get_platform_na(games_data)
    publishers = get_max_mean_sales_jp(games_data)
    eu_jp_sales = get_games_eu_jp(games_data)
    print_data(best_selling_game, best_selling_genre, platforms, publishers, eu_jp_sales)

games_sales_lesson_3_g_mandzhieva = games_sales_lesson_3_g_mandzhieva()