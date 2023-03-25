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
    'owner': 'e.fedorchuk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 9),
    'schedule_interval': '0 10 * * *'
}

CHAT_ID = -663142135
BOT_TOKEN = '5374842715:AAEDobUqmrZDXAxVoG8Dl79gPzIrsQ7Gj_c'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

@dag(default_args=default_args, catchup=False)
def get_game_stat_2007_fedorchuk():
    @task(retries=3)
    def get_data():
        games_df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        games_df = games_df.query('Year == 2007')
        return games_df
    @task()
    def top_sales_global(games_df):
        top_sales_global = games_df.groupby('Name', as_index=False) \
            .agg({'Global_Sales': 'sum'}) \
            .sort_values('Global_Sales', ascending=False) \
            .head(1).reset_index().at[0, 'Name']
        return top_sales_global
    @task()
    def top_genre_eu(games_df):
        top_genre_eu = games_df.groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .query('EU_Sales == EU_Sales.max()').Genre.to_list()
        return top_genre_eu
    @task
    def top_platform_na(games_df):
        top_platform_na = games_df.query('NA_Sales > 1') \
            .groupby('Platform', as_index=False) \
            .agg({'Name': 'count'}) \
            .query('Name == Name.max()').Platform.to_list()
        return top_platform_na
    @task
    def top_mean_sales_jp(games_df):
        top_mean_sales_jp = games_df.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .query('JP_Sales == JP_Sales.max()').Publisher.to_list()
        return top_mean_sales_jp
    @task
    def eu_jp_better_sales(games_df):
        eu_jp_better_sales = games_df.query('EU_Sales > JP_Sales').count().Name
        return eu_jp_better_sales
    @task(on_success_callback=send_message)
    def print_data(top_sales_global, top_genre_eu, top_platform_na, top_mean_sales_jp, eu_jp_better_sales):
        print(f'''Data for year 2007
                  Top global sales: {top_sales_global}
                  Top genres in EU: {top_genre_eu}
                  The pltafrom with the biggest amount of games (> 1M sales in NA): {top_platform_na}
                  Top publishers in JP (the biggest mean sales): {top_mean_sales_jp}
                  {eu_jp_better_sales} games had bettes sales in EU than in JP''')
    
    games_df = get_data()
    
    global_sales = top_sales_global(games_df)
    genre_eu = top_genre_eu(games_df)
    platform_na = top_platform_na(games_df)
    publisher_jp = top_mean_sales_jp(games_df)
    eu_jp_sales = eu_jp_better_sales(games_df)
    
    print_data(global_sales, genre_eu, platform_na, publisher_jp, eu_jp_sales)
    
get_game_stat_2007_fedorchuk = get_game_stat_2007_fedorchuk()
