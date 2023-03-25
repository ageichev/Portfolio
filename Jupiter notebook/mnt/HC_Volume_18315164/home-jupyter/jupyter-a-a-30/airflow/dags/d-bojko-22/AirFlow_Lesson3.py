import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


year = 1994 + hash(f'{"d-bojko-22"}') % 23

default_args = {
    'owner': 'd-bojkjo-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 20),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = 5118995880
BOT_TOKEN = '5167350102:AAG5agC1mJ3--TgFgKU_yV-Qh7V3YjYQ5SU'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

@dag(default_args=default_args)
def my_second_awesome_dag_d_bojko_22():
    @task()
    def get_data():    
        df = pd.read_csv('vgsales.csv')
        return df
    
    @task()
    def get_most_popular_game(df):    
        most_popular_game = df.query('Year == @year') \
                                .groupby('Name', as_index=False) \
                                .agg({'Global_Sales': 'sum'}) \
                                .sort_values('Global_Sales', ascending=False) \
                                .head(1) \
                                .iloc[0] \
                                ['Name']
        return most_popular_game
    
    @task()
    def get_best_genre_EU(df):  
        best_genre_eu = df.query('Year == @year') \
                            .groupby('Genre', as_index=False) \
                            .agg({'EU_Sales': 'sum'}) \
                            .sort_values('EU_Sales', ascending=False) \
                            .head(1) \
                            .iloc[0] \
                            ['Genre']
        return best_genre_eu

    
    @task()
    def get_best_platform_NA(df):
        best_platform_na = df.query('Year == @year and NA_Sales > 1') \
                                .groupby('Platform', as_index=False) \
                                .agg({'NA_Sales': 'count'}) \
                                .sort_values('NA_Sales', ascending=False) \
                                .rename(columns={'NA_Sales': 'Games_Count'}) \
                                .head(1) \
                                .iloc[0] \
                                ['Platform']
        return best_platform_na
    
    @task()
    def get_best_publisher_JP(df):
        best_publisher_JP = df.query('Year == @year') \
                                .groupby('Publisher', as_index=False) \
                                .agg({'JP_Sales': 'mean'}) \
                                .sort_values('JP_Sales', ascending=False) \
                                .rename(columns={'JP_Sales': 'Avg_JP_Sales'}) \
                                .head(1) \
                                .iloc[0] \
                                ['Publisher']
        return best_publisher_JP
    
    @task()
    def count_gamnes_EU_better_than_JP(df):
        eu_vs_jp = df.query('Year == @year and EU_Sales > JP_Sales').Name.nunique()
        return eu_vs_jp
    
    @task(on_success_callback=send_message)
    def print_data(most_popular_game, best_genre_eu, best_platform_na, best_publisher_JP, eu_vs_jp):
        print(f'The most sold game in the world in {year} was {most_popular_game}')
        print(f'The most popular genre in EU in {year} was {best_genre_eu}')
        print(f'The most games sold in NA in {year} was on platform {best_platform_na}')
        print(f'The highest average sales in Japan in {year} got publisher {best_publisher_JP}')
        print(f'Number of games sold in EU better than in JP in {year} was {eu_vs_jp}')
        
    df = get_data()
    most_popular_game = get_most_popular_game(df)
    best_genre_eu = get_best_genre_EU(df)
    best_platform_na = get_best_platform_NA(df)
    best_publisher_JP = get_best_publisher_JP(df)
    eu_vs_jp = count_gamnes_EU_better_than_JP(df)
    print_data(most_popular_game, best_genre_eu, best_platform_na, best_publisher_JP, eu_vs_jp)
    
my_second_awesome_dag_d_bojko_22 = my_second_awesome_dag_d_bojko_22()