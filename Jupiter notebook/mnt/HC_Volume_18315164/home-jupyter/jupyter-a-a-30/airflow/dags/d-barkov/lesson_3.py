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

GAMES_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

login = 'd-barkov'
analyzed_year = str(1994 + hash(f'{login}') % 23)

default_args = {
    'owner': 'd-barkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 28),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = -777119718
BOT_TOKEN = '5301007392:AAFQ1FJjf6yUkDdPaw0Hhl_63LBrQYiyTZA'


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
def games_airflow_dag():
    
    
    @task(retries=3, retry_delay=timedelta(10))
    def get_data():
        games_df = pd.read_csv(GAMES_FILE, dtype={'Year': str})
        print('Year column before: ' + games_df['Year'])
        games_df['Year'] = games_df['Year'].str.split('.').str[0]
 
        return games_df
    
    
    @task()
    def get_best_selling_game(df):
        best_selling_game = df.query('Year == @analyzed_year') \
                                .sort_values('Global_Sales', ascending=False) \
                                .head(1) \
                                .Name \
                                .max()
        return best_selling_game
    
    
    @task()
    def get_popular_genres(df):
        popular_genres_list = df[df.Year == analyzed_year] \
                                .groupby('EU_Sales', as_index=False) \
                                .agg({'Genre': list}) \
                                .sort_values('EU_Sales', ascending=False) \
                                .head(1) \
                                .Genre \
                                .max()

        popular_genres = ', '.join(popular_genres_list) 
        return popular_genres
    
    
    @task()
    def get_top_platform(df):
        top_platform_list = df.query('Year == @analyzed_year and NA_Sales > 1.0') \
                                .groupby('Platform', as_index=False) \
                                .agg({'Name': 'count'}) \
                                .groupby('Name', as_index=False) \
                                .agg({'Platform': list}) \
                                .sort_values('Name', ascending=False) \
                                .head(1) \
                                .Platform \
                                .max()

        top_platform = ', '.join(top_platform_list)
        return top_platform
    
    
    @task()
    def get_top_JP_publisher(df):
        top_publisher_list = df.query('Year == @analyzed_year') \
                                .groupby('Publisher', as_index=False) \
                                .agg({'JP_Sales': 'mean'}) \
                                .groupby('JP_Sales', as_index=False) \
                                .agg({'Publisher': list}) \
                                .sort_values('JP_Sales', ascending=False) \
                                .head(1) \
                                .Publisher \
                                .max()

        top_publisher = ', '.join(top_publisher_list)
        return top_publisher
    
    
    @task()
    def get_EU_games_grater_JP(df):
        EU_games_grater_JP = df.query('Year == @analyzed_year and EU_Sales > JP_Sales').Name.nunique()
        return EU_games_grater_JP
    
    @task(on_success_callback=send_message)
    def show_info(*args, **kwargs):
        print(f'The best selling game in {analyzed_year} around the World was {args[0]};')
        print(f'The most popular games in Europe which were sold in {analyzed_year} {"were" if len(args[1].split(", ")) > 1 else "was"} {args[1]};')
        print(f"Top platform by sales more than 1M copies in Northern America in {analyzed_year} {'were' if len(args[2].split(', ')) > 1 else 'was'} {args[2]};")
        print(f"Top publisher who made the highest average sells in Japan in {analyzed_year} {'were' if len(args[3].split(', ')) > 1 else 'was'} {args[3]};")
        print(f"Amount of games which was sold rather in Europe than in Japan in {analyzed_year} was {args[4]}.")
        
        
    
    games_df = get_data()
    
    best_selling_game = get_best_selling_game(games_df)
    popular_genres = get_popular_genres(games_df)
    top_platform = get_top_platform(games_df)
    top_JP_publisher = get_top_JP_publisher(games_df)
    EU_games_grater_JP = get_EU_games_grater_JP(games_df)
    
    show_info(best_selling_game, popular_genres, top_platform, top_JP_publisher, EU_games_grater_JP)
    
    

games_airflow = games_airflow_dag()
