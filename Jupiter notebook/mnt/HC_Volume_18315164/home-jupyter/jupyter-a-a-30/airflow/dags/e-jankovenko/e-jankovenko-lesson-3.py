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

TOP_GAMES_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
Year = 1994 + hash(f'e-jankovenko') % 23

default_args = {
    'owner': 'e-jankovenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 17),
    'schedule_interval': '30 08 * * *'
}

CHAT_ID = 393516844
BOT_TOKEN = '5565281394:AAFgkuzAfKO_j4cxqBgz16XOhed4IzOoH6k'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)
    
@dag(default_args=default_args, catchup=False)
def e_jankovenko_top_games_2009():
    @task(retries = 4,retry_delay=timedelta(5))
    def get_data_2009():
        games_data = pd.read_csv(TOP_GAMES_FILE,sep=',')
        games_data=games_data.query("Year == @Year")
        return games_data

    @task()
    def get_top_game(games_data):
        #games_data = pd.read_csv(games_data)
        top_game = games_data \
            .groupby('Name', as_index=False)\
            .agg({'Global_Sales':'sum'})\
            .sort_values('Global_Sales', ascending=False)\
            .head(1).loc[0:]['Name']
        return top_game

    @task()
    def get_top_genre(games_data):
        #games_data = pd.read_csv(games_data)
        top_genre = games_data\
            .groupby('Genre', as_index=False)\
            .agg({'EU_Sales':'sum'})
        top_genre = top_genre[top_genre['EU_Sales']==top_genre['EU_Sales'].max()].loc[0:]['Genre']
        return top_genre

    @task()
    def get_top_publisher(games_data):
        #games_data = pd.read_csv(games_data)
        top_publisher = games_data[games_data['JP_Sales']>1] \
            .groupby('Publisher', as_index=False)\
            .agg({'JP_Sales':'mean'})\
            .sort_values('JP_Sales', ascending=False)
        top_publisher = top_publisher[top_publisher['JP_Sales']==top_publisher['JP_Sales'].max()].loc[0:]['Publisher']
        return top_publisher
    
    @task()
    def get_NA_sales(games_data):
        #games_data = pd.read_csv(games_data)
        NA_sales = games_data \
            .groupby(['Platform','Name'], as_index=False)\
            .agg({'NA_Sales':'sum'}) \
            .sort_values('NA_Sales', ascending=False)
        top_platform = NA_sales[NA_sales['NA_Sales']>1] \
            .groupby('Platform', as_index=False)\
            .agg({'Name':'count'})\
            .sort_values('Name', ascending=False)
        top_platform = top_platform[top_platform['Name']==top_platform['Name'].max()].loc[0:]['Platform']
        return top_platform

    @task()
    def get_EU_sales(games_data):
        #games_data = pd.read_csv(games_data)
        sales = games_data \
            .groupby('Name')\
            .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        top_EU_sales = sales[sales['EU_Sales']>sales['JP_Sales']].shape[0]
        return top_EU_sales
    
    @task(on_success_callback=send_message)
    def print_data(top_game, top_genre, top_publisher, get_NA_sales, get_EU_sales):

        print(f'''Top game in {Year} in Global:
        {top_game}''')

        print(f'''Top genre in {Year} in Europe:
        {top_genre}''')
        
        print(f'''Top platform in {Year} in North America:
        {get_NA_sales}''')

        print(f'''Top publisher in {Year} in Japan:
        {top_publisher}''')

        print(f'''Count games sold in EU than in JP in {Year}:
        {get_EU_sales}''')

   
    games_data = get_data_2009()
    top_game = get_top_game(games_data)
    top_genre = get_top_genre(games_data)
    top_publisher = get_top_publisher(games_data)
    top_platform = get_NA_sales(games_data)
    top_EU_sales = get_EU_sales(games_data)
    print_data(top_game, top_genre, top_publisher, top_platform, top_EU_sales)

e_jankovenko_top_games_2009 = e_jankovenko_top_games_2009()
