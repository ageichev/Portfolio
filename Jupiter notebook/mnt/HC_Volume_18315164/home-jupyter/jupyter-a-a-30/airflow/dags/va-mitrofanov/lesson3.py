#import requests
#from zipfile import ZipFile
#from io import BytesIO
import pandas as pd
#import numpy as np
from datetime import timedelta
from datetime import datetime
#from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
# TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {
    'owner': 'va-mitrofanov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 30),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = -881554493
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
    
login = 'va-mitrofanov'
year = 1994 + hash(f'{login}') % 23
    
@dag(default_args=default_args, catchup=False)
def va_mitrofanov_l3():
    @task(retries=4, retry_delay=timedelta(10))
    def get_data():        
        vgsales = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        vgsales = vgsales.query('Year == @year')
        return vgsales

    @task()
    def best_selling_game(vgsales):
        name = vgsales.loc[vgsales['Global_Sales'].idxmax()]['Name']
        return name

    @task()
    def top_genre_eu(vgsales):
        genre_eu = vgsales.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        top_genre_eu = genre_eu[genre_eu['EU_Sales'] == genre_eu['EU_Sales'].max()]['Genre'].to_list()
        return top_genre_eu

    @task()
    def top_platform_1M_NA(vgsales):
        platform = vgsales.query('NA_Sales > 1') \
            .groupby('Platform', as_index=False) \
            .agg({'Rank': 'count'}) \
            .rename(columns={'Rank': 'Count'})
        top_platform = platform[platform['Count'] == platform['Count'].max()]['Platform'].to_list()
        return top_platform

    @task()
    def top_avg_sales_JP(vgsales):
        avg_sales_JP = vgsales.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .rename(columns={'JP_Sales' : 'avg_sales'})
        top_avg_sales_JP = avg_sales_JP[avg_sales_JP['avg_sales'] == avg_sales_JP['avg_sales'].max()]['Publisher'].to_list()
        return top_avg_sales_JP
    
    @task()
    def count_games_EU_more_JP(vgsales):
        count_games = vgsales.query('EU_Sales > JP_Sales')['Name'].count()
        return count_games

    @task()
    def print_data(year, name, top_genre_eu, top_platform, top_avg_sales_JP, count_games):

#         context = get_current_context()
#         date = context['ds']

#         ru_avg, ru_median = ru_stat['ru_avg'], ru_stat['ru_median']
#         com_avg, com_median = com_stat['com_avg'], com_stat['com_median']

#         print(f'''Data from .RU for {date}
#                   Avg rank: {ru_avg}
#                   Median rank: {ru_median}''')

#         print(f'''Data from .COM for {date}
#                           Avg rank: {com_avg}
#                           Median rank: {com_median}''')
        print(f'''************Data for {year} year************
                    The best-selling game is {name}
                    The genre of the best-selling games in Europe is {top_genre_eu}
                    Most games that have sold over a million copies in North America have been released on the {top_platform}
                    {top_avg_sales_JP} had the highest average sales in Japan
                    {count_games} games sold better in Europe than in Japan''')
        

    vgsales = get_data()
    name = best_selling_game(vgsales)
    top_genre_eu = top_genre_eu(vgsales)
    top_platform = top_platform_1M_NA(vgsales)
    top_avg_sales_JP = top_avg_sales_JP(vgsales)
    count_games = count_games_EU_more_JP(vgsales)

    print_data(year, name, top_genre_eu, top_platform, top_avg_sales_JP, count_games)

va_mitrofanov_l3 = va_mitrofanov_l3()