#import requests
#from zipfile import ZipFile
#from io import BytesIO
import pandas as pd
#import numpy as np
from datetime import timedelta
from datetime import datetime
#from io import StringIO
#import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
# TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {
    'owner': 'k-ljutikova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 11, 27),
    'schedule_interval': '0 12 * * *'
}

# CHAT_ID = -620798068
# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''

# def send_message(context):
#     date = context['ds']
#     dag_id = context['dag'].dag_id
#     message = f'Huge success! Dag {dag_id} completed on {date}'
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass

my_year = 1994 + hash(f'k-ljutikova') % 23

@dag(default_args=default_args, catchup=False)
def k_lyu_airflow_3():
    @task(retries=4, retry_delay=timedelta(10))
    def get_data():
        vgsales = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        vgsales = vgsales[vgsales['Year']== my_year]
        return vgsales

    @task()
    def best_seller_global(vgsales):
        best_seller_global = vgsales[vgsales['Global_Sales'] == vgsales['Global_Sales'].max()]['Name'].to_list()
        return best_seller_global

    @task()
    def best_selling_genres_eu(vgsales):
        eu_sales_sum = vgsales\
        .groupby('Genre', as_index=False)\
        .agg({'EU_Sales':'sum'})
        best_selling_genres_eu = eu_sales_sum[eu_sales_sum['EU_Sales'] == eu_sales_sum['EU_Sales'].max()]['Genre'].to_list()
        return best_selling_genres_eu

    @task()
    def top_selling_platforms_NA(vgsales):
        platform_NA = vgsales\
        .query('NA_Sales > 1')\
        .groupby('Platform', as_index=False)\
        .agg({'Name':'count'})
        top_selling_platforms_NA = platform_NA[platform_NA['Name'] == platform_NA['Name'].max()]['Platform'].to_list()
        return top_selling_platforms_NA

    @task()
    def best_JP_Publisher (vgsales):
        JP_Publishers =vgsales\
        .groupby('Publisher', as_index=False)\
        .agg({'JP_Sales':'mean'})
        best_JP_Publisher = JP_Publishers[JP_Publishers['JP_Sales'] == JP_Publishers['JP_Sales'].max()]['Publisher'].to_list()
        return best_JP_Publisher
    
    @task()
    def EU_winners_vs_JP_count(vgsales):
        EU_winners_vs_JP_count = len(vgsales.query('EU_Sales > JP_Sales'))
        return EU_winners_vs_JP_count

    @task()
    def print_data(my_year, best_seller_global, best_selling_genres_eu, top_selling_platforms_NA, best_JP_Publisher, EU_winners_vs_JP_count):
        
        print(
f'''Sales data for {my_year} year.
Top Global sales game:
{best_seller_global}

Top EU sales by genre:
{best_selling_genres_eu}

Top NA sales by platform:
{top_selling_platforms_NA}

Top JP sales by publisher:
{best_JP_Publisher}

Quantity of games sold better in EU than JP:
{EU_winners_vs_JP_count}
''')

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

    vgsales = get_data()
    best_seller_global = best_seller_global(vgsales)
    best_selling_genres_eu = best_selling_genres_eu(vgsales)
    top_selling_platforms_NA = top_selling_platforms_NA(vgsales)
    best_JP_Publisher = best_JP_Publisher(vgsales)
    EU_winners_vs_JP_count = EU_winners_vs_JP_count(vgsales)

    print_data(my_year, best_seller_global, best_selling_genres_eu, top_selling_platforms_NA, best_JP_Publisher, EU_winners_vs_JP_count)

k_lyu_airflow_3 = k_lyu_airflow_3()
