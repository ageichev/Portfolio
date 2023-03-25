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


login = 'e-sokolovatarjan'
my_year = 1994 + hash(f'{login}') % 23


default_args = {
    'owner': 'e-sokolovatarjan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 14)
}

CHAT_ID = -735108925
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



@dag(default_args=default_args, schedule_interval='30 22 * * *', catchup=False)
def sokolovavatarjan_3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv('vgsales.csv')
        df = df[df['Year'] == my_year]
        return df

    @task()
    def get_best_seller(df):
        Best_seller = df.groupby('Name').Global_Sales.sum().idxmax()
        return Best_seller

    @task()
    def get_genre_best_seller(df):
        Genre_best_seller = round(df.groupby('Genre').EU_Sales.sum(), 2)
        Genre_best_seller = Genre_best_seller[Genre_best_seller == Genre_best_seller.max()].reset_index()['Genre']
        return Genre_best_seller.to_csv(index=False, header=False)

    @task()
    def get_noth_best_platform(df):
        Noth_best_platform = df[df['NA_Sales'] > 1].groupby('Platform').Rank.count()
        Noth_best_platform = Noth_best_platform[Noth_best_platform == Noth_best_platform.max()].reset_index()[
            'Platform']
        return Noth_best_platform.to_csv(index=False, header=False)

    @task()
    def get_publisher_jp_bestseller(df):
        
        Publisher_jp_bestseller = df.groupby('Publisher')['JP_Sales'].mean()
        Publisher_jp_bestseller = \
        Publisher_jp_bestseller[Publisher_jp_bestseller == Publisher_jp_bestseller.max()].reset_index()['Publisher']
        return Publisher_jp_bestseller.to_csv(index=False, header=False)

    @task()
    def get_diff_eu_jp(df):
        
        Diff_eu_jp = df.groupby('Name').agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).reset_index()
        Diff_eu_jp = (Diff_eu_jp['EU_Sales'] > Diff_eu_jp['JP_Sales']).sum()
        return Diff_eu_jp

    @task(on_success_callback=send_message)
    def print_data(year, best_seller, genre_best_seller, noth_best_platform, publisher_jp_bestseller, diff_eu_jp):
        print(f'Worldwide best-selling game in {year} : \n{best_seller}')
        print(f'Best-selling genre in Europe in {year} : \n{genre_best_seller}')
        print(f'Best Platform in Noth America in {year} : \n{noth_best_platform}')
        print(f'Best publisher in Japan in {year} : \n{publisher_jp_bestseller}')
        print(f'Number of games that sold better in Europe than in Japan in {year} : \n{diff_eu_jp}')

    data = get_data()
    best_seller_3 = get_best_seller(data)
    genre_best_seller_3 = get_genre_best_seller(data)
    noth_best_platform_3 = get_noth_best_platform(data)
    publisher_jp_bestseller_3 = get_publisher_jp_bestseller(data)
    diff_eu_jp_3 = get_diff_eu_jp(data)
    print_data(my_year, best_seller_3, genre_best_seller_3, noth_best_platform_3, publisher_jp_bestseller_3,
                   diff_eu_jp_3)


sokolovavatarjan_3 = sokolovavatarjan_3()