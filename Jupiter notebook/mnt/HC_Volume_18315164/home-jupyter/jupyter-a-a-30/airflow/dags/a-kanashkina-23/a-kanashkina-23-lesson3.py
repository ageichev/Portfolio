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

df_path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a-kanashkina-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 4),
}
schedule_interval = '0 10 * * *'
   
CHAT_ID = 521040829
BOT_TOKEN = '5309289861:AAGu3gq8GGVqRiowbFLokNDhD8fDlGcPTiA'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def akanashkina23_lesson3():
    @task(retries=3)
    def get_data():
        login = 'a-kanashkina-23'
        year = 1994 + hash(f'{login}') % 23
        df = pd.read_csv(df_path).query('Year == @year')
        return df #.to_csv(index=False)

    @task(retries=4, retry_delay=timedelta(10))
    def get_top_game(df):
        #df = pd.read_csv(StringIO(df), names=['Rank', 'Name', 'Platform', 'Year', 'Genre', 'Publisher',
        #                                      'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales'])
        top_game = df.head(1).Name.reset_index(drop=True).loc[0]
        return top_game #top_data_ru.to_csv(index=False)

    @task()
    def get_top_genre(df):
        #df = pd.read_csv(StringIO(df), names=['Rank', 'Name', 'Platform', 'Year', 'Genre', 'Publisher',
        #                                      'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales'])
        top_genre = df.groupby(['Genre'], as_index=False) \
            .agg({'Rank': 'count'}) \
            .sort_values(['Rank'], ascending=False) \
            .rename(columns = {'Rank': 'count_genre'})
        count_top_genre = top_genre.count_genre.max()
        top_genre = top_genre.query('count_genre == @count_top_genre').Genre.to_frame()
        return top_genre.to_csv(index=False)

    @task()
    def get_top_platform(df):
        #df = pd.read_csv(StringIO(df), names=['Rank', 'Name', 'Platform', 'Year', 'Genre', 'Publisher',
        #                                      'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales'])
        top_platform = df.query('NA_Sales > 1').groupby(['Platform'], as_index=False) \
            .agg({'Rank': 'count'}) \
            .sort_values(['Rank'], ascending=False) \
            .rename(columns = {'Rank': 'count_platform'})
        count_top_platform = top_platform.count_platform.max()
        top_platform = top_platform.query('count_platform == @count_top_platform').Platform.to_frame()
        return top_platform.to_csv(index=False)

    @task()
    def get_top_publisher(df):
        #df = pd.read_csv(StringIO(df), names=['Rank', 'Name', 'Platform', 'Year', 'Genre', 'Publisher',
        #                                      'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales'])
        top_publisher = df.groupby(['Publisher'], as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .sort_values(['JP_Sales'], ascending=False) \
            .rename(columns = {'JP_Sales': 'avg_JP_Sales'})
        max_avg_JP_Sales = top_publisher.avg_JP_Sales.max()
        top_publisher = top_publisher.query('avg_JP_Sales == @max_avg_JP_Sales').Publisher.to_frame()
        return top_publisher.to_csv(index=False)
    
    @task()
    def get_eu_vs_jp(df):
        #df = pd.read_csv(StringIO(df), names=['Rank', 'Name', 'Platform', 'Year', 'Genre', 'Publisher',
        #                                      'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales'])
        eu_vs_jp = df.query('EU_Sales > JP_Sales').shape[0]
        year = int(df.Year.reset_index(drop=True).loc[0])
        return {'eu_vs_jp': eu_vs_jp, 'year': year}

    @task(on_success_callback=send_message)
    def print_data(top_game, top_genre, top_platform, top_publisher, eu_vs_jp):

        context = get_current_context()
        date = context['ds']
        
        eu_vs_jp, year = eu_vs_jp['eu_vs_jp'], eu_vs_jp['year']

        print(f'''Самая продаваемая игра в {year} г.:
        {top_game}''')

        print(f'''Самыми продаваемыми в Европе в {year} г. были игры жанра:
        {top_genre}''')
        
        print(f'''Больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {year} г., были на платформе
        {top_platform}''')
        
        print(f'''Самые высокие средние продажи в Японии в {year} г. были у издателя
        {top_publisher}''')
        
        print(f'''В Европе {eu_vs_jp} игр продалось лучше, чем в Японии, в {year} г.''')
        
    df = get_data()
    top_game = get_top_game(df)
    top_genre = get_top_genre(df)
    top_platform = get_top_platform(df)
    top_publisher = get_top_publisher(df)
    eu_vs_jp = get_eu_vs_jp(df)

    print_data(top_game, top_genre, top_platform, top_publisher, eu_vs_jp)

akanashkina23_lesson3 = akanashkina23_lesson3()
