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

path_to_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'i-mihajlova'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'i-mihajlova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 21),
    'schedule_interval': '0 12 * * *'
}

#CHAT_ID = -620798068
#try:
#    BOT_TOKEN = Variable.get('telegram_secret')
#except:
#    BOT_TOKEN = ''

#def send_message(context):
#    date = context['ds']
#    dag_id = context['dag'].dag_id
#    message = f'Huge success! Dag {dag_id} completed on {date}'
#    if BOT_TOKEN != '':
#        bot = telegram.Bot(token=BOT_TOKEN)
#        bot.send_message(chat_id=CHAT_ID, text=message)
#    else:
#        pass

@dag(default_args=default_args, catchup=False)
def i_mihajlova_task3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(path_to_file)
        df_year = df.query('Year == @year')
        return df_year

    @task(retries=4, retry_delay=timedelta(10))
    def get_top_saled_game(df_myyear):
        return df_myyear.loc[df_myyear.Global_Sales.idxmax(), 'Name']

    @task()
    def get_best_EU_genre(df_myyear):
        EU_sales_byGenre = df_myyear.groupby('Genre', as_index=False)\
            .agg({'EU_Sales':'sum'})
        return ' '.join(EU_sales_byGenre.loc[EU_sales_byGenre.EU_Sales == EU_sales_byGenre.EU_Sales.max()].Genre.values)

    @task()
    def get_top_NA_platform(df_myyear):
        top_NA_sales = df_myyear.query('NA_Sales >= 1.0')\
            .groupby('Platform', as_index=False)\
            .agg({'Name':pd.Series.nunique})
        return ' '.join(top_NA_sales.loc[top_NA_sales.Name == top_NA_sales.Name.max()].Platform.values)

    @task()
    def get_best_publishers_JP(df_myyear):
        publishers_JP = df_myyear.groupby('Publisher',as_index=False)\
            .agg({'JP_Sales':'sum'})
        return ' '.join(publishers_JP.loc[publishers_JP.JP_Sales == publishers_JP.JP_Sales.max()].Publisher.values) 

    @task()
    def get_EU_preferable(df_myyear):
        return df_myyear.groupby('Name', as_index=False)\
            .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})\
            .query('EU_Sales > JP_Sales').Name.count()
         
    
#    @task(on_success_callback=send_message)
    @task()
    def print_data(best_game, best_genre,  top_platform, best_publisher, games_EU):

#        context = get_current_context()
#        date = context['ds']

        print(f'best saled game in {year} was {best_game}')

        print(f'''The most popular genres in EU in {year}:
                          {best_genre}''')
        print(f'''In {year} the most popular platform(s) in Northern America:
                  {top_platform}''')
        print(f'''In {year} japanese publishers
                  {best_publisher}
                   earned the most money''')
        print(f'''in {year} {games_EU} games earned more money in EU than in Japan''')

    df_myyear = get_data()
    best_game = get_top_saled_game(df_myyear)
    best_genre = get_best_EU_genre(df_myyear)
    top_platform = get_top_NA_platform(df_myyear)
    best_publisher = get_best_publishers_JP(df_myyear)
    games_EU = get_EU_preferable(df_myyear)
    print_data(best_game, best_genre,  top_platform, best_publisher, games_EU)

task3 = i_mihajlova_task3()
