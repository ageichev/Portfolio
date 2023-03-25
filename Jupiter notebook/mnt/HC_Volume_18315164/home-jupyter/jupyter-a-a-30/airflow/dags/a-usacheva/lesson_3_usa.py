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

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year=1994+hash(f'a-usacheva')%23 

default_args = {
    'owner': 'a.usacheva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2022, 7, 30),
    'schedule_interval': '05 12 * * *'
}

#CHAT_ID = -620798068
#try:
    #BOT_TOKEN = Variable.get('telegram_secret')
#except:
   #BOT_TOKEN = ''

#def send_message(context):
    #date = context['ds']
    #dag_id = context['dag'].dag_id
    #message = f'Huge success! Dag {dag_id} completed on {date}'
    #if BOT_TOKEN != '':
        #bot = telegram.Bot(token=BOT_TOKEN)
        #bot.send_message(chat_id=CHAT_ID, text=message)
    #else:
        #pass

@dag(default_args=default_args, catchup=False)
def usacheva_lesson_3():
    
    @task()
    def get_data():
        vgsales_usa = pd.read_csv(vgsales).query('Year == @year')
        return vgsales_usa
    
# Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def max_sales_game_usa(vgsales_usa):
        best_game_global_usa = vgsales_usa[vgsales_usa.Global_Sales==vgsales_usa.Global_Sales.max()].Name.values[0]
        return best_game_global_usa
    
#Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько    
    @task()
    def eu_genre_max_usa(vgsales_usa):
        eu_genre_usa = vgsales_usa.groupby('Genre', as_index = False).agg({'EU_Sales':'sum'})
        eu_genre_usa = eu_genre_usa[eu_genre_usa.EU_Sales==eu_genre_usa.EU_Sales.max()].Genre.to_list()
        return eu_genre_usa
    
#На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
#Перечислить все, если их несколько
    @task()
    def na_platform_max_usa(vgsales_usa):
        na_best_platform_usa =vgsales_usa.query('NA_Sales>1').groupby('Platform', as_index = False).agg({'Name':'count'})
        na_best_platform_usa = na_best_platform_usa[na_best_platform_usa.Name == na_best_platform_usa.Name.max()].Platform.to_list()
        return na_best_platform_usa
    
#У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def jp_mean_sales_usa(vgsales_usa):
        jp_mean_usa = vgsales_usa.groupby('Publisher', as_index = False).agg({'JP_Sales':'mean'})
        jp_mean_usa = jp_mean_usa[jp_mean_usa.JP_Sales == jp_mean_usa.JP_Sales.max()].Publisher.values[0]
        return jp_mean_usa

#Сколько игр продались лучше в Европе, чем в Японии?    
    @task()
    def best_sales_usa_in_eu(vgsales_usa):
        eu_best_games_sales_usa = vgsales_usa.query('EU_Sales>JP_Sales').Name.count()
        return eu_best_games_sales_usa

    @task()
    def print_data(best_game_global_usa, eu_genre_usa, na_best_platform_usa, jp_mean_usa, eu_best_games_sales_usa):

        context = get_current_context()
        date = context['ds']

        print(f'The best selling game in {year} in the world {best_game_global_usa}') 
    
        print(f'The best selling genre in {year} in EU {eu_genre_usa}') 
                   
        print(f'The best selling game platform in {year}  in NA {na_best_platform_usa}') 
      
        print(f'The best selling game publisher in {year} in JP {jp_mean_usa}') 
               
        print(f'The number of games sold in {year} in EU is more than in JP {eu_best_games_sales_usa}') 

    vgsales_usa = get_data()
    best_game_global_usa = max_sales_game_usa(vgsales_usa)
    eu_genre_usa = eu_genre_max_usa(vgsales_usa)
    na_best_platform_usa = na_platform_max_usa(vgsales_usa)
    jp_mean_usa = jp_mean_sales_usa(vgsales_usa)
    eu_best_games_sales_usa = best_sales_usa_in_eu(vgsales_usa)
    
    print_data(best_game_global_usa, eu_genre_usa, na_best_platform_usa, jp_mean_usa, eu_best_games_sales_usa)

usacheva_lesson_3 = usacheva_lesson_3()