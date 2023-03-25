from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import requests
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

link ='/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'e-korobkova') % 23

default_args = {
    'owner': 'e-korobkova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 26),
    'schedule_interval': '0 16 * * *'
}

CHAT_ID = -1001853974067
BOT_TOKEN ='5926139156:AAE0kiiV9emrTtYpWG6vh2-o09o58aaEEdE'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

@dag(default_args=default_args, catchup=False)
def e_korobkova_lesson3():
    
    @task()
    def get_data():
        vgsales = pd.read_csv(link)
        vgsales = vgsales.query('Year == @year')
        return vgsales
    
    @task() # Какая игра была самой продаваемой в этом году во всем мире?
    def the_best_game(vgsales):
        the_best_game = vgsales[vgsales['Global_Sales'] == vgsales['Global_Sales'].max()]['Name']
        return the_best_game.to_csv(index=False, header=False)
    
    @task() # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def the_best_genre(vgsales):
        genres = vgsales.groupby('Genre', as_index = False).agg({'EU_Sales': 'sum'})
        the_best_genre = genres[genres['EU_Sales']== genres['EU_Sales'].max()]['Genre']
        return the_best_genre.to_csv(index=False, header=False)
                                     
    @task() # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def the_best_platforms(vgsales):
        vgsales_NA_million = vgsales.query('NA_Sales > 1.0')
        platforms = vgsales_NA_million.groupby('Platform', as_index = False).agg({'NA_Sales': 'count'})
        the_best_platforms= platforms[platforms['NA_Sales']== platforms['NA_Sales'].max()]['Platform']
        return the_best_platforms.to_csv(index=False, header=False)
                                     
    @task() # У какого издателя самые высокие средние продажи в Японии?
    def the_best_publishers(vgsales):
        vgsales_publishers = vgsales.groupby('Publisher', as_index = False).agg({'JP_Sales': 'mean'})
        the_best_publishers = vgsales_publishers[vgsales_publishers['JP_Sales']== vgsales_publishers['JP_Sales'].max()]['Publisher']
        return the_best_publishers.to_csv(index=False, header=False)        
                                     
    @task() # Сколько игр продались лучше в Европе, чем в Японии?                             
    def best_in_EU(vgsales):
        best_in_EU = vgsales[vgsales['EU_Sales'] > vgsales['JP_Sales']].shape[0]
        return best_in_EU                                 
                                     
    @task(on_success_callback=send_message)
    def print_data(the_best_game, the_best_genre, the_best_platforms, the_best_publishers, best_in_EU):
        context = get_current_context()
        date = context['ds']
                                     
        print(f'''Year {year}:\n
                   The best selling game in the world : {the_best_game} \n
                   The most popular genre in Europe : {the_best_genre} \n
                   The most popular platform in North America (games > 1million) : {the_best_platforms} \n
                   The publisher with the highest average sales in Japan : {the_best_publishers} \n
                   Number of games that more popular in Europe than in Japan : {best_in_EU} ''')
      
    vgsales = get_data()                                                                  
    
    the_best_game = the_best_game(vgsales)
    the_best_genre = the_best_genre(vgsales)
    the_best_platforms = the_best_platforms(vgsales)
    the_best_publishers = the_best_publishers(vgsales)
    best_in_EU = best_in_EU(vgsales)

    print_data(the_best_game, the_best_genre, the_best_platforms, the_best_publishers, best_in_EU)

e_korobkova_lesson3 = e_korobkova_lesson3()                                                                         