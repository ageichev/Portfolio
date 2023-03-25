import requests
from zipfile import ZipFile
import json
from urllib.parse import urlencode
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
default_args = {
'owner': 'n-horkov',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2022, 2, 9),
'schedule_interval': '0 14 * * *'
}
chat_id = -749680935
try:
    BOT_TOKEN = '5355014076:AAFHXOH7W5RZsmoTRWphTiAZ7-Me0A62jjA'
except:
    BOT_TOKEN = ''
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Успешно! Dag {dag_id} сработал {date}'
    if BOT_TOKEN != '':
        params = {'chat_id': chat_id, 'text': message}
        base_url = f'https://api.telegram.org/bot5355014076:AAFHXOH7W5RZsmoTRWphTiAZ7-Me0A62jjA/'
        url = base_url + 'sendMessage?' + urlencode(params)
        resp = requests.get(url)
        bot = telegram.Bot(token=BOT_TOKEN)
        #bot.send_message(chat_id=CHAT_ID, message=message)
    else:
        pass
@dag(default_args=default_args,catchup=False)
def n_horkov_lesson3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv', sep=',')
        login = 'n-horkov'
        year = 1994 + hash(f'{login}') % 23
        df_year = df.loc[df['Year'] == year]
        df_year['Year'] = df_year['Year'].astype('int')
        return  df_year
    @task()
    #Какая игра была самой продаваемой в этом году во всем мире?
    def top_sales(df_year):
        df_1 = df_year
        top_games = df_year.groupby('Name',as_index=False).agg({'Global_Sales':'sum'}).sort_values(by='Global_Sales', ascending=False).Name.iloc[0]
        return top_games 
    @task()
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def game_europe(df_year):
        top_genre = df_year.groupby('Genre',as_index=False).agg({'EU_Sales':'sum'}).sort_values(by='EU_Sales', ascending=False)
        return top_genre.Genre.tolist()   
    @task()
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    #Перечислить все, если их несколько
    def top_platform(df_year):
        platform = df_year.groupby(['Platform','Name'],as_index=False).agg({'NA_Sales':'sum'}).query('NA_Sales > 1')
        top_plat = platform.groupby('Platform',as_index=False).agg({'NA_Sales':'count'}).sort_values(by='NA_Sales', ascending=False)
        return top_plat.Platform.tolist()
    @task()
    #У какого издателя самые высокие средние продажи в Японии?
    #Перечислить все, если их несколько
    def mean_sales(df_year):
        top_mean = df_year.groupby('Publisher',as_index=False).agg({'JP_Sales':'mean'}).sort_values(by='JP_Sales', ascending=False).head(10)
        return top_mean.Publisher.tolist()
    @task()
    #Сколько игр продались лучше в Европе, чем в Японии?
    def games_eu(df_year):
        games = df_year.groupby(['Name'],as_index=False).agg({'JP_Sales':'sum','EU_Sales':'sum'})
        quantity_games = games.loc[games['EU_Sales']>games['JP_Sales']].shape[0]
        return quantity_games
    import requests
    @task(on_success_callback=send_message)
    def print_data(games_print,genre_print,plat_print,mean_print,quantity_games_print):
        context = get_current_context()
        date = context['ds']    
        print(f'''Данные от  {date}
                  Самая продавая игра в мире: {games_print}
                  Самая продаваемый жанр в Епропе: {genre_print}
                  На какой платформе тираж был больше 1 млн в Северной Америке: {plat_print}
                  Самые выоские средние продажи в Японии: {mean_print}
                  Сколько игр продались лучше в Европе, чем в Японии: {quantity_games_print}'''
              )
        
    df_year = get_data()
    games_print = top_sales(df_year)
    genre_print = game_europe(df_year)
    plat_print = top_platform(df_year)
    mean_print = mean_sales(df_year)     
    quantity_games_print = games_eu(df_year)
    print_data(games_print,genre_print,plat_print,mean_print,quantity_games_print)
n_horkov_lesson3 = n_horkov_lesson3()
