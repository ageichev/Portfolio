import pandas as pd
import numpy as np
from io import StringIO
from datetime import timedelta
from datetime import datetime
from urllib.parse import urlencode
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

path_to_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'a-altynbaev'

year = 1994 + hash(f'{login}') % 23

CHAT_ID = 484277427
BOT_TOKEN = '5181764956:AAF980ok5Tra57jiIhnXFFiiaOleRgK_eAc'

#Пишем функцию которая будет отправлять сообщения в чат телеграм
def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Very good! Dag {dag_id} is completed on {date}.'
    params = {'chat_id': CHAT_ID, 'text': message}
    base_url = f'https://api.telegram.org/bot{BOT_TOKEN}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    resp = requests.get(url)
                     
default_args = {
    'owner': 'a.altynbaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 31),
    'schedule_interval' : '0 3 * * *',
}

#функция запускающая все другие функции                     
@dag(default_args=default_args, catchup=False)

def answers_for_task():
                     
    #Считали и вернули таблицу
    @task(retries=3)
    def get_data():
        df = pd.read_csv(path_to_file)
        df = df.query("Year == @year")
        return df 
    
    #Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=3, retry_delay=timedelta(1))
    def bestseller_game(df):
        bestseller_game_res = df.groupby(['Name'], as_index=False) \
            .agg({'Global_Sales':'sum'}) \
            .rename(columns={'Global_Sales':'Total_Sales'}) \
            .sort_values(by='Total_Sales', ascending=False) \
            .iloc[0].Name
        return bestseller_game_res

    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def bestseller_games_euro(df):
        result = df.groupby(["Genre"], as_index=False).agg({"EU_Sales":"sum"}).sort_values("EU_Sales", ascending=False)
        bestseller_games_euro_res = ', '.join(result.Genre.tolist())    
        return bestseller_games_euro_res
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task()
    def million_platform_na(df):
        df['MoreThan1M'] = df.NA_Sales > 1.0
        million_platform_na_res = df.groupby(["Platform"], as_index=False)['NA_Sales']\
        .agg({"MoreThan1M":"sum"}).sort_values("MoreThan1M", ascending=False)
        return million_platform_na_res
    
    # У какого издателя самые высокие средние продажи в Японии?
    # Перечислить все, если их несколько
    @task()
    def best_publisher_jp(df):
        best_publisher_jp = df.groupby(["Publisher"], as_index=False).agg({"JP_Sales":"mean"}).sort_values("JP_Sales", ascending=False)
        best_publisher_jp_res = best_publisher_jp.query("JP_Sales > 0")
        return best_publisher_jp_res
    
    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def games_eur_vs_jp(df):
        df['EurMoreThanJp'] = df.EU_Sales > df.JP_Sales
        games_eur_vs_jp_res = df.EurMoreThanJp.count()
        return games_eur_vs_jp_res  
    
    @task(on_success_callback = send_message)
    def print_data(bestseller_game_res, bestseller_games_euro_res, million_platform_na_res, best_publisher_jp_res, games_eur_vs_jp_res):
        
        context = get_current_context()
        date = context['ds']
        
        print('----------------')
        print(f' Какая игра была самой продаваемой в {date}г. во всем мире?')
        print(bestseller_game_res)
        print('----------------')
        print(f' Игры какого жанра были самыми продаваемыми в {date}г. в Европе?')
        print(bestseller_games_euro_res)
        print('----------------')
        print(f' На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в {date}г. в Северной Америке?')
        print(million_platform_na_res)
        print('----------------')
        print(f' У какого издателя самые высокие средние продажи в Японии за {date}г.?')
        print(best_publisher_jp_res)
        print('----------------')
        print(f' Сколько игр продались лучше в Европе, чем в Японии за {date}г.?')
        print(games_eur_vs_jp_res)
        print('----------------')
        
    top_data = get_data()
    best_game_RES = bestseller_game(top_data)
    bestseller_games_euro_RES = bestseller_games_euro(top_data)
    million_platform_na_RES = million_platform_na(top_data)
    best_publisher_jp_RES = best_publisher_jp(top_data)
    games_eur_vs_jp_RES = games_eur_vs_jp(top_data)
    
    print_data(best_game_RES, bestseller_games_euro_RES, million_platform_na_RES, best_publisher_jp_RES, games_eur_vs_jp_RES)
    
dag_answers_for_task = answers_for_task()
