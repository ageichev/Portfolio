import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

# для тележки
import requests
import json
from urllib.parse import urlencode

#для airflow
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

def telega (message):
    
    with open('token') as f:
        token = f.read().strip()
    
    message=message
    api_patern=f'https://api.telegram.org/bot{token}'
    get_Up=f'{api_patern}/getUpdates'
    
    info=requests.get(get_Up)
    chat_id = info.json()['result'][0]['message']['chat']['id']
    
    params = {'chat_id': chat_id, 'text': message}

    base_url = f'https://api.telegram.org/bot{token}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    
    resp = requests.get(url)


#параметры дага
default_args = {
    'owner': 'a-kartashov-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 22),
    'schedule_interval': '1 21 * * *'}

#
def send_callbac_message(context):
    
    date = context['ds']
    dag_id = context['dag'].dag_id
    telega(message=f'Все получилось: Даг {dag_id} закончен в {date}')
    
#
def send_fatal_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    telega(message=f'Что-то пошло не так!')
    
#   
@dag(default_args=default_args, catchup=False)

def game_stat_a_kartashov():
    
    @task()
    def df_start():
        df = pd.read_csv('vgsales.csv')
        login = 'a-kartashov-21'
        year = 1994 + hash(f'{login}') % 23
        my_df_for_analiys = df[df.Year==year]
        return my_df_for_analiys.to_csv(index=False)

    @task()
    def Max_sale_games(my_df_for_analiys):
        my_df = pd.read_csv(StringIO(my_df_for_analiys))
        max_sales_game = my_df[my_df.Global_Sales==my_df.Global_Sales.max()].iloc[0]['Name']
        return max_sales_game
    
    @task()
    def Most_popular_ganre_eu(my_df_for_analiys):
        my_df = pd.read_csv(StringIO(my_df_for_analiys))
        most_popular_ganre_eu=my_df.groupby('Genre',as_index=False).EU_Sales.sum().max().at['Genre']
        count_sales_eu=int(my_df.groupby('Genre',as_index=False).EU_Sales.sum().max().at['EU_Sales'])
        return {'most_popular_ganre_eu':most_popular_ganre_eu,'count_sales_eu':count_sales_eu}

    @task()
    def Most_popular_platform (my_df_for_analiys):
        my_df = pd.read_csv(StringIO(my_df_for_analiys))
        most_popular_platform = my_df[my_df.NA_Sales>1].groupby('Platform',as_index=False).Name.count().max().at['Platform']
        count_games = my_df[my_df.NA_Sales>1].groupby('Platform',as_index=False).Name.count().max().at['Name']
        return {'most_popular_platform':most_popular_platform,'count_games':count_games}

    @task()
    def Most_publisher_jp (my_df_for_analiys):
        my_df = pd.read_csv(StringIO(my_df_for_analiys))
        most_publisher_jp = my_df.groupby('Publisher',as_index=False).JP_Sales.median().max().at['Publisher']
        median_jp = my_df.groupby('Publisher',as_index=False).JP_Sales.median().max().at['JP_Sales']
        return {'most_publisher_jp':most_publisher_jp,'median_jp': median_jp}

    @task()
    def Max_sale_games(my_df_for_analiys):
        my_df = pd.read_csv(StringIO(my_df_for_analiys))
        count_EUvsJP=(my_df.EU_Sales>my_df.JP_Sales).sum()
        return count_EUvsJP
                           
    @task(on_success_callback=send_callbac_message, on_failure_callback=send_fatal_message)
    def massage_print(ansver1, ansver2, ansver3, ansver4, ansver5):
        
        login = 'a-kartashov-21'
        year = 1994 + hash(f'{login}') % 23
        
        context = get_current_context()
        date = context['ds']
    
        max_sales_game=ansver1
        most_popular_ganre_eu, count_sales_eu = ansver2['most_popular_ganre_eu'],ansver2['count_sales_eu']
        most_popular_platform, count_games = ansver3['most_popular_platform'],ansver3['count_games']
        most_publisher_jp,  median_jp = ansver4['most_publisher_jp'],ansver4['median_jp']
        count_EUvsJP = ansver5
    
        print(f'''Дата вывода: {date}''')
        print(f'''Самая продаваямая игра в мире в {year} году: {max_sales_game}.''')
        print(f'''Самый популярный жанр в Европе в {year} году:  {most_popular_ganre_eu}. 
        Колличество: {count_sales_eu}.''')
        print(f'''Популярная платформа в Северной америке в {year} году: {most_popular_platform}. 
        Количество ИГОР: {count_games}.''')
        print(f'''Популярный издатель в Японии в {year} году: {most_publisher_jp}. 
        Среднее: {median_jp}.''')
        print(f'''Количество игр продались лучше в Европе, чем в Японии {year} году: {count_EUvsJP}.''')

    my_df_for_analiys = df_start()
    
    ansver1=Max_sale_games(my_df_for_analiys)
    ansver2=Most_popular_ganre_eu(my_df_for_analiys)
    ansver3=Most_popular_platform(my_df_for_analiys)
    ansver4=Most_publisher_jp(my_df_for_analiys)
    ansver5=Max_sale_games(my_df_for_analiys)

    massage_print(ansver1, ansver2, ansver3, ansver4, ansver5)

game_stat_a_kartashov = game_stat_a_kartashov()