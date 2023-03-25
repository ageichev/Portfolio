import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

sales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash('d-porfirev-23') % 23

default_args = {
    'owner': 'd-porfirev-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 25),
    'schedule_interval': '0 10 * * *'
}

CHAT_ID = 1086097753

BOT_TOKEN = '5702013019:AAGPhk3GjaUKRqgp3uu26zh2u22UWtlOSis'


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)

@dag(default_args=default_args, catchup=False)
def d_porfirev_23_zad_3():
    @task()
    def get_data():
        data = pd.read_csv(sales)
        data = data.query('Year == @year')
        return data
    
    ##Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_top_one_sales_one(data):
        top_data_one = data.groupby('Name').agg({'Global_Sales':sum}).idxmax()
        return top_data_one
    
    ##Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_top_genre_eu(data):
        data_top_genre_eu = data.groupby('Genre').agg({'EU_Sales':sum}).EU_Sales.idxmax()
        return data_top_genre_eu

    ##На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task()
    def get_top_platform_na(data):
        data_top_platform_na = data.query('NA_Sales > 1').groupby('Platform').agg({'Name': 'count'}).Name.idxmax()
        return data_top_platform_na
    
    ##У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_top_publisher_jp(data):
        data_top_publisher_jp = data.groupby('Publisher').agg({'JP_Sales': 'mean'}).JP_Sales.idxmax()
        return data_top_publisher_jp
    
    ##Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_count_games_ea_best_jp(data):
        data_count_games_ea_best_jp = data.query('EU_Sales > JP_Sales').shape[0]
        return data_count_games_ea_best_jp
    

    @task(on_success_callback=send_message)
    def print_data(top_data_one, data_top_genre_eu, data_top_platform_na, data_top_publisher_jp, data_count_games_ea_best_jp):
        context = get_current_context()
        date = context['ds']
        print(f'Дата выпрлнение дага {date}')
        print(f'Самая продавая игра в {year} в мире {top_data_one}')
        print(f'Самые продаваемые жанры в {year} в Европе {data_top_genre_eu}')
        print(f'Самые продаваемые игры в  {year} в Америке  {data_top_platform_na}')
        print(f'В {year} издатель с самыми высокими продажами в Японии {data_top_publisher_jp}')
        print(f'Сколько игр в {year} продалось в Европе лучге чем в Японии {data_count_games_ea_best_jp}') 
        
    
            
    data = get_data()
    top_data_one = get_top_one_sales_one(data)
    data_top_genre_eu = get_top_genre_eu(data)
    data_top_platform_na = get_top_platform_na(data)
    data_top_publisher_jp = get_top_publisher_jp(data)
    data_count_games_ea_best_jp = get_count_games_ea_best_jp(data)
    print_data(top_data_one, data_top_genre_eu, data_top_platform_na, data_top_publisher_jp, data_count_games_ea_best_jp)
    

d_porfirev_23_zad_3 = d_porfirev_23_zad_3()
