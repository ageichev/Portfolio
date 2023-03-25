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

PATH_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 's-s'
YEAR = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 's.savintsev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 27),
    'schedule_interval': '0 12 * * *'
}

#CHAT_ID = 5472842983
#BOT_TOKEN = Variable.get('telegram_secret')

#def send_message(context):
#    date = context['ds']
#    dag_id = context['dag'].dag_id
#    message = f'Success! Dag {dag_id} completed on {date}'
#    bot = telegram.Bot(token=BOT_TOKEN)
#    bot.send_message(chat_id=CHAT_ID, text=message)


@dag(default_args=default_args, catchup=False)
def sss_d2():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(PATH_FILE)
        return df
    
    @task(retries=4, retry_delay=timedelta(10))
    def data_preparation(df):
        df.Year = pd.to_datetime(df.Year, format="%Y")
        df_name_year = df.query('Year.notna()')[['Name', 'Year']] # отберем все строки с непустыми годами в отдельную таблицу
        df_name_year = df_name_year.drop_duplicates('Name') # удалим дубликаты
        df_name_year_dict = pd.Series(df.Year.values,index=df.Name).to_dict() # преобразуем данные в словарь
        df.Year = df.Name.map(df_name_year_dict) # заполним пропуски в данных по годам значениями из словаря
        df_name_Publisher = df.query('Publisher.notna()')[['Name', 'Publisher']] # отберем все строки с непустыми Publisher в отдельную таблицу
        df_name_Publisher = df_name_Publisher.drop_duplicates('Name') # удалим дубликаты
        df_name_Publisher_dict = pd.Series(df.Publisher.values,index=df.Name).to_dict() # преобразуем данные в словарь
        df.Publisher = df.Name.map(df_name_Publisher_dict) # заполним пропуски в данных по издателям из словаря
        df = df.dropna() # Удалим оставшиеся пропуски
        df_y = df.query('Year == @YEAR') # отберем данные за нужный год
        return df_y
    
    @task(retries=4, retry_delay=timedelta(10))
    def most_sales_game(df):
        # Какая игра была самой продаваемой в этом году во всем мире?
        #df_ = pd.read_csv(StringIO(df))
        most_sales_game = df.sort_values('Global_Sales', ascending=False).iloc[0,1]
        return most_sales_game

    @task(retries=4, retry_delay=timedelta(10))
    def most_eu_sales_genre(df):
        # Игры какого жанра были самыми продаваемыми в Европе?
        #df_ = pd.read_csv(StringIO(df))
        most_ue_sales_genre = df.sort_values('EU_Sales', ascending=False).iloc[0,4]
        return most_ue_sales_genre
    
    @task(retries=4, retry_delay=timedelta(10))
    def most_sales_game_platform(df):
        # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
        most_sales_game_platform = df.query('NA_Sales > 1').sort_values('Global_Sales', ascending=False).iloc[0,2]
        return most_sales_game_platform
    
    @task(retries=4, retry_delay=timedelta(10))
    def most_sales_publisher_jp(df):
        # У какого издателя самые высокие средние продажи в Японии?
        most_sales_publisher_jp = df \
            .groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .sort_values('JP_Sales', ascending=False) \
            .iloc[0,0]
        return most_sales_publisher_jp
    
    @task(retries=4, retry_delay=timedelta(10))
    def count_games_eu_gr_jp(df):
        # Сколько игр продались лучше в Европе, чем в Японии?
        count_games_eu_gr_jp = len(df.query('EU_Sales > JP_Sales'))
        return count_games_eu_gr_jp
    
    @task() #(on_success_callback=send_message)
    def print_data(q_1, q_2, q_3, q_4, q_5):
        context = get_current_context()
        print(f'Самая продаваемая в {YEAR} году во всем мире: {q_1}')
        print(f'В {YEAR} году самыми продаваемыми были игры жанра: {q_2}')
        print(f'В {YEAR} году больше всего игр, которые продались более чем миллионным тиражом в Северной Америке было на платформе: {q_3}')
        print(f'В {YEAR} году издатель, у которого были самые высокие средние продажи в Японии: {q_4}')
        print(f'Количество игр, которые в {YEAR} году продались лучше в Европе, чем в Японии: {q_5}')

    
    data = get_data()
    prepared_data = data_preparation(data)
    
    q_1 = most_sales_game(prepared_data)
    q_2 = most_eu_sales_genre(prepared_data)
    q_3 = most_sales_game_platform(prepared_data)
    q_4 = most_sales_publisher_jp(prepared_data)
    q_5 = count_games_eu_gr_jp(prepared_data)
    
    print_data(q_1, q_2, q_3, q_4, q_5)

sss_d2 = sss_d2()
