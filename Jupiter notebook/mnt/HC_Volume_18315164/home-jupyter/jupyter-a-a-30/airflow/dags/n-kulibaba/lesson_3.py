# Импортируем библиотеки
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# Подгружаем данные
df = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year_look = 1994 + hash(f'n-kulibaba') % 23


# Инициализируем DAG
default_args = {
    'owner': 'n.kulibaba',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 18),
    'schedule_interval': '0 24 * * *'
}
 
   
    
# Таски & dag
@dag(default_args=default_args, catchup=False)
def n_kulibaba_lesson_3():
    @task()
    def get_data():
        vgsales = pd.read_csv(df)
        return vgsales

    @task()
    def get_game(vgsales):
        task_1 = vgsales.query('Year == @year_look').groupby('Name', as_index=False).agg({'Global_Sales': 'sum'}) \
        .sort_values('Global_Sales', ascending=False).head(1).Name.iloc[0]
        return task_1

    @task()
    def get_genre(vgsales):
        task_2 = vgsales.query('Year == @year_look').groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}) \
        .sort_values('EU_Sales', ascending=False).head(1).Genre.iloc[0]
        return task_2

    @task() 
    def get_platform(vgsales):
        task_3 = vgsales.query('Year == @year_look and NA_Sales > 1').groupby('Platform', as_index=False) \
        .agg({'NA_Sales': 'count'}).sort_values('NA_Sales', ascending=False).head(1).Platform.iloc[0]
        return task_3

    @task()
    def get_published(vgsales):
        task_4 = vgsales.query('Year == @year_look').groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}) \
        .sort_values('JP_Sales', ascending=False).head(1).Publisher.iloc[0]
        return task_4

    @task()
    def get_number_games(vgsales):
        task_5 = vgsales.query('Year == @year_look').groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        task_5['diff_games'] = task_5.EU_Sales - task_5.JP_Sales
        task_5 = task_5.query('diff_games > 0').agg({'Name': 'count'}).iloc[0]
        return task_5
    
    @task()
    def print_data(task_1, task_2, task_3, task_4, task_5):
        print(f'''Смотрим данные за {year_look} год 
                  самая продаваемая игра в {year_look} году во всем мире {task_1} 
                  игры жанра {task_2} были самыми продаваемыми в Европе
                  платформе {task_3} было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке 
                  у издателя {task_4} самые высокие средние продажи в Японии
                  {task_5} игр продались лучше в Европе, чем в Японии''')     


    
# Задаем порядок выполнения         
    vgsales = get_data()
    task_1 = get_game(vgsales)
    task_2 = get_genre(vgsales)
    task_3 = get_platform(vgsales)
    task_4 = get_published(vgsales)
    task_5 = get_number_games(vgsales)
    print_data(task_1, task_2, task_3, task_4, task_5)

n_kulibaba_lesson_3 = n_kulibaba_lesson_3()