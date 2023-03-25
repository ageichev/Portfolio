#Update 02/08 23-24
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.models import Variable

#исходные данные
FILE = 'vgsales.csv'

#получение данных + заданный условием год
def get_data():
    year = 1994 + hash(f'j-postnova') % 23
    games_data = pd.read_csv("vgsales.csv").query("Year == @year")
    return games_data

# Какая игра была самой продаваемой в этом году во всем мире?
def best_world_game (games_data):
    best_seller = games_data.groupby('Name', as_index=False).\
    agg('Global_Sales').sum().sort_values('Global_Sales', ascending=False)
    best_seller = best_seller.Name[0]
    return best_seller

# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
def best_eu_games (games_data):
    best_eu_genre = games_data.groupby('Genre', as_index=False).\
    agg('EU_Sales').sum().sort_values('EU_Sales', ascending=False).Genre[0]
    return best_eu_genre

# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
# Перечислить все, если их несколько
def best_platform (games_data):
    global_sales = games_data.groupby(['Name', 'Platform'], as_index=False).agg('Global_Sales').sum()
    global_sales_1 = global_sales[global_sales['Global_Sales'] > 1].\
    groupby('Platform', as_index=False).agg('Name').count().sort_values('Name', ascending=False).reset_index().Platform[0]
    return global_sales_1

# У какого издателя самые высокие средние продажи в Японии?
# Перечислить все, если их несколько

def best_mean_sales (games_data):
    best_mean_sales_JP = games_data.groupby('Publisher', as_index=False).\
    agg('JP_Sales').mean().sort_values('JP_Sales', ascending=False).max().Publisher
    return best_mean_sales_JP


# Сколько игр продались лучше в Европе, чем в Японии?
def best_diff (games_data):
    games_data['diff'] = games_data['EU_Sales'] - games_data['JP_Sales']
    diff = games_data[games_data['diff'] > 0].Name.count()
    return diff

# печать данных
def print_data (games_data):
    year = 1994 + hash(f'j-postnova') % 23
    print (f"Самая продаваемая игра в {'year'} году {'best_seller'}")
    print (f"Игры жанра  {'best_eu_genre'} были самыми продаваемыми в {'year'} году")
    print (f"Более 1 млн игр продалось в {'year'} году в СА на платформе {'global_sales_1'}")
    print (f"Самые высокие средние продажи в {'year'} году в Японии были у издателя {'best_mean_sales_JP'}")
    print (f"В {'year'} году в Европе лучше, чем в Японии продалост {'diff'} игр")

default_args = {
    'owner': 'j.postnova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
}
schedule_interval = '0 12 * * *'

dag = DAG('games.postnova', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='best_world_game',
                    python_callable=best_world_game,
                    dag=dag)

t3 = PythonOperator(task_id='best_eu_games',
                    python_callable=best_eu_games,
                    dag=dag)

t4 = PythonOperator(task_id='best_platform',
                    python_callable=best_platform,
                    dag=dag)

t5 = PythonOperator(task_id='best_mean_sales',
                    python_callable=best_mean_sales,
                    dag=dag)
t6 = PythonOperator(task_id='best_diff',
                    python_callable=best_diff,
                    dag=dag)
t7 = PythonOperator(task_id='print_data',
                    python_callable=best_diff,
                    dag=dag)

t1 >> [t2, t3, t4, t5, t6] >> t7