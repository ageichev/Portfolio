import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'e-lapin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
}
schedule_interval = '0 12 * * *'


@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def top_games_lapin():
    @task(retries=3)
    def get_data():
        link = 'https://drive.google.com/file/d/1dqesdMfFgoAX9gHfOmZ-sU5Rp66Hxbdr/view?usp=drivesdk'
        path = 'https://drive.google.com/uc?export=download&id='+link.split('/')[-2]
        top_games = pd.read_csv(path)
        return top_games

    @task(retries=3)    
    def get_best_game(top_games):
        return top_games.query('Year == 2014').sort_values('Global_Sales', ascending=False).Name.values[0]

    @task(retries=3)    
    def get_top_games_gengre_eu(top_games):
        top_games_gengre_eu = top_games\
            .query('Year == 2014')\
            .groupby(['Genre'])\
            .agg({'EU_Sales': 'sum'})\
            .reset_index()

        max_gengre_eu = top_games_gengre_eu.EU_Sales.max()
        max_gengre_eu
        A = top_games_gengre_eu.query('EU_Sales == @max_gengre_eu').Genre.tolist()
        StrA = " ".join(A)
        StrA

        return StrA

    @task(retries=3)
    def get_top_platform_na(top_games):
        top_games['over_mil'] = np.where(top_games.NA_Sales > 1, 1, 0)
        top_platform_na = top_games\
            .query('Year == 2014')\
            .groupby(['Platform'])\
            .agg({'over_mil': 'sum'})\
            .reset_index()

        max_over_mil = top_platform_na.over_mil.max()
        max_over_mil
        A = top_platform_na.query('over_mil == @max_over_mil').Platform.tolist()
        StrA = " ".join(A)
        StrA

        return StrA
    
    @task(retries=3)
    def get_top_publisher_jp(top_games):
        top_publisher_jp = top_games\
            .query('Year == 2014')\
            .groupby(['Publisher'])\
            .agg({'JP_Sales':'mean'})\
            .reset_index()

        max_publisher_jp_mean = top_publisher_jp.JP_Sales.max()
        max_publisher_jp_mean
        top_publisher_jp.query('JP_Sales == @max_publisher_jp_mean').Publisher
        A = top_publisher_jp.query('JP_Sales == @max_publisher_jp_mean').Publisher.tolist()
        StrA = " ".join(A)
        StrA

        return StrA
    
    @task(retries=3)
    def get_eu_better_jp(top_games):
        top_games['eu_better_jp'] = np.where(top_games.EU_Sales > top_games.JP_Sales, 1, 0)
        return top_games.query('Year == 2014').eu_better_jp.sum()
    
    @task(retries=3)
    def print_data(pr_best_game, pr_top_games_gengre_eu, pr_top_platform_na, pr_top_publisher_jp, pr_eu_better_jp):
        year = 1994 + hash(f'e-lapin') % 23
        print(f'Самая продаваемаая игра во всем мире в {year}: {pr_best_game}')
        print(f'Самые продаваемые жанры в европе в {year}: {pr_top_games_gengre_eu}')
        print(f'Платформа, на которой в NA больше всего игр с миллионым тиражом в {year}: {pr_top_platform_na}')   
        print(f'Издатель с самыми высокими продажами в Япноии в {year}: {pr_top_publisher_jp}')       
        print(f'Количество игр, которые продались в Европе лучше чем в Япноии в {year}: {pr_eu_better_jp}')

    t = get_data()
    best_game = get_best_game(t)
    top_games_gengre_eu = get_top_games_gengre_eu(t)
    top_platform_na = get_top_platform_na(t)
    top_publisher_jp = get_top_publisher_jp(t)
    eu_better_jp = get_eu_better_jp(t)

    print_data(best_game, top_games_gengre_eu, top_platform_na, top_publisher_jp, eu_better_jp)
    
top_games_lapin = top_games_lapin()