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


vg ='/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year=1994+hash(f'a-shihanov-30')%23
default_args = {
    'owner': 'Александр Шиханов',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 16),
    'schedule_interval': '45 16 * * *'
}

@dag(default_args=default_args, catchup=False)
def A_Shikhanov():  
    @task()
    def get_data():
        vgsales = pd.read_csv(vg)
        vgsales.columns=vgsales.columns.str.lower()
        return vgsales
    @task()
    def get_top_game(vgsales):
        return vgsales.query('year == @year').groupby(['name', 'year'], as_index=False)\
                .agg({'global_sales':'sum'}).sort_values('global_sales', ascending = False).name.iloc[0]
    @task()
    def get_top_genre_eu(vgsales):
        return vgsales.query('year == @year').groupby(['genre'], as_index=False).agg({'eu_sales':'sum'})\
                                                .sort_values('eu_sales', ascending = False).genre.iloc[0]
    @task()
    def get_top_game_renge(vgsales):
        return vgsales.query('year == @year & na_sales>1') .groupby('platform', as_index = False)\
                                    .agg({'na_sales': 'sum'}).sort_values('na_sales', ascending = False)\
                                    .query('na_sales>1').sort_values('na_sales', ascending = False)
    @task()
    def get_top_game_mean_jp(vgsales):
        return vgsales.query('year == @year').groupby('publisher', as_index=False).agg({'jp_sales':'mean'})\
                                                .sort_values('jp_sales', ascending = False).publisher.iloc[0]
    @task()
    def get_game_sale_eu_jp(vgsales):
        return (vgsales.query('year == @year').eu_sales > vgsales.query("year == @year").jp_sales).sum()
    @task()
    def print_data(top_game, top_genre_eu, top_game_renge, top_game_mean_jp, game_sale_eu_jp):
        print(
            f'''
             -------------------------------------------------------------------------------------------------------------
             Какая игра была самой продаваемой в этом году во всем мире?{year}:{top_game}
             -------------------------------------------------------------------------------------------------------------
             Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько {year}:{top_genre_eu}
             -------------------------------------------------------------------------------------------------------------
             На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
             Перечислить все, если их несколько {year}:{top_game_renge}
             -------------------------------------------------------------------------------------------------------------
             У какого издателя самые высокие средние продажи в Японии? Перечислить все,
             если их несколько {year}:{top_game_mean_jp}
             -------------------------------------------------------------------------------------------------------------
             Сколько игр продались лучше в Европе, чем в Японии? {year}:{game_sale_eu_jp}
             -------------------------------------------------------------------------------------------------------------
             '''
             )
    vgsales=get_data()
    top_game=get_top_game(vgsales)
    top_genre_eu=get_top_genre_eu(vgsales)
    top_game_renge=get_top_game_renge(vgsales)
    top_game_mean_jp=get_top_game_mean_jp(vgsales)
    game_sale_eu_jp=get_game_sale_eu_jp(vgsales)
    print_data(top_game, top_genre_eu, top_game_renge, top_game_mean_jp, game_sale_eu_jp)

A_Shikhanov=A_Shikhanov()

