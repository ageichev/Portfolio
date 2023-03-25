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
year=2007+hash(f'i-bezhentsev')%23
default_args = {
    'owner': 'Ilya Bezhentsev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 22),
    'schedule_interval': '15 10 * * *'
}

@dag(default_args=default_args, catchup=False)
def I_Bezhentsev():
    @task()
    def get_data():
        vgsales = pd.read_csv(vg)
        vgsales.columns=vgsales.columns.str.lower()
        return vgsales
    @task()
    def get_top_sale(vgsales):
        return vgsales.query('year == @year').groupby(['name', 'year'], as_index=False)\
                .agg({'global_sales':'sum'}).sort_values('global_sales', ascending = False).head(1)
    @task()
    def get_top_EU_genre(vgsales):
        return vgsales.query('year == @year').groupby(['genre'], as_index=False).agg({'eu_sales':'sum'})\
                                                .sort_values('eu_sales', ascending = False).genre.head(3)
    @task()
    def get_top_NA_platform(vgsales):
        return vgsales.query('year == @year & na_sales>=1') .groupby('platform', as_index = False)\
                                    .agg({'na_sales': 'sum'}).sort_values('na_sales', ascending = False)\
                                    .query('na_sales>=1').sort_values('na_sales', ascending = False).head(3)
    @task()
    def get_top_JP_mean(vgsales):
        return vgsales.query('year == @year').groupby('publisher', as_index=False).agg({'jp_sales':'mean'})\
                                                .sort_values('jp_sales', ascending = False).publisher.head(3)
    @task()
    def get_EU_over_JP(vgsales):
        return (vgsales.query('year == @year').eu_sales > vgsales.query("year == @year").jp_sales).sum()
    @task()
    def print_data(top_sale, top_EU_genre, top_NA_platform, top_JP_mean, EU_over_JP):
        print(
            f'''
             -------------------------------------------------------------------------------------------------------------
             Какая игра была самой продаваемой в этом году во всем мире?{year}:{top_sale}
             -------------------------------------------------------------------------------------------------------------
             Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько {year}:{top_EU_genre}
             -------------------------------------------------------------------------------------------------------------
             На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
             Перечислить все, если их несколько {year}:{top_NA_platform}
             -------------------------------------------------------------------------------------------------------------
             У какого издателя самые высокие средние продажи в Японии? Перечислить все,
             если их несколько {year}:{top_JP_mean}
             -------------------------------------------------------------------------------------------------------------
             Сколько игр продались лучше в Европе, чем в Японии? {year}:{EU_over_JP}
             -------------------------------------------------------------------------------------------------------------
             '''
        )

    vgsales=get_data()
    top_sale=get_top_sale(vgsales)
    top_EU_genre=get_top_EU_genre(vgsales)
    top_NA_platform=get_top_NA_platform(vgsales)
    top_JP_mean=get_top_JP_mean(vgsales)
    EU_over_JP=get_EU_over_JP(vgsales)
    print_data(top_sale, top_EU_genre, top_NA_platform, top_JP_mean, EU_over_JP)

I_Bezhentsev = I_Bezhentsev()