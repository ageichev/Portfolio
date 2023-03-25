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



PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'v-kachanovskij-24'
myYear = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'v-kachanovskij-24',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2021, 11, 13),
    'schedule_interval': '00 6 * * *'
}


@dag(default_args=default_args, catchup=False)
def v_kachanovskij_24_lesson3():
    @task(retries=3, retry_delay=timedelta(10))
    def v_kachanovskij_24_get_data():
        data = pd.read_csv(PATH)
        vg_my_df = data.query('Year == @myYear')
        return vg_my_df


    @task(retries=3, retry_delay=timedelta(10))
    def v_kachanovskij_24_max_sales_global(vg_my_df):
        max_sales_global = vg_my_df.groupby('Name',as_index = False) \
            .agg({'Global_Sales' : 'sum'}) \
            .sort_values('Global_Sales', ascending = False) \
            .iloc[0]['Name']
        return max_sales_global


    @task(retries=3, retry_delay=timedelta(10))
    def v_kachanovskij_24_eu_mp_sales(vg_my_df):
        eu_mp_sales = vg_my_df.groupby('Genre',as_index = False) \
            .agg({'EU_Sales' : 'sum'}) \
            .sort_values('EU_Sales', ascending = False) \
            .head(5)
        eu_mp_sales_top1 = eu_mp_sales.iloc[0]['Genre']
        return eu_mp_sales_top1


    @task(retries=3, retry_delay=timedelta(10))
    def v_kachanovskij_24_top_na_mp_platform(vg_my_df):
        na_mp_platform = vg_my_df.query('NA_Sales > 1') \
            .groupby('Platform', as_index = False) \
            .agg({'Name' : 'count'}) \
            .sort_values('Name', ascending = False)
        na_mp_platform_top1 = na_mp_platform.iloc[0]['Platform']
        return na_mp_platform_top1


    @task(retries=3, retry_delay=timedelta(10))
    def v_kachanovskij_24_jp_popular_publisher(vg_my_df):
        jp_popular_publisher = vg_my_df.groupby('Publisher',as_index = False) \
            .sort_values('JP_Sales', ascending = False)
        jp_popular_publisher_top1 = jp_popular_publisher.iloc[0]['Publisher']
        return jp_popular_publisher_top1


    @task(retries=3, retry_delay=timedelta(10))
    def v_kachanovskij_24_e_b_j(vg_my_df):
        e_b_j = vg_my_df.query('EU_Sales > JP_Sales').count()[0]
        return e_b_j


    @task(retries=3, retry_delay=timedelta(10))
    def v_kachanovskij_24_print_data(max_sales_global,eu_mp_sales_top1,na_mp_platform_top1,jp_popular_publisher_top1,e_b_j):
        
        context = get_current_context()
        date = context['ds']

        print(f'''{max_sales_global} - best-selling game in the world;
            
        {eu_mp_sales_top1} - best-selling genre of games in Europe;
        
        {na_mp_platform_top1} - the most games that sold more than a million copies in North America;
            
        {jp_popular_publisher_top1} - the highest average sales in Japan;
            
        {e_b_j} games sold better in Europe than in Japan;''')

    vg_my_df = v_kachanovskij_24_get_data()
    max_sales_global = v_kachanovskij_24_max_sales_global(vg_my_df)
    eu_mp_sales_top1 = v_kachanovskij_24_eu_mp_sales(vg_my_df)
    na_mp_platform_top1 = v_kachanovskij_24_top_na_mp_platform(vg_my_df)
    jp_popular_publisher_top1 = v_kachanovskij_24_jp_popular_publisher(vg_my_df)
    e_b_j = v_kachanovskij_24_e_b_j(vg_my_df)
    v_kachanovskij_24_print_data(max_sales_global,eu_mp_sales_top1,na_mp_platform_top1,jp_popular_publisher_top1,e_b_j)
        
v_kachanovskij_24_lesson3 = v_kachanovskij_24_lesson3()