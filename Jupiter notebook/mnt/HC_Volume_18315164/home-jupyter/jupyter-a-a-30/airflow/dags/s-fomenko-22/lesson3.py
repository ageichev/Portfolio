import pandas as pd
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

file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year=1994+hash(f'{"s-fomenko-22"}')%23

default_args = {
    'owner': 's-fomenko-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 2),
    'schedule_interval': '6 6 * * *'
}

@dag(default_args=default_args, catchup=False)
def games_s_fomenko_22():
    @task(retries=3)
    def get_data():
        games_year=pd.read_csv(file).query("Year == @year")
        return games_year
    
    @task()
    def world_top_sales(games_year):
        world_sales = games_year\
            .groupby('Name',as_index=False)\
            .agg(total_sales=('Global_Sales','sum'))
        top_sale = world_sales\
            [world_sales['total_sales']==world_sales['total_sales'].max()]\
            .Name.to_csv(index=False, header=False)
        return top_sale
    
    @task()
    def EU_genre(games_year):
        EUsales_by_genre = games_year\
            .groupby('Genre',as_index=False)\
            .agg(total_sales=('EU_Sales','sum'))\
            .sort_values(by='total_sales', ascending=False)
        max_sale=EUsales_by_genre['total_sales'].max()
        top_EU_genre = EUsales_by_genre\
            [EUsales_by_genre['total_sales']>=max_sale]['Genre'].to_csv()
        return top_EU_genre
    
    @task()
    def NA_platfrom(games_year):
        games_by_platform = games_year\
            .query('NA_Sales>1')\
            .groupby('Platform',as_index=False)\
            .agg(total_games=('Name','count'))
        topNA_platform = games_by_platform\
            [games_by_platform['total_games']==games_by_platform['total_games'].max()]
        NA_platform = topNA_platform['Platform'].to_csv()
        return NA_platform
    
    @task()
    def JP_pub(games_year):
        JPsales_pub = games_year\
            .groupby('Publisher',as_index=False)\
            .agg(total_sales=('JP_Sales','sum'))
        JP_topPub = JPsales_pub\
            [JPsales_pub['total_sales']==JPsales_pub['total_sales'].max()]
        publishers = JP_topPub['Publisher'].to_csv()
        return publishers
    
    @task()
    def EU_JP_sales(games_year):
        EU_vs_JP = games_year\
            .groupby('Name',as_index=False)\
            .agg(EU_sale=('EU_Sales','sum'), JP_Sale=('JP_Sales','sum'))
        games_count = len(EU_vs_JP[EU_vs_JP['EU_sale']>EU_vs_JP['JP_Sale']].index)
        return games_count
    
    @task()
    def print_data(top_sale, 
                   top_EU_genre, 
                   NA_platform, 
                   publishers, 
                   games_count):
        context = get_current_context() 
        date = context['ds']
        
        print(f'The best selling game in {year} in the world is {top_sale}')    
        print(f'The best selling genre in {year} in EU is {top_EU_genre}')     
        print(f'The best selling platform in {year} in NA is {NA_platform}')
        print(f'The best selling publisher in {year} in JP is {publishers}')               
        print(f'{games_count} in {year} sold better in EU than in JP') 
        
    games_year = get_data()
    
    top_sale = world_top_sales(games_year)
    top_EU_genre = EU_genre(games_year)
    NA_platform = NA_platfrom(games_year)
    publishers = JP_pub(games_year)
    games_count = EU_JP_sales(games_year)

    print_data(top_sale, top_EU_genre, NA_platform, publishers, games_count)

games_s_fomenko_22 = games_s_fomenko_22()