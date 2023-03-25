import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'al-savelev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 7),
    'schedule_interval': '0 12 * * *'
}


@dag(default_args=default_args, catchup=False)
def dag_savelev_les_3():

    @task()
    def get_data():
        df = pd.read_csv(file)   
        login = 'al-savelev'
        year = 1994 + hash(f'{login}') % 23
        df_year = df.query('Year == @year')
        return df_year

    @task()
    def world_best_game(df_year):
        return df_year.groupby('Name')\
                      .Global_Sales\
                      .sum()\
                      .idxmax()

    @task()
    def eu_best_genre(df_year):
        return df_year.groupby('Genre')\
                      .EU_Sales\
                      .sum()\
                      .idxmax()

    @task()
    def na_mil_sales(df_year):
        return df_year.groupby('Platform', as_index=False)\
                      .NA_Sales\
                      .sum()\
                      .sort_values(by='NA_Sales', ascending=False)\
                      .query('NA_Sales > 1')

    @task()
    def jp_best_publisher(df_year):
        return df_year.groupby('Publisher')\
                      .JP_Sales\
                      .sum()\
                      .sort_values()\
                      .idxmax()

    @task()
    def eu_jp_better_sales(df_year):
        return df_year.groupby('Name', as_index=False)\
                      .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})\
                      .query('EU_Sales > JP_Sales')\
                      .shape[0]

    @task()
    def print_res(world_best_game, eu_best_genre, na_mil_sales, jp_best_publisher, eu_jp_better_sales):
        login = 'al-savelev'
        year = 1994 + hash(f'{login}') % 23
        print(f'The world bestseller game in {year} year was {world_best_game}.')
        print(f'The EU bestseller genre in {year} year was {eu_best_genre}.')
        print(f'The NA over million sales in {year} year were:\n{na_mil_sales}')
        print(f'The JP best game publisher in {year} year was {jp_best_publisher}.')
        print(f'The number of games with better sales in EU than in JP in {year} year was {eu_jp_better_sales}.')

    df_year = get_data()
    world_best_game = world_best_game(df_year)
    eu_best_genre = eu_best_genre(df_year)
    na_mil_sales = na_mil_sales(df_year)
    jp_best_publisher = jp_best_publisher(df_year)
    eu_jp_better_sales = eu_jp_better_sales(df_year)
    print_res(world_best_game, eu_best_genre, na_mil_sales, jp_best_publisher, eu_jp_better_sales)    
    
dag_savelev_les_3 = dag_savelev_les_3()