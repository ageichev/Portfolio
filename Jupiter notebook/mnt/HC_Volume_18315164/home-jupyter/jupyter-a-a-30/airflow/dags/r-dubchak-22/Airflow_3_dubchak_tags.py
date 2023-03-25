import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'         
login = 'r-dubchak-22'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': login,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 29),
    'schedule_interval' : '0 14 * * *'
}
@dag(default_args=default_args,catchup=False)
def Airflow_3_dubchak_tags():
    @task()
    def get_data_dubchak():
        df = pd.read_csv(PATH)  
        df_year = df.query("Year==@year")          
        return df_year

    @task()
    def get_top_global_s_game_dubchak(df_year):
        global_s_game = df_year.groupby('Name',as_index=False)['Global_Sales'].sum()
        top_global_s_game = global_s_game[global_s_game.Global_Sales==global_s_game.Global_Sales.max()]['Name']
        top_global_s_game = top_global_s_game.to_csv(index=False, header=False)
        return top_global_s_game


    @task()
    def get_top_genre_EU_dubchak(df_year):
        eu_s_genre = df_year.groupby('Genre',as_index=False)['EU_Sales'].sum()
        top_genre_EU = eu_s_genre[eu_s_genre.EU_Sales==eu_s_genre.EU_Sales.max()]['Genre']
        top_genre_EU = top_genre_EU.to_csv(index=False, header=False)
        return top_genre_EU


    @task()
    def get_top_platform_NA_dubchak(df_year):
        game_Platform_NA_best =df_year.query("NA_Sales>1").groupby('Platform',as_index=False)['Name'].count()
        top_platform_NA = game_Platform_NA_best[game_Platform_NA_best.Name==game_Platform_NA_best.Name.max()]['Platform']
        top_platform_NA = top_platform_NA.to_csv(index=False, header=False)
        return top_platform_NA


    @task()
    def get_top_publisher_JP_dubchak(df_year):
        publisher_mean_jp = df_year.groupby('Publisher',as_index=False)['JP_Sales'].mean()
        top_publisher_JP = publisher_mean_jp[publisher_mean_jp.JP_Sales==publisher_mean_jp.JP_Sales.max()]['Publisher']
        top_publisher_JP = top_publisher_JP.to_csv(index=False, header=False)
        return top_publisher_JP


    @task()       
    def get_best_game_EU_JP_dubchak(df_year):
        game_salse_EU_JP = df_year.groupby('Name',as_index=False)[['EU_Sales','JP_Sales']].sum()
        best_game_EU_JP = (game_salse_EU_JP.EU_Sales>game_salse_EU_JP.JP_Sales).sum()
        data = {'count':[best_game_EU_JP]}
        best_game_EU_JP = pd.DataFrame(data)
        best_game_EU_JP = best_game_EU_JP.to_csv(index=False, header=False)
        return best_game_EU_JP


    @task()    
    def output_info_dubchak(top_global_s_game,top_genre_EU,top_platform_NA,top_publisher_JP,best_game_EU_JP):

        date = year

        print(f'Top game by Global Sales in {date}')
        print(top_global_s_game)

        print(f'Top Genre by EU Sales in {date}')
        print(top_genre_EU)

        print(f'Top Platform by NA Sales in {date}')
        print(top_platform_NA) 

        print(f'Top Publisher by JP Sales in {date}')
        print(top_publisher_JP)   

        print(f'Count EU Sales then >  JP Sales in {date}')
        print(best_game_EU_JP)   
    
    df_year=get_data_dubchak()    
    top_global_s_game = get_top_global_s_game_dubchak(df_year)   
    top_genre_EU = get_top_genre_EU_dubchak(df_year)
    top_platform_NA = get_top_platform_NA_dubchak(df_year)
    top_publisher_JP = get_top_publisher_JP_dubchak(df_year)
    best_game_EU_JP = get_best_game_EU_JP_dubchak(df_year)
    output_info_dubchak(top_global_s_game,top_genre_EU,top_platform_NA,top_publisher_JP,best_game_EU_JP)

Airflow_3_dubchak_tags = Airflow_3_dubchak_tags()