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

default_args = {
    'owner': 'p-annenkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 4)
}

schedule_interval = '0 12 * * *'

login = 'p-annenkov'
year = 1994 + hash(f'{login}') % 23
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def games_stats_p_annenkov():
    @task()
    def get_data():
        vgsales_path = "/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv"
        vgsales = pd.read_csv(vgsales_path)
        vgsales = vgsales.query("Year == @year")
        
        return vgsales

    @task()
    def get_best_selling_game(vgsales):
        
        best_selling_game_csv = pd.DataFrame()
        best_selling_game_csv = best_selling_game_csv.append(pd.Series(vgsales.groupby('Name', as_index=False).agg({'Global_Sales': 'sum'}).sort_values('Global_Sales', ascending=False).reset_index(drop=True)['Name'][0]), ignore_index=True)
        best_selling_game_csv = best_selling_game_csv.rename(columns={0: 'best_selling_game'})
        
        return best_selling_game_csv.to_csv(index=False)

    @task()
    def get_top_genre_eu(vgsales):
        
        top_genre_eu_csv = pd.DataFrame()
        top_genre_eu_csv = top_genre_eu_csv.append(pd.Series(vgsales.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending=False).reset_index(drop=True)['Genre']), ignore_index=True)
        top_genre_eu_csv = top_genre_eu_csv.rename(columns={0: 'top_genre_eu'})
        
        return top_genre_eu_csv.to_csv(index=False)

    @task()
    def get_top_platform_na(vgsales):
        
        top_platform_na_csv = pd.DataFrame()
        top_platform_na_csv = top_platform_na_csv.append(pd.Series(vgsales.query("NA_Sales > 0").groupby('Platform', as_index=False).agg({'NA_Sales': 'sum'}).sort_values('NA_Sales', ascending=False).reset_index(drop=True)['Platform']), ignore_index=True)
        top_platform_na_csv = top_platform_na_csv.rename(columns={0: 'top_platform_na'})
        
        return top_platform_na_csv.to_csv(index=False)

    @task()
    def get_publisher_jp_mean(vgsales):
        
        publisher_jp_mean_csv = pd.DataFrame()
        publisher_jp_mean_csv = publisher_jp_mean_csv.append(pd.Series(vgsales.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False).reset_index(drop=True)['Publisher']), ignore_index=True)
        publisher_jp_mean_csv = publisher_jp_mean_csv.rename(columns={0: 'publisher_jp_mean'})
        
        return publisher_jp_mean_csv.to_csv(index=False)
    
    @task()
    def get_count_game_eu_jp(vgsales):
        
        vgsales['name_copy'] = vgsales['Name']

        count_game_eu_jp_csv = pd.DataFrame()
        count_game_eu_jp_csv = count_game_eu_jp_csv.append(pd.Series(vgsales.query("EU_Sales > JP_Sales").groupby('Name', as_index=False).agg({'name_copy': 'count'}).shape[0]), ignore_index=True)
        count_game_eu_jp_csv = count_game_eu_jp_csv.rename(columns={0: 'count_game_eu_jp'})
        
        return count_game_eu_jp_csv.to_csv(index=False)

    @task()
    def print_data(best_selling_game_csv, top_genre_eu_csv, top_platform_na_csv, publisher_jp_mean_csv, count_game_eu_jp_csv):

        context = get_current_context()
        date = context['ds']

        print(f'''Data for {year}
                  Which game was the best-selling this year worldwide?: {best_selling_game_csv}
                  What genre of games were the best-selling in Europe?: {top_genre_eu_csv}
                  Which platform had the most games that sold more than a million copies in North America?: {top_platform_na_csv}
                  Which publisher has the highest average sales in Japan?: {publisher_jp_mean_csv}
                  How many games have sold better in Europe than in Japan?: {count_game_eu_jp_csv}
                  ''')

    vgsales = get_data()
    
    best_selling_game_csv = get_best_selling_game(vgsales)
    top_genre_eu_csv = get_top_genre_eu(vgsales)
    top_platform_na_csv = get_top_platform_na(vgsales)
    publisher_jp_mean_csv = get_publisher_jp_mean(vgsales)
    count_game_eu_jp_csv = get_count_game_eu_jp(vgsales)

    print_data(best_selling_game_csv, top_genre_eu_csv, top_platform_na_csv, publisher_jp_mean_csv, count_game_eu_jp_csv)

games_stats_p_annenkov = games_stats_p_annenkov()