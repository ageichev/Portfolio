import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import json
from urllib.parse import urlencode

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# Параметры Airflow
default_args = {
    'owner': 'i-mosin-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=4),
    'start_date': datetime(2022, 9, 4),
    'schedule_interval': '0 12 * * *'
}

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'i-mosin-23'
year = 1994 + hash(f'{login}') % 23

@dag(default_args=default_args, catchup=False)
def i_mosin_23_3():
    @task()
    def get_data(path, year):
        df = pd.read_csv(path)
        year_data = df[df.Year == year]
        return year_data.to_csv(index=False)
    
    #Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def best_global_game(year_data):
        df = pd.read_csv(StringIO(year_data))
        bgg = df.groupby('Name').Global_Sales.sum().to_frame().Global_Sales.idxmax()
        return bgg
    
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def best_eu_genres(year_data):
        df = pd.read_csv(StringIO(year_data))
        grouped_eu_df = df.groupby('Genre').EU_Sales.sum().to_frame()
        max_eu_sales = grouped_eu_df.EU_Sales.max()
        eu_best = grouped_eu_df[grouped_eu_df.EU_Sales == max_eu_sales]
        genres = ', '.join(eu_best.index)
        return genres
    
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    #Перечислить все, если их несколько
    @task()
    def best_na_platforms(year_data):
        df = pd.read_csv(StringIO(year_data))
        grouped_na_df = df.loc[df.NA_Sales > 1].groupby('Platform').Name.count().to_frame()
        max_na_count = grouped_na_df.Name.max()
        na_best = grouped_na_df[grouped_na_df.Name == max_na_count]
        platforms = ', '.join(na_best.index)
        return platforms
    
    #У какого издателя самые высокие средние продажи в Японии? 
    #Перечислить все, если их несколько
    @task()
    def best_jp_publishers(year_data):
        df = pd.read_csv(StringIO(year_data))
        grouped_jp_df = df.groupby('Publisher').JP_Sales.mean().to_frame()
        max_jp_mean = grouped_jp_df.JP_Sales.max()
        jp_best = grouped_jp_df[grouped_jp_df.JP_Sales == max_jp_mean]
        publishers = ', '.join(jp_best.index)
        return publishers
    
    #Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def eu_jp_compare(year_data):
        df = pd.read_csv(StringIO(year_data))
        grouped_eu_jp_sum = df.groupby('Name', as_index=False) \
                              .agg(eu_sum=('EU_Sales', 'sum'), jp_sum=('JP_Sales', 'sum'))
        grouped_eu_jp_sum['task_cond'] = np.where(grouped_eu_jp_sum.eu_sum > grouped_eu_jp_sum.jp_sum, True, False)
        eu_best_jp_counter = grouped_eu_jp_sum[grouped_eu_jp_sum.task_cond].shape[0]
        return str(eu_best_jp_counter)

    @task()
    def print_data(best_game, eu_genres, na_platforms, jp_publishers, eu_vs_jp):
        context = get_current_context()
        date = context['ds']
        print(f'Random game year report at {date}')
        print(f'''
            Gaming Top {year}:
            The best selling game in the world: {best_game}
            Top selling game genres in Europe: {eu_genres}
            Top platforms in North America: {na_platforms}
            Publishers with the highest average sales in Japan: {jp_publishers}
            Number of games that sold better in Europe than in Japan: {eu_vs_jp}''')


    year_data = get_data(path, year)

    best_game = best_global_game(year_data)
    eu_genres = best_eu_genres(year_data)
    na_platforms = best_na_platforms(year_data)
    jp_publishers = best_jp_publishers(year_data)
    eu_vs_jp = eu_jp_compare(year_data)

    print_data(best_game, eu_genres, na_platforms, jp_publishers, eu_vs_jp)

i_mosin_23_les3 = i_mosin_23_3()