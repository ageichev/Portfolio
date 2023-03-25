import pandas as pd
import numpy as np
from io import BytesIO, StringIO
from datetime import timedelta, datetime
from zipfile import ZipFile

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


year = 1994 + hash(f'o-lokotilova') % 23
path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'o-lokotilova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 6),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)

def o_lokotilova_hw_3():
    
    @task(retries=3)
    def get_data():    
        df = pd.read_csv(path)
        vgsales = df[df.Year==year]
        return vgsales.to_csv(index=False)

    @task(retries=3)
    # Какая игра была самой продаваемой в этом году во всем мире?
    def top_global(vgsales):
        df = pd.read_csv(StringIO(vgsales))
#         df = pd.read_csv(StringIO(df))
        tg = df.groupby('Name').Global_Sales.sum().idxmax()
        return tg

    @task(retries=3)
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def top_eu_genre(vgsales):
        df = pd.read_csv(StringIO(vgsales))
        genres = df.groupby('Genre').EU_Sales.sum()
        top_genres = genres[genres==genres.max()]
        return top_genres.to_csv(index=False)
    
    @task(retries=3)
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    # Перечислить все, если их несколько
    def pop_platforms(vgsales):
        df = pd.read_csv(StringIO(vgsales))
        platforms = df.groupby(['Platform', 'Name']).NA_Sales.sum().reset_index() \
            .query('NA_Sales > 1') \
            .groupby('Platform').Name.count() \
            .sort_values(ascending=False)
        return platforms[platforms==platforms.max()].to_csv()    
    
    @task(retries=3)
    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    def top_jp(vgsales):
        df = pd.read_csv(StringIO(vgsales))
        top_avg_jp = df.groupby('Publisher').JP_Sales.mean().sort_values(ascending=False)
        return top_avg_jp[top_avg_jp==top_avg_jp.max()].to_csv()
    
    @task(retries=3)
    # Сколько игр продались лучше в Европе, чем в Японии?
    def eu_beats_jp(vgsales):
        df = pd.read_csv(StringIO(vgsales))
        res = df.groupby('Name')[['EU_Sales', 'JP_Sales']].sum() \
            .query('EU_Sales > JP_Sales').shape[0]
        return res

    
    @task(retries=3)
    def print_data(top_global, top_eu_genre, pop_platforms, top_jp, res):
        context = get_current_context()   # dict 
        date = context['ds']
        
        print(f'Top_global_game: {top_global}')
        print(f'Top EU genres: {top_eu_genre}')
        print(f'Best NA platforms: {pop_platforms}')
        print(f'Top JP publishers: {top_jp}')
        print(f'EU beated JP games: {res}')

    df = get_data()
    top_global = top_global(df)
    top_eu_genre = top_eu_genre(df)
    pop_platforms = pop_platforms(df)
    top_jp = top_jp(df)
    res = eu_beats_jp(df)
    print_data(top_global, top_eu_genre, pop_platforms, top_jp, res)
    
my_dag = o_lokotilova_hw_3()
