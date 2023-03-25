import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash('i-fomina-27') % 23
default_args = {'owner': 'a.batalov',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2021, 2, 24)}


@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def fomina_vgsales():
    @task(retries=3)
    def get_data():
        vgsales = pd.read_csv(path)
        vgsales_2015 = vgsales.query(f'Year == {year}').reset_index().drop(columns='index', axis=1)
        return vgsales_2015

    @task(retries=3)
    def get_popular_game(vgsales_2015):
        popular_game = vgsales_2015.groupby('Name', as_index=False).agg({'Global_Sales': 'sum'}).sort_values('Global_Sales', ascending=False).head(1).reset_index().drop(columns='index', axis=1)
        return popular_game

    @task(retries=3)
    def get_europe_genres(vgsales_2015):
        europe_genres = vgsales_2015.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}).sort_values('EU_Sales', ascending=False).head(1).reset_index().drop(columns='index', axis=1)
        return europe_genres

    @task(retries=3)
    def get_na_platform(vgsales_2015):
        na_platform = vgsales_2015.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'NA_Sales': 'count'}).sort_values('NA_Sales', ascending=False).head(1).reset_index().drop(columns='index', axis=1)
        return na_platform

    @task(retries=3)
    def get_jp_avg_sales(vgsales_2015):
        jp_avg_sales = vgsales_2015.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).sort_values('JP_Sales', ascending=False).head(1).reset_index().drop(columns='index', axis=1)
        return jp_avg_sales

    
    @task(retries=3)
    def get_eu_better_jp(vgsales_2015):
        eu_better_jp = vgsales_2015.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'}).sort_values('EU_Sales', ascending=False).query('EU_Sales > JP_Sales')
        return eu_better_jp

    @task(retries=3)
    def print_data(a, b, c, d, e):
        print(f'Самой продаваемой игрой во всем мире в 2015 году была игра "{a.Name[0]}".')
        print(f'\nВ 2015 году самыми продаваемыми были игры жанра "{b.Genre[0]}".')
        print(f'\nВ Северной Америке в 2015 году на платформе "{c.Platform[0]}" было больше всего игр, которые продались более чем миллионным тиражом.')
        print(f'\nВ Японии в 2015 году самые высокие средние продажи были у издателя "{d.Publisher[0]}".')
        print(f'\nВ 2015 году {e.shape[0]} игр продались в Европе лучше, чем в Японии.')
        

    vgsales_2015 = get_data()
    popular_game = get_popular_game(vgsales_2015)
    europe_genre = get_europe_genres(vgsales_2015)
    na_platform  = get_na_platform(vgsales_2015)
    jp_avg_sales = get_jp_avg_sales(vgsales_2015)
    eu_better_jp = get_eu_better_jp(vgsales_2015)

    print_data(popular_game, europe_genre, na_platform, jp_avg_sales, eu_better_jp)
    
my_dag = fomina_vgsales()

