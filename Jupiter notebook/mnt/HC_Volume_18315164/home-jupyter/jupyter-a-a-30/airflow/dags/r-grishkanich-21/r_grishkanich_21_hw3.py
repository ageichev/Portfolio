import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'r-grishkanich-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 2),
    'schedule_interval': '0 20 * * *'
}

my_year = 1994 + hash(f'r-grishkanich-21') % 23

@dag(default_args=default_args, catchup=False)
def r_grishkanich_21_hmw3():
    
    @task()
    def get_all_data():
        df = pd.read_csv(vgsales).query('Year == @my_year')
        return df

    @task()
    def get_best_sales_game(df):
        best_sales_game = df.groupby('Name').agg({'Global_Sales':'sum'}).sort_values('Global_Sales', ascending = False).idxmax()
        return best_sales_game
    
    @task()
    def get_eu_best_sales_genre(df):
        best_eu_genre = (df.groupby('Genre')
                        .agg({'EU_Sales':'sum'})
                        .sort_values('EU_Sales', ascending = False)
                        .idxmax())
        return best_eu_genre
    
    @task()
    def get_na_best_platform(df):
        best_na_platform = (df.query('NA_Sales > 1')
                           .groupby('Platform')
                           .agg({'NA_Sales':'count'})
                           .sort_values('NA_Sales', ascending = False)
                           .idxmax())
        return best_na_platform
    
    @task()
    def get_jp_best_publisher(df):
        best_jp_publisher = (df.groupby('Publisher')
                            .agg({'JP_Sales':'mean'})
                            .sort_values('JP_Sales', ascending = False)
                            .idxmax())
        return best_jp_publisher
    
    @task
    def get_count_game_eu_vs_jp(df):
        best_eu_game_vs_jp = df.query('EU_Sales > JP_Sales').agg({'Name':'count'})
        return best_eu_game_vs_jp
    
    @task
    def print_data(bestgame, best_genre, best_platform, best_publisher, best_eu_game):
        print(f'''
            Top global sales game in {my_year}: {best_sales_game}
            Top genre in EU in {my_year}: {best_eu_genre}
            Top platform in North America in {my_year}: {best_na_platform}
            Top publisher in Japan in {my_year}: {best_jp_publisher}
            Number of Games EU vs. JP in {my_year}: {best_eu_game_vs_jp}''')
    
    df = get_all_data()

    best_sales_game = get_best_sales_game(df)
    best_eu_genre = get_eu_best_sales_genre(df)
    best_na_platform = get_na_best_platform(df)
    best_jp_publisher = get_jp_best_publisher(df)
    best_eu_game_vs_jp = get_count_game_eu_vs_jp(df)

    print_data(best_sales_game, best_eu_genre, best_na_platform, best_jp_publisher, best_eu_game_vs_jp)


r_grishkanich_21_hmw3 = r_grishkanich_21_hmw3()

