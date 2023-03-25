import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

login = 'k.volkov-23'
VGSALES = '/var/lib/airflow/airflow.git/dags/a.kosheleva-14/vgsales-1.csv'
YEAR = 2011 + hash(f'k.volkov-23') % 23

default_args = {
    'owner': 'k.volkov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 1),
    'schedule_interval': '0 11 * * *'
}


@dag(default_args=default_args, catchup=False)
def games_volkov_23():
    @task(retries=3, retry_delay=timedelta(10))
    def get_games_data():
        games_data = pd.read_csv(VGSALES).query("Year == @YEAR")
        return games_data
    

    @task()
    def get_best_selling_game(games_data):
        best_selling_game = games_data.sort_values('Global_Sales', ascending=False) \
                                        .iloc[0]['Name']
        return best_selling_game
    

    @task()
    def get_best_genre_EU(games_data):
        best_genre_EU = list(games_data \
                             .sort_values('EU_Sales', ascending=False) \
                             .head(5).Genre \
                             .unique())
        return best_genre_EU

    @task()
    def get_best_platform_NA(games_data):
        best_platform_NA = list(games_data.query('NA_Sales >= 1.0') \
                        .groupby(['Platform'], as_index=False) \
                        .agg({'Rank': 'count'}) \
                        .sort_values('Rank', ascending=False)\
                        .Platform.head(3))
        return best_platform_NA

    @task()
    def get_mean_platform_sales_JP(games_data):
        mean_platform_sales_JP = list(games_data.groupby(['Publisher'], as_index=False).agg({'JP_Sales': 'mean'}) \
                                    .sort_values('JP_Sales', ascending=False) \
                                    .Publisher.head(5))
        return mean_platform_sales_JP
       
    
    @task()
    def get_more_JPsales_than_EUsales(games_data):
        more_JPsales_than_EUsales = ((games_data['JP_Sales'] - games_data['EU_Sales'])>0).sum()
        
        return more_JPsales_than_EUsales
    

    @task()
    def print_data(best_selling_game,
                  best_genre_EU,
                  best_platform_NA,
                  mean_platform_sales_JP,
                  more_JPsales_than_EUsales):

        context = get_current_context()
        date = context['ds']

        print(f'Год {YEAR}:')
              
        print(f'Самая продаваемая игра в мире: {best_selling_game}')

        print(f"Самый продаваемый жанр игр в Европе: {', '.join(best_genre_EU)}")
        
        print(f"Игры, которые продались более чем миллионным тиражом в Северной Америке, на платформе/платформах {','.join(best_platform_NA)}")
              
        print(f"Самые высокие средние продажи издателя в Японии {', '.join(mean_platform_sales_JP)}")
              
        print(f'Количество игр, которые проданы в Европе лучше, чем в Японии: {more_JPsales_than_EUsales}')
    
    
    
    
    games_data = get_games_data()
    
    best_selling_game = get_best_selling_game(games_data)
    best_genre_EU = get_best_genre_EU(games_data)
    best_platform_NA = get_best_platform_NA(games_data)
    mean_platform_sales_JP = get_mean_platform_sales_JP(games_data)
    more_JPsales_than_EUsales = get_more_JPsales_than_EUsales(games_data)

    print_data(best_selling_game, best_genre_EU, best_platform_NA, mean_platform_sales_JP, more_JPsales_than_EUsales)

games_volkov_23 = games_volkov_23()