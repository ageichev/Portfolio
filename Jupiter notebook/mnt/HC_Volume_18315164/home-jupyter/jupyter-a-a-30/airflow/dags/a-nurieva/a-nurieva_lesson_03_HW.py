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


path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a-nurieva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 4),
}
schedule_interval = '0 13 * * *'

@dag(default_args=default_args, catchup=False)
def vgsales_nurieva_lection_3():
    
    @task()
    def get_data():
        vgsales = pd.read_csv(path)
        login = 'a-nurieva'
        tasks_year = 1994 + hash(f'{login}') % 23 # определение года для анализа
        vgsales_year = vgsales.query(f'Year=={tasks_year}') 
        return vgsales_year
    
    @task()
    def get_popular_game(vgsales_year): #самой продаваемая игра в этом году во всем мире
        popular_game = vgsales_year.groupby('Name', as_index=False)\
                                    .agg({'Global_Sales':'sum'})\
                                    .sort_values('Global_Sales', ascending=False).head(1)\
                                    .Name.values[0]
        return popular_game
    
    @task()
    def get_genre(vgsales_year): # самый продаваемый жанр игр в Европе
        genre_year = vgsales_year.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'})\
                                 .sort_values('EU_Sales', ascending=False)
        max_genre = genre_year['EU_Sales'].max()
        popular_genre = genre_year.query(f'EU_Sales == {max_genre}').Genre.values
        popular_genre_list = ', '.join(popular_genre)
        return popular_genre_list
          
    
    @task()
    def get_NA_platform(vgsales_year): #платформа игр в Сев. Америке с продаже более чем миллионным тиражом
        NA_platform = vgsales_year.groupby('Platform', as_index=False)\
                                  .agg({'NA_Sales':'sum'})\
                                  .query('NA_Sales>1')\
                                  .Platform.values
        NA_platform_list = ', '.join(NA_platform)
        return NA_platform_list
        
        
    @task()
    def get_JP_publisher(vgsales_year):  #издатель с самыми высокими средними продажами в Японии
        JP_publisher = vgsales_year.groupby('Publisher', as_index=False)\
                                   .agg({'JP_Sales':'mean'})
        max_JP_Sales = JP_publisher.JP_Sales.max()
        popular_JP_publisher = JP_publisher.query(f'JP_Sales=={max_JP_Sales}')\
                                           .Publisher.values
        JP_publisher_list = ', '.join(popular_JP_publisher)
        return JP_publisher_list
    
    @task()
    def get_EU_JP_games(vgsales_year):  #кол-во игр, проданных лучше в Европе, чем в Японии
        EU_JP_sales = vgsales_year.groupby('Name', as_index=False)\
                                  .agg({'EU_Sales':'sum','JP_Sales':'sum'})
        EU_JP_sales['Difference_EU_JP'] = EU_JP_sales.EU_Sales-EU_JP_sales.JP_Sales
        EU_JP_games = EU_JP_sales.query('Difference_EU_JP > 0').Name.count()
        return EU_JP_games

    @task()
    def get_print(popular_game, popular_genre_list, NA_platform_list, JP_publisher_list, EU_JP_games):
        tasks_year = 1994 + hash('a-nurieva') % 23       
        print(f'The most sold game in {tasks_year} year was {popular_game}.')
        print(f'Popular genre(s) in {tasks_year} year: {popular_genre_list}.')
        print(f'Platform(s) with most sold games in {tasks_year} year in North Amerika: {NA_platform_list}.')
        print(f'Publisher(s) with highst average of sold games in {tasks_year} year in Japan: {JP_publisher_list}.')
        print(f'Amount of games, which sales were higher in Europe than in Japan in {tasks_year} year, were {EU_JP_games}.')
            

    vgsales_year = get_data()
    popular_game = get_popular_game(vgsales_year)
    popular_genre_list = get_genre(vgsales_year)
    NA_platform_list = get_NA_platform(vgsales_year)
    JP_publisher_list = get_JP_publisher(vgsales_year)
    EU_JP_games = get_EU_JP_games(vgsales_year)

    get_print(popular_game, popular_genre_list, NA_platform_list, JP_publisher_list, EU_JP_games)

vgsales_nurieva_lection_3 = vgsales_nurieva_lection_3()