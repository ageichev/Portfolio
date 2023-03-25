import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
# import telegram

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.models import Variable

import requests
import json
from urllib.parse import urlencode

login = 's.rybalko_23'
desired_year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 's.rybalko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 24),
    'schedule_interval': '00 12 * * *'
} 


    

@dag(default_args=default_args, catchup=False)
def s_rybalko_lesson_3():
   
    @task()
    def get_data():
        
        vgsales = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')        
        vgsales_desired_year = vgsales[vgsales.Year == desired_year]        
        return vgsales_desired_year

     #Какая игра была самой продаваемой в этом году во всем мире?   
    @task()
    def best_selling_world(vgsales_desired_year):
        selling = vgsales_desired_year.groupby('Name', as_index=False).Global_Sales.sum()
        best_selling_world = vgsales_desired_year[vgsales_desired_year.Global_Sales == vgsales_desired_year.Global_Sales.max()]['Name'] \
        .values.tolist()
        
        return best_selling_world
    
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def top_genre_europe(vgsales_desired_year):
        genre_europe = vgsales_desired_year.groupby('Genre', as_index=False).EU_Sales.sum()
        top_genre_europe= genre_europe[genre_europe.EU_Sales == genre_europe.EU_Sales.max()]['Genre'].values.tolist()
        
        return top_genre_europe
    
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    
    @task()
    def top_games_NA(vgsales_desired_year):
        platform_NA = vgsales_desired_year[vgsales_desired_year.NA_Sales >1].groupby('Platform', as_index=False).NA_Sales.sum()
        top_platform_NA = platform_NA[platform_NA.NA_Sales == platform_NA.NA_Sales.max()]['Platform'].values.tolist()
        return top_platform_NA
    
    #У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def top_publisher_jp(vgsales_desired_year):
        publisher_jp = vgsales_desired_year.groupby('Publisher', as_index=False).JP_Sales.mean()
        top_publisher_jp = publisher_jp[publisher_jp.JP_Sales == publisher_jp.JP_Sales.max()]['Publisher'].values.tolist()
        return top_publisher_jp
    
    #Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def europe_better_jp(vgsales_desired_year):
        selling_eur_jp = vgsales_desired_year.groupby('Name', as_index=False).agg({'EU_Sales':'sum','JP_Sales':'sum'})
        europe_better_jp = selling_eur_jp[selling_eur_jp.EU_Sales > selling_eur_jp.JP_Sales].shape[0]
        return europe_better_jp
    
    
    #финальный таск писал в лог ответ на каждый вопрос
    @task()
    def print_data(best_selling_world,top_genre_europe,top_games_NA,top_publisher_jp,europe_better_jp):
        
        print(f'The best selling game in {desired_year} worldwide is {best_selling_world}')
        
        print(f'The best selling game genre in Europe in {desired_year} is {top_genre_europe}')
        
        print(f'The platform that had the most games that sold in North America in {desired_year} is  {top_games_NA}')
        
        print(f'The publisher with the highest average sales in Japan in {desired_year} is {top_publisher_jp}')
            
        print(f'Number of games sold better in Europe than in Japan in {desired_year} - {europe_better_jp}') 
        
    
    vgsales_desired_year = get_data()
    best_selling_world = best_selling_world(vgsales_desired_year)
    top_genre_europe = top_genre_europe(vgsales_desired_year)
    top_games_NA = top_games_NA(vgsales_desired_year)
    top_publisher_jp = top_publisher_jp(vgsales_desired_year)
    europe_better_jp = europe_better_jp(vgsales_desired_year)

    print_data(best_selling_world,top_genre_europe,top_games_NA,top_publisher_jp,europe_better_jp) 

s_rybalko_lesson_3 = s_rybalko_lesson_3()
