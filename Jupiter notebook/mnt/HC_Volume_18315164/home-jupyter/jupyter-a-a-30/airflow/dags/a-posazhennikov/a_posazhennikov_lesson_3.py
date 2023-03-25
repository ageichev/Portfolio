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
    'owner': 'a_posazhennikov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 9)    
}


# Зададим функцию dag
@dag(default_args=default_args, catchup=False,schedule_interval = '* 15 * * *' )
def a_pos_games_research():
    
    # Путь к файлу
    path_to_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
# Определим год за который смотреть данные 

    login = 'a-posazhennikov'
    year = 1994 + hash(f'{login}') % 23


    
    @task(retries=4)    
# функция читает csv собирает датафрейм и фильтрует по нужному году

    def a_pos_get_data(path_to_file, year):
        
        df = pd.read_csv(path_to_file)              
        games = df.loc[df.Year == float(year)]
        return games

    @task(retries=4)
# 1 функция определяет какая игра была самой продаваемой в этом году во всем мире

    def get_best_sales(games):   
        best_sales = games.Name[games.Global_Sales == games.Global_Sales.max()]
        return best_sales.to_string(index = False)
        
    @task(retries=4)
# 2 функция определяет игры какого жанра были самыми продаваемыми в Европе
    def get_eu_best_genre(games):
        
        best_genre = games.groupby('Genre', as_index = False)\
            .agg({'EU_Sales':'sum'})\
            .sort_values('EU_Sales', ascending = False).reset_index()
        eu_best_genre = best_genre.Genre[best_genre.EU_Sales == best_genre.EU_Sales.max()]
        return eu_best_genre.to_string(index = False).replace('\n',' ')

    @task(retries=4)
# 3 функция определяет на какой платформе было больше всего игр,
# которые продались более чем миллионным тиражом в Северной Америке   
    def get_na_best_platform(games):
        
        na_platinum = games.loc[games.NA_Sales > 1]\
            .groupby('Platform', as_index = False)\
            .agg({'Name':'count'})           
        na_best_platform = na_platinum.Platform[na_platinum.Name == na_platinum.Name.max()]
        return na_best_platform.to_string(index = False).replace('\n',' ')
    
    @task(retries=4)
# 4 функция определяет у какого издателя самые высокие средние продажи в Японии
    def get_hiest_mean_jap_publisher(games):
        jap_mean_publisher = games.groupby('Publisher', as_index = False)\
            .agg({'JP_Sales':'mean'})
        hiest_mean_jap_publisher = jap_mean_publisher.Publisher[jap_mean_publisher.JP_Sales == jap_mean_publisher.JP_Sales.max()]
        return hiest_mean_jap_publisher.to_string(index = False).replace('\n',' ')
     
    @task(retries=4)
# Функция 5 определяет сколько игр продались лучше в Европе, чем в Японии    
    def get_eu_better_jp(games):
        eu_better_jp = games.loc[games.EU_Sales > games.JP_Sales].shape[0]
        return eu_better_jp
    
    @task(retries=4)
# Функция выводит результаты предыдущих тасков

    def a_pos_print_data(year,
                         best_sales,
                         eu_best_genre,
                         na_best_platform,
                         hiest_mean_jap_publisher,
                         eu_better_jp                         
                        ):

        context = get_current_context()
        date = context['ds']
        
        print(f'Выполнение на дату {date}')
        print(f'Проанализируем данные за {year} год')
        print(f'Cамой продаваемой в этом году во всем мире была игра {best_sales}')
        print(f'В Европе в этом году самыми продаваемыми были игры жанра {eu_best_genre}')  
        print(f'В Северной Америке больше всего игр из преодолевшичх планку в 1 млн копий были выпущены для платформы {na_best_platform}')
        print(f'Для {eu_better_jp} игр продажи в Европе превысили продажи в Японии')     

# зададим последовательность выполнения Дага
              
    games = a_pos_get_data(path_to_file, year)
              
    best_sales = get_best_sales(games)
    eu_best_genre = get_eu_best_genre(games)
    na_best_platform = get_na_best_platform(games)
    hiest_mean_jap_publisher = get_hiest_mean_jap_publisher(games)
    eu_better_jp = get_eu_better_jp(games)     

    a_pos_print_data(year,
                         best_sales,
                         eu_best_genre,
                         na_best_platform,
                         hiest_mean_jap_publisher,
                         eu_better_jp                         
                        )

a_pos_games_research = a_pos_games_research()