import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

y = 1994 + hash("t-popkova") % 23


default_args = {
    'owner': 't-popkova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 28),
    'schedule_interval': '@daily'
}


schedule_interval = '0 20 * * *'


@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def tpopkova_lesson3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(vgsales).query('Year == @y')
        return df.to_csv(index=False)
    
    @task()
    def best_selling_game(df):
        df = pd.read_csv(StringIO(df))
        best_selling_game = df.query("Global_Sales == Global_Sales.max()").values[0][1]
        return best_selling_game
    
    @task
    def best_selling_genre(df):
        df = pd.read_csv(StringIO(df))
        best_selling_genre = df.groupby('Genre').agg({'EU_Sales':'sum'}).query('EU_Sales == EU_Sales.max()').index.to_list()
        return best_selling_genre
    
    @task
    def best_selling_platform(df):
        df = pd.read_csv(StringIO(df))
        best_selling_platform = df.query('NA_Sales > 1')\
                                    .groupby('Platform')\
                                    .agg({'Name':'count'})\
                                    .query('Name == Name.max()')\
                                    .index\
                                    .to_list()
        return best_selling_platform
    
    @task
    def best_selling_publisher(df):
        df = pd.read_csv(StringIO(df))
        best_selling_publisher = df.groupby('Publisher')\
                                    .agg({'JP_Sales':'mean'})\
                                    .query('JP_Sales == JP_Sales.max()')\
                                    .index\
                                    .to_list()
        return best_selling_publisher
    
    @task
    def comparison(df):
        df = pd.read_csv(StringIO(df))
        comparison = str(len(df[df.EU_Sales > df.JP_Sales]))
        return comparison
    
    @task()
    def print_data(best_selling_game, 
                   best_selling_genre, 
                   best_selling_platform, 
                   best_selling_publisher, 
                   comparison):
        
        context = get_current_context()
        date = context['ds']


        print(f'Самая продаваемая игра во всем мире в {y} — {best_selling_game}.')
        print(f'Самыми продаваевыми играми в Европе в {y} были игры жанра/ов {best_selling_genre}.')
        print(f'''Больше всего игр, продавшихся более чем миллионным тиражом в Северной Америке, в {y} было выпущено на платформе/ах — {best_selling_platform}.''')
        print(f'''Издатель/и с самыми высокими средними продажами в Японии в {y} — {best_selling_publisher}.''')
        print(f'''В {y} в Европе продались лучше, чем в Японии {comparison} игр.''')


    df = get_data()
    best_selling_game = best_selling_game(df)
    best_selling_genre = best_selling_genre(df)
    best_selling_platform = best_selling_platform(df)
    best_selling_publisher = best_selling_publisher(df)
    comparison = comparison(df)

    print_data(best_selling_game, 
                   best_selling_genre, 
                   best_selling_platform, 
                   best_selling_publisher, 
                   comparison)

tpopkova_lesson3 = tpopkova_lesson3()
