import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context



vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 's-androsova-23-lesson3',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 1),
    'schedule_interval': '@daily'
}
my_year = 1994 + hash('s-androsova-23') % 23

@dag(default_args=default_args, catchup=False)
def s_androsova_23_lesson_3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(vgsales).query('Year == @my_year')
        return df.to_csv(index=False)
    
    
# 1.Какая игра была самой продаваемой в этом году во всем мире?
    
    @task()
    def get_top_sales_game(df):
        game = df.groupby('Name', as_index=False).agg({'Global_Sales':'sum'}).sort_values('Global_Sales', ascending=False)
        game = game[game['Global_Sales']==game['Global_Sales'].max()]
        return top_sales_game


# 2. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько    

    @task()
    def get_top_sales_genre_in_Europe(df):
        genre = df.groupby('Genre', as_index=False).agg({'EU_Sales':'sum'}).sort_values('EU_Sales', ascending=False)
        genre = genre[genre['EU_Sales']==genre['EU_Sales'].max()]
        return top_sales_genre_in_Europe
    
    
# 3. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?  

    @task()
    def get_best_NA_platform(df):
        NA_best_platform = df.query('NA_Sales>1').groupby('Platform', as_index=False).agg({'Name':'count'})\
                            .sort_values('Name', ascending=False)\
                            .reset_index()\
                            .query('Name == Name.max()')['Platform'].values
        return best_NA_platform

# 4. У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    
    @task()
    def get_best_mean_in_Japan(df):
        best_in_Japan_1 = df.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'})\
                            .rename(columns = {'JP_Sales' : 'JP_Sales_mean'}).sort_values('JP_Sales', ascending=False)
        max_value = best_in_Japan_1['JP_Sales_mean'].max()
        best_mean_in_Japan = best_in_Japan_1.query("JP_Sales_mean == @max_value")
        return best_mean_in_Japan

# 5. Сколько игр продались лучше в Европе, чем в Японии?    

    @task()
    def get_EU_vs_JP(df):
        EU_vs_JP = df.groupby('Name', as_index=False).sum().reset_index()\
                        .query('EU_Sales>JP_Sales').Name.nunique()
        return EU_vs_JP
    
    @task()
    def print_data(top_sales_game, top_sales_genre_in_Europe, best_NA_platform, best_mean_in_Japan, EU_vs_JP):
        
        context = get_current_context()
        date = context['ds']
    

        print(f'Данные за период {date}:')
        print(f'Самая продаваемая игра во всем мире в {my_year} — {top_sales_game}')
        print(f'Самыми продаваевыми играми в Европе в {my_year} были игры жанра/ов {top_sales_genre_in_Europe}')
        print(f'Больше всего игр, продавшихся более чем миллионным тиражом в Северной Америке, в {my_year} было выпущено на платформе — {best_NA_platform}')
        print(f'Издатель/и с самыми высокими средними продажами в Японии в {my_year} — {best_mean_in_Japan}')
        print(f'В {my_year} в Европе продавались лучше, чем в Японии {EU_vs_JP} игр')


    df = get_data()
    top_sales_game = get_top_sales_game(df)
    top_sales_genre_in_Europe = get_top_sales_genre_in_Europe(df)
    best_NA_platform = get_best_NA_platform(df)
    best_mean_in_Japan = get_best_mean_in_Japan(df)
    EU_vs_JP = get_EU_vs_JP(df)

    print_data(top_sales_game, top_sales_genre_in_Europe, best_NA_platform, best_mean_in_Japan, EU_vs_JP)
    
s_androsova_23_lesson_3 = s_androsova_23_lesson_3() 