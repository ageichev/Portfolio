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
from airflow.models import Variable

year = 1994 + hash(f'p-grischenko-29') % 23

analytics_data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'


default_args = {
    'owner': 'p.grischenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 11),
    'schedule_interval': '0 12 * * *'
}


@dag(default_args=default_args, catchup=False)
def p_grischenko_lesson_3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(analytics_data)
        sales = df[df.Year == year]
        return sales

    @task()
    def best_selling_game(sales):
        ex1 = sales \
                .groupby('Name', as_index=False) \
                .agg({'Global_Sales' : 'sum'}) \
                .nlargest(1, 'Global_Sales', keep='all') #keep='all', если сумма продаж одинакова у первых мест топа 
        
        ex1_res = list(map(lambda x: x.Name, ex1.iloc)) #выводим список, если у топовых мест равная сумма продаж
        
        return ex1_res

    @task()
    def top_genre_eu(sales):
        ex2 = sales \
                .groupby('Genre', as_index=False) \
                .agg({'EU_Sales' : 'sum'}) \
                .nlargest(1, 'EU_Sales', keep='all')
        
        ex2_res = list(map(lambda x: x.Genre, ex2.iloc))
        
        return ex2_res

    @task()
    def top_NA_sales(sales):
        ex3 = sales \
                .query('NA_Sales > 1') \
                .groupby('Platform', as_index=False) \
                .agg({'NA_Sales' : 'sum'}) \
                .nlargest(1, 'NA_Sales', keep='all')
        
        ex3_res = list(map(lambda x: x.Platform, ex3.iloc))
        
        return ex3_res

    @task()
    def top_JP_publisher(sales):
        ex4 = sales \
                .groupby('Publisher', as_index=False) \
                .agg({'JP_Sales' : 'mean'}) \
                .nlargest(1, 'JP_Sales', keep='all')
        
        ex4_res = list(map(lambda x: x.Publisher, ex4.iloc))
        
        return ex4_res
    
    @task()
    def EU_JP_match(sales):
        ex5 = sales \
                .groupby('Name', as_index=False) \
                .agg({'EU_Sales' : 'sum', 'JP_Sales' : 'sum'})
        
        ex5_res = int(ex5.query('EU_Sales > JP_Sales').EU_Sales.count())
        
        return ex5_res

    @task()
    def print_data(best_selling_game, top_genre_eu, top_NA_sales, top_JP_publisher, EU_JP_match):

        context = get_current_context()
        date = context['ds']

        print(f'''Data from sales for {date} in {year}
                  Best selling game in th world: {best_selling_game}
                  Best selling game genre in Europe: {top_genre_eu}
                  Best platform in North America: {top_NA_sales}
                  Best publisher in Japan: {top_JP_publisher}
                  Sales comparison between Europe and Japan: {EU_JP_match}''')

    sales = get_data()
    
    best_selling_game = best_selling_game(sales)
    top_genre_eu = top_genre_eu(sales)
    top_NA_sales = top_NA_sales(sales)
    top_JP_publisher = top_JP_publisher(sales)
    EU_JP_match = EU_JP_match(sales)

    print_data(best_selling_game, top_genre_eu, top_NA_sales, top_JP_publisher, EU_JP_match)

p_grischenko_lesson_3 = p_grischenko_lesson_3()
