import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

YEAR = 1994 + hash(f'ar-medvedev') % 23

default_args = {
    'owner': 'ar.medvedev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 27),
}

@dag(default_args=default_args, schedule_interval = '20 15 * * *', catchup=False)
def ar_medvedev_lesson3():
    
    @task()
    def get_data():
        sales_df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        sales_per_year_df = sales_df.query('Year == ' + str(YEAR))
        return sales_per_year_df
    
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    
    @task()
    def get_best_selling(sales_per_year_df):
        best_selling = sales_per_year_df[['Name','Global_Sales']].sort_values('Global_Sales', ascending=False).head(1)
        return best_selling
    
    
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    
    @task()
    def get_top_genre_eu(sales_per_year_df):
        top_genre_eu = sales_per_year_df[['Genre','EU_Sales']] \
                .groupby('Genre') \
                .agg({'EU_Sales':'sum'}) \
                .sort_values('EU_Sales',ascending=False)

        top_genre_eu = top_genre_eu[top_genre_eu.EU_Sales == top_genre_eu.EU_Sales.max()]
        return top_genre_eu
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    
    @task()
    def get_million_sales_na(sales_per_year_df):
        million_sales_na = sales_per_year_df[sales_per_year_df.NA_Sales > 1][['Platform','NA_Sales']] \
                .groupby('Platform') \
                .agg({'NA_Sales':'count'}) \
                .sort_values('NA_Sales',ascending=False)
        
        million_sales_na = million_sales_na[million_sales_na.NA_Sales == million_sales_na.NA_Sales.max()]
        return million_sales_na
        
        
    
    # У какого издателя самые высокие средние продажи в Японии?
    # Перечислить все, если их несколько
    
    @task()
    def get_top_avg_sales_jp(sales_per_year_df):
        top_avg_sales_jp = sales_per_year_df[['Publisher','JP_Sales']] \
                .groupby('Publisher') \
                .agg({'JP_Sales':'mean'}) \
                .sort_values('JP_Sales',ascending=False)
        
        top_avg_sales_jp = top_avg_sales_jp[top_avg_sales_jp.JP_Sales == top_avg_sales_jp.JP_Sales.max()]
        return top_avg_sales_jp

    # Сколько игр продались лучше в Европе, чем в Японии?
    
    @task()
    def get_eu_more_than_jp(sales_per_year_df):
        eu_more_than_jp = sales_per_year_df[['EU_Sales','JP_Sales']] \
                                [sales_per_year_df.EU_Sales > sales_per_year_df.JP_Sales].shape[0]
    
        return eu_more_than_jp

    

    @task()
    def print_data(best_selling, top_genre_eu, million_sales_na, top_avg_sales_jp, eu_more_than_jp):
        
        context = get_current_context()
        date = context['ds']

        print(f'Current date: {date}')
        
        print(f'Game with most sales (global) in {YEAR}')
        print(best_selling)

        print(f'Most sold genre in EU in {YEAR}')
        print(top_genre_eu)

        print(f'Platform with most games that have sold over a million copies in NA in {YEAR}')
        print(million_sales_na)
        
        print(f'Publisher with highest average sales in JP in {YEAR}')
        print(top_avg_sales_jp)
        
        print(f'The number of games that sold better in Europe than in Japan in {YEAR}')
        print(eu_more_than_jp)


    
    sales_per_year_df = get_data()

    best_selling = get_best_selling(sales_per_year_df)
    top_genre_eu = get_top_genre_eu(sales_per_year_df)
    million_sales_na = get_million_sales_na(sales_per_year_df)
    top_avg_sales_jp = get_top_avg_sales_jp(sales_per_year_df)
    eu_more_than_jp = get_eu_more_than_jp(sales_per_year_df)

    print_data(best_selling, top_genre_eu, million_sales_na, top_avg_sales_jp, eu_more_than_jp)

    
    
ar_medvedev_lesson3 = ar_medvedev_lesson3()
