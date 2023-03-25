import pandas as pd
import numpy as np
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


default_args = {
    'owner': 'n-ljutetskij-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 20),
    'schedule_interval': '0 17 * * *'
}


@dag(default_args=default_args, catchup=False)
def ljutetskij_22_airflow_2():            
    # 0
    # Data load
    @task(retries=3)
    def get_data():
        path   = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'         
        df     = pd.read_csv(path)
        login  = 'n-ljutetskij-22'
        year   = 1994 + hash(f'{login}') % 23
        df_res = df.query('Year == @year')        
        return df_res    
    

    # 1 
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_top_sold(df):        
        return df.loc[df.Global_Sales.idxmax()].Name
    
    
    # 2 
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def eu_sales(df):        
        eu_sales   = df.groupby('Genre').agg(total_sales_eu=('EU_Sales', 'sum'))
        top_genres = ', '.join(eu_sales[eu_sales.total_sales_eu == eu_sales.total_sales_eu.max()].index)
        return top_genres
    
    
    # 3 
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task()
    def na_sales(df):
        na_sales = df.query('NA_Sales >= 1').groupby('Platform').agg(titles_total=('Name','count'))
        top_platforms = ', '.join(na_sales[na_sales.titles_total == na_sales.titles_total.max()].index)        
        return top_platforms
        
    
    # 4 
    # У какого издателя самые высокие средние продажи в Японии?
    # Перечислить все, если их несколько  
    @task()
    def jp_sales(df):
        jp_sales = df.groupby('Publisher').agg(mean_sales=('JP_Sales','mean'))
        top_mean_sales = ', '.join(jp_sales[jp_sales.mean_sales == jp_sales.mean_sales.max()].index)      
        return top_mean_sales
    
    
    # 5 
    # Сколько игр продались лучше в Европе, чем в Японии?   
    @task()
    def eu_vs_jp_sales(df):   
        return df[df.EU_Sales >= df.JP_Sales].Name.nunique()     
    

    @task()
    def print_data(top_sold, top_genres, top_platforms, top_mean_sales, eu_vs_jp, df):
        
        year = df.reset_index().loc[0, 'Year']
        
        print(f'1. Какая игра была самой продаваемой в этом году ({year}) во всем мире? - {top_sold}')
        print(f'2. Игры какого жанра были самыми продаваемыми в этом году ({year}) в Европе ? - {top_genres}')
        print(f'3. На какой платформе было больше всего игр в этом году ({year}), которые продались более чем миллионным тиражом в Северной Америке? - {top_platforms}')
        print(f'4. У какого издателя самые высокие средние продажи в этом году ({year}) в Японии? - {top_mean_sales}')
        print(f'5. Сколько игр продались в этом году ({year}) лучше в Европе, чем в Японии? - {eu_vs_jp}')                       
        

    data           = get_data()        
    top_sold       = get_top_sold(data)
    top_genres     = eu_sales(data)    
    top_platforms  = na_sales(data)
    top_mean_sales = jp_sales(data)
    eu_vs_jp       = eu_vs_jp_sales(data)
    
    print_data(top_sold, top_genres, top_platforms, top_mean_sales, eu_vs_jp, data)


ljutetskij_22_airflow_2 = ljutetskij_22_airflow_2()
