import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from io import StringIO
from urllib.parse import urlencode
from airflow import DAG
from airflow.operators.python import PythonOperator


year = 1994 + hash('a-markovicheva-26') % 23
SALES_YEAR = 'sales_for_year_selected.csv'


def get_data_for_year_selected():
    """
    Сначала определим год, за какой будем смотреть данные.
    """
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?' 
    public_key = 'https://disk.yandex.ru/d/DqsoMAMSlyd2hQ' 
    final_url = base_url + urlencode(dict(public_key=public_key))
    df = pd.read_csv(requests.get(final_url).json()['href'])
        
    df = df.dropna(subset=['Year'])
    df_my_year = df[df.Year == year]
    #df_my_year.Year = df_my_year.Year.map(int)
    #df_my_year.reset_index(drop=True, inplace=True)
    df_my_year = df_my_year.to_csv(index=False)
    
    with open(SALES_YEAR, 'w') as f:
        f.write(df_my_year)



def top_sold_game_global():
    """
    Какая игра была самой продаваемой в этом году во всем мире?
    """
    df_my_year = pd.read_csv(SALES_YEAR)
    top_sold_game_global = df_my_year.sort_values('Global_Sales', ascending=False).head(1)
    top_sold_game_global = top_sold_game_global[['Name', 'Global_Sales']]
    
    with open('top_sold_game_global.csv', 'w') as f:
        f.write(top_sold_game_global.to_csv(index=False, header=False))



def max_EU_Sales():
    """
    Игры какого жанра были самыми продаваемыми в Европе?
    """
    df_my_year = pd.read_csv(SALES_YEAR)
    max_EU_sales = df_my_year.groupby('Genre', as_index=False)         .agg({'EU_Sales': 'sum'})         .sort_values('EU_Sales', ascending=False).EU_Sales.iloc[0]
    df_EU_Sales = df_my_year.groupby('Genre', as_index=False)         .agg({'EU_Sales': 'sum'})         .sort_values('EU_Sales', ascending=False)
    df_max_EU_Sales = df_EU_Sales[df_EU_Sales.EU_Sales == max_EU_sales]
    
    with open('max_EU_Sales.csv', 'w') as f:
        f.write(df_max_EU_Sales.to_csv(index=False, header=False))


def max_platf_NA():
    """
    На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    """
    df_my_year = pd.read_csv(SALES_YEAR)
    df_platf_NA = df_my_year[df_my_year.NA_Sales > 1]         .groupby('Platform', as_index=False)         .agg({'Name': 'count'})         .sort_values('Name', ascending=False)         .rename(columns={'Name': 'Number_of_Games'})
    max_count = df_my_year[df_my_year.NA_Sales > 1]         .groupby('Platform', as_index=False)         .agg({'Name': 'count'})         .sort_values('Name', ascending=False)         .rename(columns={'Name': 'Number_of_Games'}).Number_of_Games.iloc[0]
    df_max_platf_NA = df_platf_NA[df_platf_NA.Number_of_Games == max_count]
    
    with open('max_platf_NA.csv', 'w') as f:
        f.write(df_max_platf_NA.to_csv(index=False, header=False))



def max_avg_sales_JP():
    """
    У какого издателя самые высокие средние продажи в Японии?
    """
    df_my_year = pd.read_csv(SALES_YEAR)
    df_avg_sales_JP = df_my_year.groupby('Publisher', as_index=False)         .agg({'JP_Sales': 'mean'})         .sort_values('JP_Sales', ascending=False)         .rename(columns={'JP_Sales': 'JP_Avg_Sales'})
    max_sales = df_my_year.groupby('Publisher', as_index=False)         .agg({'JP_Sales': 'mean'})         .sort_values('JP_Sales', ascending=False)         .rename(columns={'JP_Sales': 'JP_Avg_Sales'}).JP_Avg_Sales.iloc[0]
    df_max_avg_sales_JP = df_avg_sales_JP[df_avg_sales_JP.JP_Avg_Sales == max_sales]
    
    with open('max_avg_sales_JP.csv', 'w') as f:
        f.write(df_max_avg_sales_JP.to_csv(index=False, header=False))


def better_EU_than_JP():
    """
    Сколько игр продались лучше в Европе, чем в Японии?
    """
    df_my_year = pd.read_csv(SALES_YEAR)
    EU_better = df_my_year[df_my_year.JP_Sales < df_my_year.EU_Sales].shape[0]
    df_better_EU_than_JP = pd.DataFrame(data={'better_in_EU_than_JP': EU_better}, index=[0])
    
    with open('better_EU_than_JP.csv', 'w') as f:
        f.write(df_better_EU_than_JP.to_csv(index=False, header=False))


def print_data(ds):
    """
    финальный таск который собирает все ответы
    """
    with open('top_sold_game_global.csv', 'r') as f:
        top_sold_game_global = f.read()
    
    with open('max_EU_Sales.csv', 'r') as f:
        max_EU_Sales = f.read()
        
    with open('max_platf_NA.csv', 'r') as f:
        max_platf_NA = f.read()
        
    with open('max_avg_sales_JP.csv', 'r') as f:
        max_avg_sales_JP = f.read()
        
    with open('better_EU_than_JP.csv', 'r') as f:
        better_EU_than_JP = f.read()
    
    date = ds
    
    print(f'Launched on {date}')
    print(f'The best-selling game globally for year {year} (with the number of copies sold globally in millions):')
    print(top_sold_game_global)
    
    print(f'The best-selling genre(s) in Europe for year {year} (with the number of copies sold in Europe in millions):')
    print(max_EU_Sales)
    
    print(f'The top platform(s) with more than 1 mln copies sold in North America for year {year} (with the number of games sold):')
    print(max_platf_NA)
    
    print(f'The publisher(s) with highest avg sales in Japan for year {year} (with the average number of games sold in Japan in millions):')
    print(max_avg_sales_JP)
    
    print(f'The number of games more popular in Europe than in Japan for year {year}:')
    print(better_EU_than_JP)



default_args = {
    'owner': 'a.markovicheva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 20)
}

schedule_interval = '0 10 * * *'


dag = DAG('anna_mar_26_lesson_3', default_args=default_args, schedule_interval=schedule_interval)



t1 = PythonOperator(task_id='get_data_for_year_selected',
                    python_callable=get_data_for_year_selected,
                    dag=dag)

t2 = PythonOperator(task_id='top_sold_game_global',
                    python_callable=top_sold_game_global,
                    dag=dag)

t3 = PythonOperator(task_id='max_EU_Sales',
                        python_callable=max_EU_Sales,
                        dag=dag)

t4 = PythonOperator(task_id='max_platf_NA',
                        python_callable=max_platf_NA,
                        dag=dag)

t5 = PythonOperator(task_id='max_avg_sales_JP',
                    python_callable=max_avg_sales_JP,
                    dag=dag)

t6 = PythonOperator(task_id='better_EU_than_JP',
                    python_callable=better_EU_than_JP,
                    dag=dag)

t7 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)



t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7

