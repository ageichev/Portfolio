import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 's-chigin'
FILE_NAME = 'our_data.csv'
year = 1994 + hash(f'{login}') % 23

def get_data():
    df = pd.read_csv(path) \
        .query(f'Year == {year}') \
        .reset_index(drop = True)
    
    with open(FILE_NAME, 'w') as f:
        f.write(df.to_csv(index = False))


def best_selling_game():
    df = pd.read_csv(FILE_NAME)
    best_sell_game = df \
                        .groupby('Name', as_index = False) \
                        .agg(sales_sum = ('Global_Sales', 'sum')) \
                        .sort_values(by = 'sales_sum', ascending = False) \
                        .reset_index(drop = True)['Name'][0]
    with open('task_1.txt', 'w') as f:
        f.write(best_sell_game)


def best_selling_genre_in_EU():
    df = pd.read_csv(FILE_NAME)
    best_sell_genre_eu = df \
                            .groupby('Genre', as_index = False) \
                            .agg(eu_sales_sum = ('EU_Sales', 'sum')) \
                            .sort_values(by = 'eu_sales_sum', ascending = False) \
                            .reset_index(drop = True)['Genre'][0]
    with open('task_2.txt', 'w') as f:
        f.write(best_sell_genre_eu)


def best_platform_in_NA():
    df = pd.read_csv(FILE_NAME)
    best_patform_na = df \
                            .query("NA_Sales > 1") \
                            .groupby('Platform', as_index = False) \
                            .agg(cnt_games = ('Name', 'count')) \
                            .sort_values(by = 'cnt_games', ascending = False) \
                            .reset_index(drop = True)['Platform'][0]
    with open('task_3.txt', 'w') as f:
            f.write(best_patform_na)


def best_publisher_in_JP():
    df = pd.read_csv(FILE_NAME)
    best_publisher_jp = df \
                            .groupby('Publisher', as_index = False) \
                            .agg(sales_mean = ('JP_Sales', 'mean')) \
                            .sort_values(by = 'sales_mean', ascending = False) \
                            .reset_index(drop = True)['Publisher'][0]
    with open('task_4.txt', 'w') as f:
            f.write(best_publisher_jp)

def n_games():
    df = pd.read_csv(FILE_NAME)
    n = df \
            .groupby('Name', as_index = False) \
            .agg(jp_sales = ('JP_Sales', 'sum'), eu_sales = ('EU_Sales', 'sum')) \
            .query('eu_sales > jp_sales') \
            .shape[0]
    with open('task_5.txt', 'w') as f:
            f.write(str(n))

def print_results(ds):
    with open('task_1.txt', 'r') as f:
        task_1_data = f.read()
        
    with open('task_2.txt', 'r') as f:
        task_2_data = f.read()
        
    with open('task_3.txt', 'r') as f:
        task_3_data = f.read()
                
    with open('task_4.txt', 'r') as f:
        task_4_data = f.read()
                
    with open('task_5.txt', 'r') as f:
        task_5_data = f.read()
                
    date = ds
                
    print(f'All task has been run on {date}')
    print(f'Results for year = {year}')
    print(f"1. Best selling game: {task_1_data}")
    print(f"2. Best selling genre in EU: {task_2_data}")
    print(f"3. Best selling platform in NA (games with sales > 1 mln): {task_3_data}")
    print(f"4. Best publisher in Japan: {task_4_data}")
    print(f"5. Number of games, which were sold better in EU than in JP: {task_5_data}")
    pass


default_args = {
    'owner': 's-chigin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 1),
}
schedule_interval = '30 16 * * *'

dag = DAG('s-chigin_lesson_3', default_args=default_args, schedule_interval=schedule_interval)

t0 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t1 = PythonOperator(task_id='task_1',
                    python_callable=best_selling_game,
                    dag=dag)

t2 = PythonOperator(task_id='task_2',
                    python_callable=best_selling_genre_in_EU,
                    dag=dag)

t3 = PythonOperator(task_id='task_3',
                    python_callable=best_platform_in_NA,
                    dag=dag)

t4 = PythonOperator(task_id='task_4',
                    python_callable=best_publisher_in_JP,
                    dag=dag)

t5 = PythonOperator(task_id='task_5',
                    python_callable=n_games,
                    dag=dag)
                

t6 = PythonOperator(task_id='print_data',
                    python_callable=print_results,
                    dag=dag)

t0 >> [t1, t2, t3, t4, t5] >> t6