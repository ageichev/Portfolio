import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

VGSALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
VGSALES_FILE = 'vgsales.csv'

year = 1994 + hash('a_tirel') % 23
year

def get_data():
    games_raw = pd.read_csv(VGSALES)
    games_year = games_raw.loc[games_raw['Year'] == year]
    games_data = games_year.to_csv(index=False)

    with open(VGSALES_FILE, 'w') as f:
        f.write(games_data)


## Самая продаваемая игра в году

def top_global_sales():
    games_data = pd.read_csv(VGSALES_FILE)
    top_game = games_data \
    .groupby('Name', as_index=False) \
    .agg({'Global_Sales': 'max'}) \
    .sort_values('Global_Sales', ascending=False) \
    .reset_index() \
    .Name[0]
    with open('top_game.txt', 'w') as f:
        f.write(top_game)


## Самая продаваемый жанр в Европе

def top_genre_Europe():
    games_data = pd.read_csv(VGSALES_FILE)
    top_genre = games_data \
    .groupby('Genre', as_index=False) \
    .agg({'EU_Sales': 'sum'}) \
    .sort_values('EU_Sales', ascending=False) \
    .reset_index() \
    .Genre[0]
    with open('top_genre.txt', 'w') as f:
        f.write(top_genre)
        
        
## Самая популярная платформа в играх тиражом +1 млн (Северная Америка) 

def top_platform_NA():
    games_data = pd.read_csv(VGSALES_FILE)
    top_platform = games_data.loc[games_data['NA_Sales'] >= 1] \
    .groupby('Platform', as_index=False) \
    .agg({'Name': 'nunique'}) \
    .sort_values('Name', ascending=False) \
    .reset_index() \
    .Platform[0]
    with open('top_platform.txt', 'w') as f:
        f.write(top_platform)
        
        
## Самая высокий средний показатель продаж в Японии (по издателю)

def top_publisher():
    games_data = pd.read_csv(VGSALES_FILE)
    top_publisher = games_data \
    .groupby('Publisher', as_index=False) \
    .agg({'JP_Sales': 'mean'}) \
    .sort_values('JP_Sales', ascending=False) \
    .reset_index() \
    .Publisher[0]
    with open('top_publisher.txt', 'w') as f:
        f.write(top_publisher)

        
## Кол-во более успешных игр в Европе против Японии

def EU_vs_JP():
    games_data = pd.read_csv(VGSALES_FILE)
    EU_vs_JP = str(games_data.loc[games_data['EU_Sales'] > games_data['JP_Sales']].Name.nunique())
    with open('EU_vs_JP.txt', 'w') as f:
        f.write(EU_vs_JP)


def print_data(ds):
    with open('top_game.txt', 'r') as f:
        all_data_top_game = f.read()
    with open('top_genre.txt', 'r') as f:
        all_data_top_genre = f.read()
    with open('top_platform.txt', 'r') as f:
        all_data_top_platform = f.read() 
    with open('top_publisher.txt', 'r') as f:
        all_data_top_publisher = f.read()
    with open('EU_vs_JP.txt', 'r') as f:
        all_data_EU_vs_JP = f.read()
    date = ds

    print(f'Bestseller in {year} for date {date}')
    print(all_data_top_game)

    print(f'Top genre in Europe in {year} for date {date}')
    print(all_data_top_genre)
    
    print(f'Top platform for $1+ million games in {year} for date {date}')
    print(all_data_top_platform)
    
    print(f'Top publisher by average sales in Japan in {year} for date {date}')
    print(all_data_top_publisher)
    
    print(f'Sum of games where Europe sales greater then Japan sales in {year} for date {date}')
    print(all_data_EU_vs_JP)


default_args = {
    'owner': 'a.tirel',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 7, 4),
}
schedule_interval = '15 17 * * *'

dag = DAG('a_tirel_lesson_03', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_global_sales',
                    python_callable=top_global_sales,
                    dag=dag)

t3 = PythonOperator(task_id='top_genre_Europe',
                    python_callable=top_genre_Europe,
                    dag=dag)

t4 = PythonOperator(task_id='top_platform_NA',
                    python_callable=top_platform_NA,
                    dag=dag)

t5 = PythonOperator(task_id='top_publisher',
                    python_callable=top_publisher,
                    dag=dag)

t6 = PythonOperator(task_id='EU_vs_JP',
                    python_callable=EU_vs_JP,
                    dag=dag)

t7 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7