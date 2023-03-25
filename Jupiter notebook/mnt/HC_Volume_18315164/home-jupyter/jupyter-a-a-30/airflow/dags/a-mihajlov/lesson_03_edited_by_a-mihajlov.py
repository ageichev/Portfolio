import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

video_games = 'vgsales.csv'
video_games_FILE = 'vgsales.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    vg = pd.read_csv(video_games)
    vg_data = vg.to_csv(index=False)

    with open(video_games_FILE, 'w') as f:
        f.write(vg_data)


def get_top_game():
    top_game = pd.read_csv(video_games_FILE)
    top_game = top_game \
        .query('Year == 1994.0') \
        .groupby('Name', as_index=False) \
        .agg({'Global_Sales' : 'sum'}) \
        .sort_values('Global_Sales', ascending=False) \
        .head(1)
    with open('top_game.csv', 'w') as f:
        f.write(top_game.to_csv(index=False, header=False))

        
def get_top_EU_genres():
    top_EU_genres = pd.read_csv(video_games_FILE)
    top_EU_genres = top_EU_genres \
        .query('Year == 1994.0') \
        .groupby('Genre', as_index=False) \
        .agg({'EU_Sales' : 'sum'}) \
        .sort_values('EU_Sales', ascending=False) \
        .head(3)
    with open('top_EU_genres.csv', 'w') as f:
        f.write(top_EU_genres.to_csv(index=False, header=False))

        
def get_platform_w_mil_NA_sales():
    platform_w_mil_NA_sales = pd.read_csv(video_games_FILE)
    platform_w_mil_NA_sales = platform_w_mil_NA_sales \
        .query('Year == 1994.0 & NA_Sales > 1') \
        .groupby('Platform', as_index=False) \
        .agg({'Name' : 'count'}) \
        .sort_values('Name', ascending=False)
    with open('platform_w_mil_NA_sales.csv', 'w') as f:
        f.write(platform_w_mil_NA_sales.to_csv(index=False, header=False))

def get_publ_w_JP_avg_sales():
    publ_w_JP_avg_sales = pd.read_csv(video_games_FILE)
    publ_w_JP_avg_sales = publ_w_JP_avg_sales \
        .query('Year == 1994.0') \
        .groupby('Publisher', as_index=False) \
        .agg({'JP_Sales' : 'mean'}) \
        .sort_values('JP_Sales', ascending=False) \
        .head(2)
    with open('publ_w_JP_avg_sales.csv', 'w') as f:
        f.write(publ_w_JP_avg_sales.to_csv(index=False, header=False))
        
def get_top_sales_in_EU_vs_JP():
    top_sales_in_EU_vs_JP = pd.read_csv(video_games_FILE)
    top_sales_in_EU_vs_JP = top_sales_in_EU_vs_JP \
        .query('Year == 1994.0 & EU_Sales > JP_Sales') \
        .groupby('Year') \
        .agg({'Name' : 'count'})
    with open('top_sales_in_EU_vs_JP.csv', 'w') as f:
        f.write(top_sales_in_EU_vs_JP.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_game.csv', 'r') as f:
        top_game_data = f.read()
    with open('top_EU_genres.csv', 'r') as f:
        top_EU_genres_data = f.read()
    with open('platform_w_mil_NA_sales.csv', 'r') as f:
        platform_w_mil_NA_sales_data = f.read()
    with open('publ_w_JP_avg_sales.csv', 'r') as f:
        publ_w_JP_avg_sales_data = f.read()
    with open('top_sales_in_EU_vs_JP.csv', 'r') as f:
        top_sales_in_EU_vs_JP_data = f.read()
    date = ds

    print(f'Top sold game in 1994')
    print(top_game_data)

    print(f'Top sold genres in EU in 1994')
    print(top_EU_genres_data)
    
    print(f'Platforms with games sold over 1 million copies in NA in 1994')
    print(platform_w_mil_NA_sales_data)
    
    print(f'Publishers with top average sales in Japan in 1994')
    print(publ_w_JP_avg_sales_data)
    
    print(f'Number of games sold better in EU in comparison with Japan in 1994')
    print(top_sales_in_EU_vs_JP_data)


default_args = {
    'owner': 'a-mihajlov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 27),
}
schedule_interval = '0 12 * * *'

dag_by_a_mihajlov = DAG('a.mihajlov_vg_gata', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_by_a_mihajlov)

t2 = PythonOperator(task_id='get_top_game',
                    python_callable=get_top_game,
                    dag=dag_by_a_mihajlov)

t3 = PythonOperator(task_id='get_top_EU_genres',
                        python_callable=get_top_EU_genres,
                        dag=dag_by_a_mihajlov)

t4 = PythonOperator(task_id='get_platform_w_mil_NA_sales',
                        python_callable=get_platform_w_mil_NA_sales,
                        dag=dag_by_a_mihajlov)

t5 = PythonOperator(task_id='get_publ_w_JP_avg_sales',
                        python_callable=get_publ_w_JP_avg_sales,
                        dag=dag_by_a_mihajlov)

t6 = PythonOperator(task_id='get_top_sales_in_EU_vs_JP',
                        python_callable=get_top_sales_in_EU_vs_JP,
                        dag=dag_by_a_mihajlov)

t7 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_by_a_mihajlov)

t1 >> [t2, t3, t4, t5, t6] >> t7
