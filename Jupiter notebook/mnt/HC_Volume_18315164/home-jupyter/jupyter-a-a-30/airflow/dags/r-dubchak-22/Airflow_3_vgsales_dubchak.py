import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'         
DF_GLOBAL ='df_global.csv'
login = 'r-dubchak-22'
year = 1994 + hash(f'{login}') % 23

def get_data_dubchak():
    df = pd.read_csv(PATH)                            
    df_glob = df.to_csv(index=False)  
    
    with open (DF_GLOBAL,'w',encoding='utf-8') as f:  
        f.write(df_glob)
        
def get_top_global_s_game_dubchak():
    df_read = pd.read_csv(DF_GLOBAL)
    df_year = df_read.query("Year==@year")
    global_s_game = df_year.groupby('Name',as_index=False)['Global_Sales'].sum()
    top_global_s_game = global_s_game[global_s_game.Global_Sales==global_s_game.Global_Sales.max()]['Name']
    with open ('top_global_s_game.csv', 'w') as f:
        f.write(top_global_s_game.to_csv(index=False, header=False))

def get_top_genre_EU_dubchak():
    df_read = pd.read_csv(DF_GLOBAL)
    df_year = df_read.query("Year==@year")
    eu_s_genre = df_year.groupby('Genre',as_index=False)['EU_Sales'].sum()
    top_genre_EU = eu_s_genre[eu_s_genre.EU_Sales==eu_s_genre.EU_Sales.max()]['Genre']
    with open ('top_genre_EU.csv', 'w') as f:
        f.write(top_genre_EU.to_csv(index=False, header=False))
        
def get_top_platform_NA_dubchak():
    df_read = pd.read_csv(DF_GLOBAL)
    df_year = df_read.query("Year==@year")
    game_Platform_NA_best =df_year.query("NA_Sales>1").groupby('Platform',as_index=False)['Name'].count()
    top_platform_NA = game_Platform_NA_best[game_Platform_NA_best.Name==game_Platform_NA_best.Name.max()]['Platform']
    with open ('top_platform_NA.csv', 'w') as f:
        f.write(top_platform_NA.to_csv(index=False, header=False))
        
def get_top_publisher_JP_dubchak():
    df_read = pd.read_csv(DF_GLOBAL)
    df_year = df_read.query("Year==@year")
    publisher_mean_jp = df_year.groupby('Publisher',as_index=False)['JP_Sales'].mean()
    top_publisher_JP = publisher_mean_jp[publisher_mean_jp.JP_Sales==publisher_mean_jp.JP_Sales.max()]['Publisher']
    with open ('top_publisher_JP.csv', 'w') as f:
        f.write(top_publisher_JP.to_csv(index=False, header=False))
        
def get_best_game_EU_JP_dubchak():
    df_read = pd.read_csv(DF_GLOBAL)
    df_year = df_read.query("Year==@year")
    game_salse_EU_JP = df_year.groupby('Name',as_index=False)[['EU_Sales','JP_Sales']].sum()
    best_game_EU_JP = (game_salse_EU_JP.EU_Sales>game_salse_EU_JP.JP_Sales).sum()
    data = {'count':[best_game_EU_JP]}
    best_game_EU_JP = pd.DataFrame(data)
    with open ('best_game_EU_JP.csv', 'w') as f:
        f.write(best_game_EU_JP.to_csv(index=False, header=False)) 
    
def output_info_dubchak():
    with open('top_global_s_game.csv', 'r') as f:
        data_top_game_global = f.read()
    with open('top_genre_EU.csv', 'r') as f:
        data_top_genre_EU = f.read()
    with open('top_platform_NA.csv', 'r') as f:
        data_top_platform_NA = f.read()   
    with open('top_publisher_JP.csv', 'r') as f:
        data_top_publisher_JP = f.read()  
    with open('best_game_EU_JP.csv', 'r') as f:
        data_best_game_EU_JP = f.read()        
    date = year

    print(f'Top game by Global Sales in {date}')
    print(data_top_game_global)

    print(f'Top Genre by EU Sales in {date}')
    print(data_top_genre_EU)
    
    print(f'Top Platform by NA Sales in {date}')
    print(data_top_platform_NA) 
    
    print(f'Top Publisher by JP Sales in {date}')
    print(data_top_publisher_JP)   
    
    print(f'Count EU Sales then >  JP Sales in {date}')
    print(data_best_game_EU_JP)   
    
default_args = {
    'owner': login,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 27),
}
schedule_interval = '0 14 * * *'

dag = DAG('Airflow_3_vgsales_dubchak', default_args=default_args, schedule_interval=schedule_interval)

t1_get_data = PythonOperator(task_id='get_data_dubchak',
                    python_callable=get_data_dubchak,
                    dag=dag)

t2_get_top_global_s_game = PythonOperator(task_id='get_top_global_s_game_dubchak',
                    python_callable=get_top_global_s_game_dubchak,
                    dag=dag)

t2_get_top_genre_EU = PythonOperator(task_id='get_top_genre_EU_dubchak',
                        python_callable=get_top_genre_EU_dubchak,
                        dag=dag)

t2_get_top_platform_NA = PythonOperator(task_id='get_top_platform_NA_dubchak',
                        python_callable=get_top_platform_NA_dubchak,
                        dag=dag)

t2_get_top_publisher_JP = PythonOperator(task_id='get_top_publisher_JP_dubchak',
                        python_callable=get_top_publisher_JP_dubchak,
                        dag=dag)

t2_get_best_game_EU_JP = PythonOperator(task_id='get_best_game_EU_JP_dubchak',
                        python_callable=get_best_game_EU_JP_dubchak,
                        dag=dag)

t3_output_info_d = PythonOperator(task_id='output_info_dubchak',
                    python_callable=output_info_dubchak,
                    dag=dag)

t1_get_data >> [t2_get_top_global_s_game,t2_get_top_genre_EU,
               t2_get_top_platform_NA,t2_get_top_publisher_JP,
               t2_get_best_game_EU_JP]>>t3_output_info_d