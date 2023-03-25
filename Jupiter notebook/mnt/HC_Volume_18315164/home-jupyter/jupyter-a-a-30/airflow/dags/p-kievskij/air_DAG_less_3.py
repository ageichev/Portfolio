import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

login = 'p-kievskij'
test_year = 1994 + hash(f'{login}') % 23

def get_data():
    df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
    df['Year'] = df['Year'].astype('Int64')
    df = df.query("Year == @test_year").reset_index().drop(columns='index')
    df.to_csv('year_vgsales.csv', index=False)

def top_sale_def():
    top_sale_df = pd.read_csv('year_vgsales.csv')
    g_top_sale_df = top_sale_df.groupby('Name', as_index=False).agg({"Global_Sales":"sum"})
    g_top_sale_df = g_top_sale_df.query("Global_Sales == @g_top_sale_df.Global_Sales.max()")
    with open('top_sale_df.csv', 'w') as f:
        f.write(g_top_sale_df.to_csv(index=False, header=True))
        
def top_genre_eu_def():
    eu_df = pd.read_csv('year_vgsales.csv')
    eu_df = eu_df.groupby("Genre", as_index=False).agg({'EU_Sales':'sum'})
    eu_df = eu_df.query("EU_Sales == @eu_df.EU_Sales.max()")
    with open('top_genre_eu.csv', 'w') as f:
        f.write(eu_df.to_csv(index=False, header=True))

def M_platform_def():
    na_df = pd.read_csv('year_vgsales.csv')
    na_df = na_df.query("NA_Sales > 1")
    na_df = na_df.groupby("Platform", as_index=False).agg({'Name':'count'}).rename(columns={'Name':'Games'})
    na_df = na_df.query("Games == @na_df.Games.max()")
    with open('1M_platform_NE.csv', 'w') as f:
        f.write(na_df.to_csv(index=False, header=True))        

def best_sales_JP_def():
    jp_df = pd.read_csv('year_vgsales.csv')
    jp_df = jp_df.groupby("Publisher", as_index=False).agg({'JP_Sales':'mean'})
    jp_df = jp_df.query("JP_Sales == @jp_df.JP_Sales.max()")
    with open('best_sales_JP.csv', 'w') as f:
        f.write(jp_df.to_csv(index=False, header=True))
        
def EU_JP_sale_def():
    EU_JP_sale = pd.read_csv('year_vgsales.csv')
    EU_JP_sale = str(EU_JP_sale.query("EU_Sales > JP_Sales").shape[0])
    with open('EU_JP_sale.txt', 'w') as f:
        f.write(EU_JP_sale)

def print_data():
    with open('top_sale_df.csv', 'r') as f:
        data_top_sale = f.read()
    with open('top_genre_eu.csv', 'r') as f:
        data_top_genre_eue = f.read()
    with open('1M_platform_NE.csv', 'r') as f:
        data_1M_platform_NE = f.read()
    with open('best_sales_JP.csv', 'r') as f:
        data_best_sales_JP = f.read()
    with open('EU_JP_sale.txt', 'r') as f:
        data_EU_JP_sale = f.read()

    print(f'Самая продоваемая в {test_year} году игра:')
    print(data_top_sale)

    print(f'Самые продаваемые в Европе жанры в {test_year} году:')
    print(data_top_genre_eue)

    print(f'Платформы с наибольшим количеством игр и миллионным тиражом Северной Америке в {test_year} году:')
    print(data_1M_platform_NE)
    
    print(f'Издатель с самыми высокими средними продажами в Японии в {test_year} году:')
    print(data_best_sales_JP)
    
    print(f'Игр которые продались в Европпе лучше чем в Японии в {test_year} году: {data_EU_JP_sale}')
    
default_args = {
    'owner': 'p-kievskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 16),
    'schedule_interval': '0 15 * * *'
}
dag = DAG('p-kievskij_less_3', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_sale',
                    python_callable=top_sale_def,
                    dag=dag)

t3 = PythonOperator(task_id='top_genre_eu',
                        python_callable=top_genre_eu_def,
                        dag=dag)

t4 = PythonOperator(task_id='1M_platform_NE',
                        python_callable=M_platform_def,
                        dag=dag)

t5 = PythonOperator(task_id='best_sales_JP',
                        python_callable=best_sales_JP_def,
                        dag=dag)

t6 = PythonOperator(task_id='EU_JP_sale',
                        python_callable=EU_JP_sale_def,
                        dag=dag)
t7 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4, t5, t6] >> t7
