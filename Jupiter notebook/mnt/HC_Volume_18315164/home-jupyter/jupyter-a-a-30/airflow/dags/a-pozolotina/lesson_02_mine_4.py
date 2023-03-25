import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_domain_zone():
    top_data_df1 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    #добавили столбец с доменной зоной
    top_data_df1['domain_zone'] = top_data_df1.domain.str.rpartition('.')[2]
    #группировка по доменной зоне, сортировка от большего к меньшему
    top_data_df1 = top_data_df1.groupby('domain_zone', as_index = False).agg({'domain': 'count'})\
      .sort_values('domain', ascending = False).head(10)
    with open('top_data_df1.csv', 'w') as f:
        f.write(top_data_df1.to_csv(index=False, header=False))
        
        
def get_long_name():
    top_data_df2 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    #строка с максимальным количеством символов в названии - самое длинное имя
    top_data_df2 = top_data_df2.loc[top_data_df2.domain.str.len() == top_data_df2.domain.str.len().max()]
    with open('top_data_df2.csv', 'w') as f:
        f.write(top_data_df2.to_csv(index=False, header=False))

    
def get_position_airflow():
    top_data_df3 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    #выводит строку с местом по домену airflow.com
    top_data_df3 = top_data_df3.loc[top_data_df3.domain == 'airflow.com']
    with open('top_data_df3.csv', 'w') as f:
        f.write(top_data_df3.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_data_df1.csv', 'r') as f:
        data_df1 = f.read()
    with open('top_data_df2.csv', 'r') as f:
        data_df2 = f.read()
    with open('top_data_df3.csv', 'r') as f:
        data_df3 = f.read()   
    date = ds

    print(f'Top 10 domain zone for date {date}')
    print(data_df1)

    print(f'The longest domain name for date {date}')
    print(data_df2)
    
    print(f'The position domain airflow.com for date {date}')
    print(data_df3)


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 26),
}
schedule_interval = '0 12 * * *'

dag_by_pozolotina = DAG('domain_stat_by_pozolotina', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_by_pozolotina)

t2 = PythonOperator(task_id='get_top_domain_zone',
                    python_callable=get_top_domain_zone,
                    dag=dag_by_pozolotina)

t3 = PythonOperator(task_id='get_long_name',
                        python_callable=get_long_name,
                        dag=dag_by_pozolotina)

t4 = PythonOperator(task_id='get_position_airflow',
                        python_callable=get_position_airflow,
                        dag=dag_by_pozolotina)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_by_pozolotina)

t1 >> [t2, t3, t4] >> t5
