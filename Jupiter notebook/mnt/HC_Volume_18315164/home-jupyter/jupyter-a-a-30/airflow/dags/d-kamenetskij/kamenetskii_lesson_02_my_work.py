"""
Необходимо скопировать DAG из лекции - к себе

Необходимо выполнить:

1) Поменять имена dag на уникальные (лучше всего как-то использовать свой логин).

Поставить новую дату начала DAG и новый интервал (все еще должен быть ежедневным)

2) Удалить таски get_stat и get_stat_com. Вместо них сделать свои собственные, которые

считают следующие:

Найти топ-10 доменных зон по численности доменов
Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
На каком месте находится домен airflow.com?
3) Финальный таск должен писать в лог результат ответы на вопросы выше
"""

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
from airflow import DAG


from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
        
# def get_stat(): #функция записывает в файл топ-10 доменов .ru
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))
        

# def get_stat_com(): #функция записывает в файл топ-10 доменов .com
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10_com.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))
        

        
# Найти топ-10 доменных зон по численности доменов

def top_10_domain_zone():
    top_data_df_zone = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df_zone['domain_zone'] = top_data_df_zone['domain'].str.split('.').str[-1]
    top_data_top_10_domain_zone = top_data_df_zone.groupby('domain_zone', as_index=False) \
                                             .agg({'rank':'count'}) \
                                             .sort_values('rank', ascending = False) \
                                             .head(10)
    with open('top_data_top_10_domain_zone.csv', 'w') as f:
        f.write(top_data_top_10_domain_zone.to_csv(index=False, header=False))

# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)

def longest_domain_name():
    top_data_df_longest = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df_longest['domain_lenght'] = top_data_df_longest['domain'].apply(len)
    top_data_df_longest[['domain', 'domain_lenght']].sort_values('domain_lenght', ascending=False).reset_index().loc[0]['domain']

    with open('top_data_df_longest.csv', 'w') as f:
        f.write(top_data_df_longest.to_csv(index=False, header=False))

# На каком месте находится домен airflow.com?        
def airflow_rank():
    airflow_rank = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    with open('airflow_rank.csv', 'w') as f:
        if airflow_rank[airflow_rank['domain'] == 'airflow.com'].empty:                               
            f.write('airflow.com is not found')
        else:
            airflow_rank = airflow_rank[airflow_rank['domain']=='airflow.com']['rank']
            f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_data_top_10_domain_zone.csv', 'r') as f:
        top_data_top_10_zone = f.read()
        
    with open('top_data_df_longest.csv', 'r') as f:
        top_data_df_longest = f.read()
    
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
        
    
    date = ds

    print(f'Top 10 dommain zones for date {date}')
    print(top_data_top_10_zone)

    print(f'longest domain name for date {date}')
    print(top_data_df_longest)
    
    print(f'position airflow for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'd-kamenetskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 4),
}
schedule_interval = '0 12 * * *'

dag = DAG('d-kamenetskii', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain_zone',
                    python_callable=top_10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain_name',
                        python_callable=longest_domain_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)