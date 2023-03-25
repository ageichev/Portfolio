# импорт библиотек

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
        
        
def top_10_domen_aria():
    # топ 10 доменныйх зон
    top_data_df     = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df.domain.str.split('.',expand=True)[[0, 1]].rename(columns={0:'domain',1:'aria'})
    top_data_top_10 = top_data_top_10.groupby('domain',as_index=False)\
                                    .aria.count().sort_values('aria',ascending=False).head(10)
    
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))

        
def big_size_word():
    # самое большее имя домена
    df_for_size              = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_for_size['size_word'] = df_for_size.domain.apply(len)
    big_size_word            = df_for_size[df_for_size.size_word==df_for_size.size_word.max()]\
                                                 .sort_values('domain',ascending=True)
    
    with open('big_size.csv', 'w') as f:
        f.write(big_size_word.to_csv(index=False, header=False))
        
        
def where_are_you():
    # место у airflow.com
    where_are_you = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

    if where_are_you.query("domain=='airflow.com'").shape[0]==0:
        answer = 'None'
        answer = pd.Series(answer)
    else:
        answer = where_are_you.query("domain=='airflow.com'")
        
    with open('Answer.csv', 'w') as f:
        f.write(answer.to_csv(index=False, header=False))  
        
        
def print_data(ds):
    
    with open('top_data_top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('big_size.csv', 'r') as f:
        big_word  = f.read()
    with open('Answer.csv', 'r') as f:
        answer_q = f.read()
    date = ds


    print(f'Топ 10 доменов {date}')
    print(top_10)

    print(f'Самый большой домен {date}')
    print(big_word)
              
    print(f'На каком месте airflow.com {date}')
    print(answer_q)

####

default_args = {
    'owner': 'a-kartashov-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 20),
}
schedule_interval = '1 21 * * *'
# даг
              
dag = DAG('my_first_dag', default_args=default_args, schedule_interval=schedule_interval)
# таски
              
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domen_aria',
                    python_callable=top_10_domen_aria,
                    dag=dag)

t3 = PythonOperator(task_id='where_are_you',
                        python_callable=where_are_you,
                        dag=dag)

t4 = PythonOperator(task_id='big_size_word',
                    python_callable=big_size_word,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5