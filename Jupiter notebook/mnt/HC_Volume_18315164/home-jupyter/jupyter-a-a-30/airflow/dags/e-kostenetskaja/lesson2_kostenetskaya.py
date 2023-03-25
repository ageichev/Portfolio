import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

#Получаем данные
def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
#Найти топ-10 доменных зон по численности доменов
def get_top_10():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_df['domain_name'] = data_df.domain.apply(lambda x: x.split('.')[-1])
    data_top_10 = data_df.groupby('domain_name', as_index = False) \
    .agg({'domain' : 'count'}) \
    .sort_values('domain', ascending = False).domain_name.head(10)
    
    with open('data_top_10.csv', 'w') as f:
        f.write(data_top_10.to_csv(index=False, header=False))
        
#Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_df['domain_length'] = data_df.domain.apply(lambda x: len(x))
    longest_domain = data_df.sort_values(by=['domain_length', 'domain'], ascending=[False, True]).domain.head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

#На каком месте находится домен airflow.com
def get_position():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    position_airflow = data_df.query("domain == 'airflow.com'")['rank']
    with open('position_airflow.csv', 'w') as f:
        f.write(position_airflow.to_csv(index=False, header=False))
        
#Пишем в лог ответы на вопросы
def print_data(ds):
    with open('data_top_10.csv', 'r') as f:
        top10 = f.read()
    with open('longest_domain.csv', 'r') as f:
        long_dom = f.read()
    with open('position_airflow.csv', 'r') as f:
        pos_air = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(top10)

    print(f'The longest domain for date {date}')
    print(long_dom)
    
    print(f'Airflow domain position for date {date}')
    print(pos_air)
    
#Задаем параметры в DAG
default_args = {
    'owner': 'e-kostenetskaja',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 27),
}
schedule_interval = '0 10 * * *'

dag = DAG('e-kostenetskaja', default_args=default_args, schedule_interval=schedule_interval)

#Инициализируем таски
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest',
                        python_callable=get_longest,
                        dag=dag)

t4 = PythonOperator(task_id='get_position',
                    python_callable=get_position,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5