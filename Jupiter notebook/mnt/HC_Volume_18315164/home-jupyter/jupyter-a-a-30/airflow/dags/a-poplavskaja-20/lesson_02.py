import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_dz():
#     Найти топ-10 доменных зон по численности доменов
    top_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_df['domain_zone'] = top_df.domain.apply(lambda x: x.split('.')[1])
    
    top_10_dz = top_df \
    .groupby('domain_zone', as_index=False) \
    .agg({'domain': 'count'}) \
    .rename(columns={'domain': 'number'}) \
    .sort_values('number', ascending='False') \
    .head(10)
    
    with open('top_10_dz.csv', 'w') as f:
        f.write(top_10_dz.to_csv(index=False, header=False))


def get_domain_lenght():
#     Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
    top_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top_df['domain_length_name'] = top_df.apply(lambda x: len(str(x).split('.')[0]))
    
    max_len = top_df.domain_length_name.agg('max')
    
    max_lenght = top_df \
    .query('domain_length_name == @max_length') \
    .sort_values('domain') \
    .head(1)
    
    with open('max_lenght.csv', 'w') as f:
        f.write(max_lenght.to_csv(index=False, header=False))
        
        
def get_airflow_position():
#     На каком месте находится домен airflow.com?
    top_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    airflow_position = str(top_df.query("domain == 'airflow.com'")['rank'])
    
    with open('airflow_position.csv', 'w') as f:
        f.write(airflow_position)


def print_data(ds):
#   таск должен писать в лог результат ответы на вопросы выше
    with open('top_10_dz.csv', 'r') as f:
        top_10_dz_data = f.read()
    with open('max_lenght.csv', 'r') as f:
        max_lenght_data = f.read()
    with open('airflow_position.csv', 'r') as f:
        airflow_position_data = f.read()
    date = ds
                
    print(f'Top domains zones for date {date}')
    print(top_10_dz_data)

    print(f'Top domain name lenght for date {date}')
    print(max_lenght_data)
    
    print(f'For {date}')
    print(airflow_position_data)



default_args = {
    'owner': 'a.poplavskaja',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 4),
}
schedule_interval = '7 * * * *'

dag = DAG('a_poplavskaja_20_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_dz',
                    python_callable=get_top_dz,
                    dag=dag)

t2_addit = PythonOperator(task_id='get_domain_lenght',
                        python_callable=get_domain_lenght,
                        dag=dag)

t2_addit_2 = PythonOperator(task_id='get_airflow_position',
                        python_callable=get_airflow_position,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_addit, t2_addit_2] >> t3
