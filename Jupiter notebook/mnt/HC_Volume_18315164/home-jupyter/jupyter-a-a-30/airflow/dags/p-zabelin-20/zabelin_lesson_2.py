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


def get_top10_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.domain.str.split('.').str[-1]
    top10_domain = df.groupby('zone', as_index=False).agg({'domain': 'count'}).sort_values('domain', ascending=False).head(10)   
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top10_domain.to_csv(index=False, header=False))


def get_len_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['len_domain'] = df.domain.map(len)
    len_domain = df.sort_values(['len_domain','domain'], ascending=[False,True]).iloc[0].domain  
    with open('len_domain.txt', 'w') as f:
        f.write(len_domain)
        
        
def get_airflow():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if df[df.domain == 'airflow.com'].shape[0] == 0:
        result = 'Нет домена в файле'
    else:
        result = df[df.domain == 'airflow.com'].iloc[0]['rank']
    with open('top_data_aifrlow.csv', 'w') as f:
        f.write(str(result))       


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_data_top_10 = f.read()
    with open('len_domain.txt', 'r') as f:
        len_domain = f.read()
    with open('top_data_aifrlow.csv', 'r') as f:
        top_data_aifrlow = f.read()    
    date = ds

    print(f'Топ-10 доменных зон {date}')
    print(top_data_top_10)

    print(f'Домен с самым длинным именем {date}')
    print(len_domain)
    
    print(f'Место airflow.com на дату  {date}')
    print(top_data_aifrlow)


default_args = {
    'owner': 'p-zabelin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 1),
}
schedule_interval = '0 13 * * *'

dag = DAG('p_zabelin_dag_lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='get_top10_domain',
                    python_callable=get_top10_domain,
                    dag=dag)

t2_Len = PythonOperator(task_id='get_len_domain',
                        python_callable=get_len_domain,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top, t2_Len, t2_airflow] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
