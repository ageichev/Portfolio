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
        
# Найти топ-10 доменных зон по численности доменов        
def get_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    #top_data_df['zone'] = top_data_df.domain.str.split('.').str[-1]
    domain_top_10 = top_data_df.zone.value_counts().head(10).reset_index().rename(columns={'index':'zone', 'zone':'amount'})
    with open('domain_top_10.csv', 'w') as f:
        f.write(domain_top_10.to_csv(index=False, header=False))
        
# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[0])
    top_data_df['length'] = top_data_df['zone'].str.len()
    longest_name = top_data_df.sort_values(['length', 'domain'], ascending={False, True})        .drop(['rank', 'zone', 'length'], axis=1).head(1)
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))
        
#На каком месте находится домен airflow.com?
def get_airflow_place():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[0])
    top_data_df['length'] = top_data_df['zone'].str.len()
    longest_name = top_data_df.sort_values(['length'], ascending=False).reset_index()
    airflow_place = longest_name.query("domain == 'airflow.com'").reset_index()
    if airflow_place.empty:
        result = 'Domain is not found'
    else:
        result = str(airflow_place.level_0)
    print(result)
    with open('airflow_place.txt', 'w') as f:
        f.write(result)
        
#Пишем ответы в лог
def print_answers(ds):
    with open('domain_top_10.csv', 'r') as f:
        top_10_domain = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_domain_name = f.read()
    with open('airflow_place.txt', 'r') as f:
        airflow_place = f.read()
    date = ds
    
    print(f'Top 10 domain zones are {date}')
    print(top_10_domain)

    print(f'Longest domain name is {date}')
    print(longest_domain_name)
    
    print(f'"airflow.com" in longest domain name list is in place {date}')
    print(airflow_place)
    
default_args = {
    'owner': 's-se',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 8),
}
schedule_interval = '15 15 * * *'

dag = DAG('s-se_lesson2_4', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_domain_zone',
                    python_callable=get_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_place',
                        python_callable=get_airflow_place,
                        dag=dag)

t5 = PythonOperator(task_id='print_answers',
                    python_callable=print_answers,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5