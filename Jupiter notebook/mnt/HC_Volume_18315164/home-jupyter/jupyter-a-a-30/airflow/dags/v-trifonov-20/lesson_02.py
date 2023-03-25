import pandas as pd
from datetime import timedelta
from datetime import datetime
import csv

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


def get_top_10_domen():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_domens = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    top_data_top_10_domens = top_data_domens.value_counts()[:10]
#     print(f'top_data_top_10_domens\n{top_data_top_10_domens}')
    
    with open('top_10_domen.csv', 'w') as f:
        f.write(top_data_top_10_domens.to_csv(index=False, header=False))
    

def get_domen_long_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_name_domain = top_data_df['domain'].apply(lambda x: x.split('.')[0])
    top_data_long_name_domain = top_data_name_domain[top_data_name_domain.transform(len) == top_data_name_domain.transform(len).max()].sort_values(ignore_index=True)[0]
#     print(f'top_data_long_name_domain = {top_data_long_name_domain}')
    
#     with open('domen_long_name.csv', 'w') as f:
#         f.write(top_data_long_name_domain.to_csv(index=False, header=False))
    with open("domen_long_name.csv", mode="w", encoding='utf-8') as w_file:
        file_writer = csv.writer(w_file, delimiter = ",", lineterminator="\r")
        file_writer.writerow([top_data_long_name_domain])

    
def get_domain_index():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain = "airflow.com"
    domain_index = top_data_df.query('domain == @domain')
    if domain_index.shape[0] != 0:
        domain_index_str = f'Domain index {domain} = {domain_index.values[0][0]}'
    else:
        domain_index_str = f'Domain "{domain}" is not found!'
    
#     with open('domain_index.csv', 'w') as f:
#         f.write(domain_index_str.to_csv(index=False, header=False))
    with open("domain_index.csv", mode="w", encoding='utf-8') as w_file:
        file_writer = csv.writer(w_file, delimiter = ",", lineterminator="\r")
        file_writer.writerow([domain_index_str])
             
            
def print_data(ds):
    with open('top_10_domen.csv', 'r') as f:
        top_data_top_10_domens = f.read()
    with open('domen_long_name.csv', 'r') as f:
        top_data_long_name_domain = f.read()
    with open('domain_index.csv', 'r') as f:
        domain_index = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(top_data_top_10_domens)

    print(f'Top long domains for date {date}')
    print(top_data_long_name_domain)
    
    print(domain_index)


default_args = {
    'owner': 'v-trifonov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 6, 26),
}
schedule_interval = '0 10 * * *'

dag = DAG('v_trifonov_20_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domen',
                    python_callable=get_top_10_domen,
                    dag=dag)

t3 = PythonOperator(task_id='get_domen_long_name',
                    python_callable=get_domen_long_name,
                    dag=dag)

t4 = PythonOperator(task_id='get_domain_index',
                    python_callable=get_domain_index,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
