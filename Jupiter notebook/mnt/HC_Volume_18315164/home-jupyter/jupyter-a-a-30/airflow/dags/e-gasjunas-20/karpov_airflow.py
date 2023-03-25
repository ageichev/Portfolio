import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

#Получение данных
def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# Найти топ-10 доменных зон по численности доменов
# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
# На каком месте находится домен airflow.com?
        
def get_top_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10 = pd.DataFrame(top_data_df.domain.str.split(".").str[-1].value_counts())
    top_10 = top_10.reset_index().rename(columns = {'index': 'domain_zon', 'domain': 'size' })
    top_10 = top_10.head(10)
    with open('top_10.csv', 'w') as f:
        f.write(top_10.to_csv(index=False, header=False))



def get_len_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['number'] = top_data_df['domain'].str.len()
    len_name = top_data_df.set_index('domain')[['number']].idxmax()[0]
    longest_name = top_data_df[top_data_df['domain'] == len_name]
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))



def get_position():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = top_data_df.query('domain == "airflow.com"')
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10.csv', 'r') as f:
        data_top_10 = f.read()
    with open('longest_name.csv', 'r') as f:
        data_longest_name = f.read()
    with open('airflow.csv', 'r') as f:
        data_airflow = f.read()
    date = ds

    print(f'Top 10 domain: {date}')
    print(data_top_10)

    print(f'Domain with the longest name: {date}')
    print(data_longest_name)
    
    print(f'airflow.com ranking: {date}')
    print(data_airflow)

default_args = {
    'owner': 'e-gasjunas-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 2),
}
schedule_interval = '0 12 * * *'



dag = DAG('e-gasjunas-20', default_args=default_args, schedule_interval=schedule_interval)




t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)




t2_top_10 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)




t2_len_name = PythonOperator(task_id='get_len_name',
                        python_callable=get_len_name,
                        dag=dag)




t2_rank = PythonOperator(task_id='get_position',
                        python_callable=get_position,
                        dag=dag)




t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)



t1 >> [t2_top_10, t2_len_name, t2_rank] >> t3
