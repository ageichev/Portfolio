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


def get_top10_domains_number():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['position', 'domain'])
    
    df['particular_domain'] = df.domain.str.split('.').str[-1]
    top_10_domains_number = df.groupby('particular_domain', as_index=False) \
                                .domain \
                                .count() \
                                .sort_values('domain', ascending=False) \
                                .head(10) \
                                .reset_index(drop=True)
    
    with open('top_data_top_10_domains_number.csv', 'w') as f:
        f.write(top_10_domains_number.to_csv(index=False, header=False))

        
def get_longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['position', 'domain'])
    
    df['domain_length'] = df.domain.apply(lambda x: len(x))
    longest_domain = df.iloc[df.sort_values('domain_length', ascending=False).head(1).index[0]].domain
    
    with open('top_data_longest_domain.csv', 'w') as f:
        f.write(longest_domain + "\n")

        
def get_particular_domain_position(searching_domain = 'airflow.com'):
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['position', 'domain'])
    
    if not df[df.domain.str.contains(searching_domain, regex=False)].empty:
        potition_number = df.query('domain == @searching_domain').position.sum()
        position = f'"{searching_domain}" is on {potition_number} place'
    else:
        position = f'Searched value "{searching_domain}" is absent in downloaded rank'
        
    with open('top_data_particular_domain_position.csv', 'w') as f:
        f.write(position + "\n")

        
def print_data(ds):
    with open('top_data_top_10_domains_number.csv', 'r') as f:
        top_10_domains_number = f.read()
    with open('top_data_longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('top_data_particular_domain_position.csv', 'r') as f:
        particular_domain_position = f.read()   
    date = ds

    print(f'Top 10 domains by count for date {date}')
    print(top_10_domains_number)

    print(f'The logest domain name for date {date}')
    print(longest_domain)
    
    print(f'{particular_domain_position} for date {date}')
    
              

default_args = {
    'owner': 'd-barkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 26),
}
schedule_interval = '0 13 * * *'


dag = DAG('d_barkov_top_10_ru_new', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_domains_number',
                    python_callable=get_top10_domains_number,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_particular = PythonOperator(task_id='get_particular_domain_position',
                        python_callable=get_particular_domain_position,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_longest, t2_particular] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)

