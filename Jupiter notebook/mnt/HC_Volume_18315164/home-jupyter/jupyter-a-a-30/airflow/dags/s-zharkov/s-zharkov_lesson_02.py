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


def get_top_10_domain_count(): # Найти топ-10 доменных зон по численности доменов
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top_data_df.domain = top_data_df.domain.str.split('.').str[-1]
    top_domain = top_data_df.groupby('domain', as_index=False).agg({'rank': 'count'})
    top_domain_count10 = top_domain.sort_values('rank', ascending=False).head(10)  
    
    with open('top_domain_count10.csv', 'w') as f:
        f.write(top_domain_count10.to_csv(index=False, header=False))        
        
def get_top_len(): # Найти домен с самым длинным именем 
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top_data_df['len'] = top_data_df.domain.str.len()
    top_len_data_df = top_data_df.sort_values(by=['len', 'domain'], ascending=[False, True])
    top_len_data_df = top_len_data_df.reset_index(drop=True)
    top_len_str = top_len_data_df.domain[0]
    
    with open('top_len_str.txt', 'w') as f:
        f.write(str(top_len_str))        
        
def get_airflow(): # На каком месте находится домен airflow.com
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    try:
        rank_af = int(top_doms_df.loc[top_doms_df['domain'] == 'airflow.com', 'rank'])
    except:
        rank_af = 'site not listed'  
        
    with open('rank_af.txt', 'w') as f:
        f.write(str(rank_af))  
               
def print_data():
    with open('top_domain_count10.csv', 'r') as f:
        top_domain = f.read()
    with open('top_len_str.txt', 'r') as f:
        top_len = f.read()
    with open('rank_af.txt', 'r') as f:
        rank_af = f.read()
        
    print('Domain site counter, top 10:')    
    print(top_domain)
    
    print('Domain with the longest name:') 
    print(top_len)
    
    print()
    print('Position airflow.com in the ranking:') 
    print(rank_af)


default_args = {
    'owner': 's-zharkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 22),
}
schedule_interval = '7 7 * * *'

dag = DAG('s-zharkov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain_count',
                    python_callable=get_top_10_domain_count,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_len',
                        python_callable=get_top_len,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5
