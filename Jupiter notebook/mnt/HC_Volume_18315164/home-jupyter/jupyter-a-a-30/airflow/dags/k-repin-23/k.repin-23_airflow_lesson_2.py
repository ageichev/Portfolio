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

# Топ-10 доменных зон по численности доменов
        
def get_top_domain_quantity():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])   
    top_data_df['zone']=top_data_df['domain'].str.split('.').str[1]   
    top_data_top_10 = top_data_df.groupby('zone', as_index=False).agg({'domain':'count'}).sort_values('domain', ascending=False).head(10)   
    with open('get_top_domain_quantity.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


# Домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)        
        
def get_top_domain_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_data_df['len']=top_data_df['domain'].str.len()
    top_lenghth_data = top_data_df.loc[top_data_df['len']==top_data_df['len'].max()].sort_values('domain').head(1)
    with open('get_top_domain_length.csv', 'w') as f:
        f.write(top_lenghth_data.to_csv(index=False, header=False))
    
# На каком месте находится домен airflow.com?

def get_airflow_domain_position():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    position = top_data_df.loc[top_data_df['domain']=='airflow.com']
    with open('get_airflow_domain_position.csv', 'w') as f:
        f.write(position.to_csv(index=False, header=False))
        

def print_data(ds):
    with open('get_top_domain_quantity.csv', 'r') as f:
        all_data = f.read()
    with open('get_top_domain_length.csv', 'r') as f:
        all_data_com = f.read()
    with open('get_airflow_domain_position.csv', 'r') as f:
        all_data_com_1 = f.read()    
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(all_data)

    print(f'Top domains lenght for date {date}')
    print(all_data_com)

    print(f'Airflow domain position for date {date}')
    print(all_data_com_1)

default_args = {
    'owner': 'k.repin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 9, 1),
}
schedule_interval = '0 19 * * *'

dag_krepin23 = DAG('dag_krepin23', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_krepin23)

t2 = PythonOperator(task_id='get_top_domain_quantity',
                    python_callable=get_top_domain_quantity,
                    dag=dag_krepin23)

t3 = PythonOperator(task_id='get_top_domain_length',
                    python_callable=get_top_domain_length,
                    dag=dag_krepin23)

t4 = PythonOperator(task_id='get_airflow_domain_position',
                    python_callable=get_airflow_domain_position,
                    dag=dag_krepin23)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_krepin23)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t3)
#t1.set_downstream(t4)
#t2.set_downstream(t5)
#t3.set_downstream(t5)
#t4.set_downstream(t5)

