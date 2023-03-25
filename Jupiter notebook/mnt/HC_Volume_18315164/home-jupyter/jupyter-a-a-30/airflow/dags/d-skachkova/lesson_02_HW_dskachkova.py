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

def get_top_10_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_zones = top_data_df
    top_zones['zones'] = top_data_df.domain.str.split('.')
    top_zones['zone'] = top_zones.zones.apply(lambda x: x[-1])
    top_10_zones = top_zones.groupby('zone', as_index=False) \
                                 .agg({'domain':'count'}) \
                                 .sort_values('domain', ascending=False).head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))

def get_domain_longest():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_lenth = top_data_df
    domain_lenth['dom_len'] = top_data_df.domain.str.len()
    domain_longest = domain_lenth.sort_values(['dom_len','domain'], ascending=[False,True]).domain.iloc[0]
    domain_longest = pd.DataFrame([domain_longest])
    with open('domain_longest.csv', 'w') as f:
        f.write(domain_longest.to_csv(index=False, header=False))
        
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = []
    if top_data_df['domain'].apply(lambda x: x == 'airflow.com').any():
        airflow_rank.append(top_data_df.loc[top_data_df['domain'] == 'airflow.com', 'rank'].iloc[0])
    else:
        airflow_rank.append('there is no airflow.com domain in this data')
    airflow_rank = pd.DataFrame([airflow_rank])
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))
        
        
# def get_stat():
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))


# def get_stat_com():
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10_com.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('domain_longest.csv', 'r') as f:
        domain_longest = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_zones)

    print(f'The longest domain name for date {date}')
    print(domain_longest)
    
    print(f'airflow.com domain rank for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'd.skachkova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 1),
}
schedule_interval = '11 11 * * *'

dag = DAG('dag_HW_dskachkova', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zones = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
                    dag=dag)

t2_lenth = PythonOperator(task_id='get_domain_longest',
                        python_callable=get_domain_longest,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zones, t2_lenth, t2_airflow] >> t3

#t1.set_downstream(t2_zones)
#t1.set_downstream(t2_lenth)
#t1.set_downstream(t2_airflow)
#t2_zones.set_downstream(t3)
#t2_lenth.set_downstream(t3)
#t2_airflow.set_downstream(t3)
