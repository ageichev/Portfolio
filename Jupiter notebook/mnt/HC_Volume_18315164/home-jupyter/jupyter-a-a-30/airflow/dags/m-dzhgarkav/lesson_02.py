import requests
import pandas as pd
# import os
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


def get_top10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_data_top_10 = top_data_df.groupby(['zone'], as_index=False).size().sort_values('size', ascending=False)
    top_data_top_10 = top_data_top_10.head(10)
    top_data_top_10[['zone', 'size']].to_csv('top_10_domain_data.csv', index=False)



def get_top_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df['domain'].str.len()
    top_data_len = top_data_df.sort_values(['len','domain'], ascending=[False,True])
    top_data_len = top_data_len.head(1)
    top_data_len[['domain', 'len']].to_csv('top_data_len.csv', index=False)


def get_domain_index(dn = 'airflow.com'):
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_com = top_data_df[top_data_df['domain'] == dn]
    print(airflow_com)
    airflow_com.to_csv('airflow_com.csv', index=True)




def print_data(ds):
    # with open('top_10_domain_data.csv', 'r') as f:
    #     all_data = f.read()
    # with open('top_data_top_10_com.csv', 'r') as f:
    #     all_data_com = f.read()
    # date = ds
    date = ds
    top_10_domain_data = pd.read_csv('top_10_domain_data.csv', index_col=False)
    print(f'Top 10 zones for date {date}')
    print(top_10_domain_data)

    print()
    print('-------------------------------------------------------------------------')
    print()

    top_data_len = pd.read_csv('top_data_len.csv', index_col=False)
    top_len_domein =top_data_len['domain'][0]
    top_len = top_data_len['len'][0]
    print(f'Top len domains - {top_len_domein} , len - {top_len} charsfor  for date {date}')

    print()
    print('-------------------------------------------------------------------------')
    print()

    airflow_com = pd.read_csv('airflow_com.csv', index_col=False)
    if airflow_com.shape[0] == 0:
        print(f'Domains airflow.com for date {date} not found')
    else:
        rn = airflow_com['rank'][0]
        print(f'Range {rn} len domains for date {date}')

    print()
    print('-------------------------------------------------------------------------')
    print()


default_args = {
    'owner': 'm-dzhgarkav',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 15),
}
schedule_interval = '0 12 * * *'

dag = DAG('m-dzhgarkav_top_10_ru_new', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get__data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10',
                    python_callable=get_top10,
                    dag=dag)

t2_len = PythonOperator(task_id='get_top_len',
                        python_callable=get_top_len,
                        dag=dag)
t2_index = PythonOperator(task_id='get_domain_index',
                        python_callable=get_domain_index,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_len, t2_index] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)