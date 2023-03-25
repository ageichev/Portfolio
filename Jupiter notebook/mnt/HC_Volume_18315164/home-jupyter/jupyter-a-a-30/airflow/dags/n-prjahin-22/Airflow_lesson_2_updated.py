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


def get_top_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df["domain"] = df["domain"].astype(str)
    top_10_domain_zone = pd.DataFrame(df.domain.str.split(".").str[-1].value_counts())
    top_10_domain_zone = top_10_domain_zone.reset_index().rename(columns = {'index': 'domain_zones', 'domain': 'size' })
    top_10_domain_zone = top_10_domain_zone.head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))

def get_max_domain_length():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df["domain"] = df["domain"].astype(str)
    df["domain_length"] = df["domain"].str.len()
    data_long = df.set_index('domain')[['domain_length']].idxmax()[0]
    top_length = df[df['domain'] == data_long]
    with open('top_length.csv', 'w') as f:
        f.write(top_length.to_csv(index=False, header=False))
        
def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_airflow = df.query('domain == "airflow.com"')
    with open('df_airflow.csv', 'w') as f:
        f.write(df_airflow.to_csv(index=False, header=False))

        
def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        all_data_top_10 = f.read()
    with open('top_length.csv', 'r') as f:
        all_data_length = f.read()
    with open('df_airflow.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top 10 domain zone for date {date}')
    print(all_data_top_10)

    print(f'Top domain length for date {date}')
    print(all_data_length)
    
    print(f'Domain contains "airflow" for date {date}')
    print(all_data_airflow)


default_args = {
    'owner': 'n.pryakhin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
}
schedule_interval = '30 11 * * *'

dag = DAG('top_domains_in_universe', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domain',
                    python_callable=get_top_domain,
                    dag=dag)

t2_len = PythonOperator(task_id='get_max_domain_length',
                        python_callable=get_max_domain_length,
                        dag=dag)

t2_air = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_len, t2_air] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_len)
#t1.set_downstream(t2_air)
#t2.set_downstream(t5)
#t2_len.set_downstream(t5)
#t2_air.set_downstream(t5)
