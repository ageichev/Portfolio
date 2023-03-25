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


def get_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    zones_count = top_data_df \
                            .groupby('domain_zone') \
                            .agg({'domain': 'count'}) \
                            .reset_index() \
                            .sort_values('domain', ascending=False) \
                            .head(10)

    with open('zones_count.csv', 'w') as f:
        f.write(zones_count.to_csv(index=False, header=False))


def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_domain = max(top_data_df['domain'].apply(len))
    longest_domain_name = top_data_df[top_data_df['domain'].str.len() == longest_domain] \
                            .reset_index() \
                            .sort_values('domain') \
                            .domain \
                            .head(1)

    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain_name.to_csv(index=False, header=False))


def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = top_data_df \
                        .query("domain == 'airflow.com'") \
                        .reset_index() \
                        ['rank']

    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))


def print_data(ds):
    with open('zones_count.csv', 'r') as f:
        all_data = f.read()
    date = ds
    print(f'Top 10 domain zones by number of domains on {date}:')
    print(all_data)


    with open('longest_domain.csv', 'r') as f:
        all_data = f.read()
    print(f'The longest domain on {date}:')
    print(all_data)


    with open('airflow_rank.csv', 'r') as f:
        all_data = f.read()
    print(f'Airflow.com rank on {date}:')
    if all_data == '':
        print('Not in top 1M')
    else:
        print(all_data)



default_args = {
    'owner': 'g-grishin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 2)
}

schedule_interval = '30 13 * * *'


dag_grishin = DAG('dag_grishin',
                  default_args=default_args,
                  schedule_interval=schedule_interval)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_grishin)

t2_1 = PythonOperator(task_id='get_domain_zones',
                    python_callable=get_domain_zones,
                    dag=dag_grishin)

t2_2 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag_grishin)

t2_3 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag_grishin)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_grishin)


t1 >> [t2_1, t2_2, t2_3] >> t3
