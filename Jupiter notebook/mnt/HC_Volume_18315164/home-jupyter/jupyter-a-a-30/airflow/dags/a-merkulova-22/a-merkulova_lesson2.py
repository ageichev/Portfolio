
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



def get_top_10_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_dom = pd.DataFrame(top_data_df.domain.str.split(".").str[-1].value_counts())
    top_10_dom = top_10_dom.reset_index().rename(columns = {'index': 'domain_zon', 'domain': 'size' })
    top_10_dom = top_10_dom.head(10)
    with open('top_10_dom.csv', 'w') as f:
        f.write(top_10_dom.to_csv(index=False, header=False))



def get_long_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['number'] = top_data_df['domain'].str.len()
    data_long = top_data_df.set_index('domain')[['number']].idxmax()[0]
    longest_name = top_data_df[top_data_df['domain'] == data_long]
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))


def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow = top_data_df.query('domain == "airflow.com"')
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_dom.csv', 'r') as f:
        date_top_10_dom = f.read()
    with open('longest_name.csv', 'r') as f:
        data_longest_name = f.read()
    with open('airflow.csv', 'r') as f:
        data_airflow = f.read()
    date = ds

    print(f'Top 10 domain zones by number of domains for date {date}')
    print(date_top_10_dom)

    print(f'Domain with the longest name for date {date}')
    print(data_longest_name)
    
    print(f'airflow.com for date {date} is:')
    print(data_airflow)


default_args = {
    'owner': 'a.merkulova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 26),
}
schedule_interval = '30 16 * * *'



dag = DAG('a-merkulova-22', default_args=default_args, schedule_interval=schedule_interval)




t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)




t2 = PythonOperator(task_id='get_top_10_dom',
                    python_callable=get_top_10_dom,
                    dag=dag)





t2_long = PythonOperator(task_id='get_long_name',
                        python_callable=get_long_name,
                        dag=dag)





t2_air = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)




t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)




t1 >> [t2, t2_long, t2_air] >> t3







