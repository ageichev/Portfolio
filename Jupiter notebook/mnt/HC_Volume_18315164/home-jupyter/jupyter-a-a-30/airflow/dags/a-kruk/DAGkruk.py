import pandas as pd
import requests
from zipfile import ZipFile
from io import BytesIO

from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


Data_url = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
Domain_data = 'top-1m.csv'

def get_data():
    data = requests.get(Data_url, stream=True)
    zipfile = ZipFile(BytesIO(data.content))
    domain_data = zipfile.read(Domain_data).decode('utf-8')

    with open(Domain_data, 'w') as f:
        f.write(domain_data)


def top_10_domain():
    data = pd.read_csv(Domain_data, names=['rank', 'name'])
    data['domain'] = data.name.apply(lambda x: x.split('.')[-1])
    top_10_domain = data \
                        .groupby('domain', as_index=False) \
                        .agg({'rank':'count'}) \
                        .sort_values('rank', ascending=False) \
                        .head(10).domain
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))


def top_length():
    data = pd.read_csv(Domain_data, names=['rank', 'name'])
    data['name_length'] = data.name.apply(lambda x: len(x))
    top_length = data.sort_values('name_length', ascending=False).head(1).name
    with open('top_length.csv', 'w') as f:
        f.write(top_length.to_csv(index=False, header=False))


def airflow_com():
    data = pd.read_csv(Domain_data, names=['rank', 'name'])
    data['name_length'] = data.name.apply(lambda x: len(x))
    data_sort = data \
                .sort_values('name_length', ascending=False) \
                .reset_index() \
                .drop(['index', 'rank'], axis=1)
    airflow_df = data_sort.loc[data_sort.name=='airflow.com']

    if not airflow_df.empty:
        position = f'airflow.com domain position is {airflow_df.index[0] + 1}'
    else:
        position = 'There is no airflow.com domain in dataframe'
    with open('airflow_position.csv', 'w') as f:
        f.write(position)


def print_data(ds):
    with open('top_10_domain.csv', 'r') as f:
        top_10_domain = f.read()
    with open('top_length.csv', 'r') as f:
        top_length = f.read()
    with open('airflow_position.csv', 'r') as f:
        airflow_position = f.read()
        
    date = ds
    
    print(f'Top 10 domain zones by number of domains for date {date}')
    print(top_10_domain)

    print(f'Domain with the longest name for date {date}')
    print(top_length)
    
    print(f'For date {date}')
    print(airflow_position)


default_args = {
    'owner': 'a.kruk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 22),
}
schedule_interval = '@daily'

dag = DAG('dag_kruk1', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top10 = PythonOperator(task_id='top_10_domain',
                    python_callable=top_10_domain,
                    dag=dag)

t2_toplen = PythonOperator(task_id='top_length',
                        python_callable=top_length,
                        dag=dag)

t2_af = PythonOperator(task_id='airflow_com',
                        python_callable=airflow_com,
                        dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top10, t2_toplen, t2_af] >> t3
