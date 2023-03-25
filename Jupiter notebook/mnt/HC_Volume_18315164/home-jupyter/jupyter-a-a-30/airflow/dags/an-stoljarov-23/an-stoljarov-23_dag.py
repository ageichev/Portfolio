import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from zipfile import ZipFile
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

#def get_data():
#    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
#    top_doms = pd.read_csv(TOP_1M_DOMAINS)
#    top_data = top_doms.to_csv(index=False)
#
#    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
#        f.write(top_data)


def top_10_doms():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_doms = top_doms
    top_10_doms = top_10_doms.domain.str.split('.').str[-1]
    top_10_doms = top_10_doms.groupby('domain_zone', as_index=False).agg({'domain': 'count'}) \
        .rename(columns={'domain': 'quantity'}) \
        .sort_values(by='quantity', ascending=False) \
        .head(10) \
        .reset_index(drop=True) \
        .domain_zone.to_list()
    with open('top_10_domzone.csv', 'w') as f:
        f.write(top_10_domzone.to_csv(index=False, header=False))


def lngst_dom():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['lng_dom'] = top_doms['domain'].str.count('.')
    top_doms = top_doms.sort_values(by='lng_dom', ascending=False)  \
        .reset_index(drop=True)
    lngst_dom = lngst_dom.iloc[0]['domain']
    with open('lngst_dom.csv', 'w') as f:
        f.write(lngst_dom.to_csv(index=False, header=False))
        

def af_rank():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    new_row = {'domain':'домена нет в списке', 'rank':'домена нет в списке'}
    af_rank = top_doms.query("domain == 'airflow.com'") \
        .reset_index(drop=True) \
        .append(new_row, ignore_index=True) \
        .iloc[0]['rank']
    with open('af_rank.csv', 'w') as f:
        f.write(af_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_domzone.csv', 'r') as f:
        top_10_data = f.read()
    with open('lngst_dom.csv', 'r') as f:
        lngst_data = f.read()
    with open('af_rank.csv', 'r') as f:
        af_data = f.read()
    date = ds

    print(f'Top 10 domains zones for date {date}')
    print(top_10_data)

    print(f'Longest domain for date {date}')
    print(lngst_data)

    print(f'airflow.com rank for date {date}')
    print(af_data)


default_args = {
    'owner': 'an-stoljarov-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 31),
}
schedule_interval = '0 8 * * *'

dag = DAG('an-stoljarov-23_top_10_ru_new', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_doms',
                    python_callable=top_10_doms,
                    dag=dag)

t3 = PythonOperator(task_id='lngst_dom',
                    python_callable=lngst_dom,
                    dag=dag)

t4 = PythonOperator(task_id='af_rank',
                    python_callable=af_rank,
                    dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1 >> t2 >> t3 >> t4 >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
