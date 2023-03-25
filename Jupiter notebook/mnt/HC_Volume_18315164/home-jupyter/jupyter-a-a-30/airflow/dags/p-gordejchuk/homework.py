# imports
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# inputs
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# Functions-------------------------------------------------------------------------------------------------------------
def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    amaz_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    data = "rank,domain\n" + amaz_data

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(data)


def top_10_domain_zones():
    # read data
    df = pd.read_csv(TOP_1M_DOMAINS_FILE)

    # process a df
    top_df = (
        df.domain.apply(lambda x: x.split('.')[-1])
        .value_counts()
        .head(10)
    )

    # write df to a csv
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_df.to_csv(header=False))


def longest_domain_name():
    # read data
    df = pd.read_csv(TOP_1M_DOMAINS_FILE)

    # process a df
    df['domain_len'] = (df.domain.apply(lambda x: len(x)))
    longest_domain = (
        df
        .sort_values(by=['domain_len', 'domain'], ascending=[False, True])
        [["domain", "domain_len"]]
        .head(1)
    )

    # write df to a csv
    with open('longest_domain_name.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))


def airflow_rank():
    # read data
    df = pd.read_csv(TOP_1M_DOMAINS_FILE)

    # process df
    airflow_rank_df = df[df.domain == "airflow.com"]['rank']

    # write df to a csv
    with open('airflow_rank.csv', 'w') as f:
        f.write(str(int(airflow_rank_df)))


def print_results(ds):

    date = ds

    # read results
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_dz = f.read()

    with open('longest_domain_name.csv', 'r') as f:
        longest_domain = f.read()
        name = longest_domain.split(',')[0]
        length = longest_domain.split(',')[1]

    with open('airflow_rank.csv', 'r') as f:
        af_rank = f.read()

    # print results

    # header
    print('')
    print('---DAG RESULTS---')
    print('')

    # task 1
    print(f"1. Top 10 domain zones for {date} are:")
    print(top_10_dz)

    # task 2
    print(f"2. Domain with the longest name for {date} is:")
    print(name)
    print(f"It's length is {int(length)}.")
    print('')

    # task 3
    print(f"3. Domain airflow.com is on the {af_rank} place in Global Ranking for {date}.")

    # tail
    print('')
    print("---END OF DAG RESULTS---")


# DAG and tasks---------------------------------------------------------------------------------------------------------
default_args = {
    'owner': 'p-gordejchuk ',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 12),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('pgord-hw', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='top_10_domains',
                        python_callable=top_10_domain_zones,
                        dag=dag)

t2_len = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain_name,
                        dag=dag)

t2_af_rank = PythonOperator(task_id='airflow_rank',
                            python_callable=airflow_rank,
                            dag=dag)

t3 = PythonOperator(task_id='print_results',
                    python_callable=print_results,
                    dag=dag)

# task order
t1 >> [t2_top, t2_len, t2_af_rank] >> t3
