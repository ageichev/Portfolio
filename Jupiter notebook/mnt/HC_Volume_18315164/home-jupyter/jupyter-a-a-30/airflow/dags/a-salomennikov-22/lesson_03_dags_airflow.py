import requests
from zipfile import ZipFile
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import BytesIO
from io import StringIO

from airflow.decorators import dag, task


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {
    'owner': 'a-salomennikov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 8, 2),
    'schedule_interval': '0 12 * * *'
}


@dag(default_args=default_args, catchup=False)
def dag2_salomennikov():
    @task()
    def read_data():
        top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
        zipfile = ZipFile(BytesIO(top_doms.content))
        top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
        print("read_data DONE")
        return top_data

    @task()
    def top_domain_zone(top_data):
       df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
       df['top_domain'] = df.domain.str.split('.').str[-1]
       top_10_domain_zone = df.groupby('top_domain', as_index=False).agg({'domain': 'count'}).sort_values('domain', ascending=False).head(10)["top_domain"]
       return top_10_domain_zone

    @task()
    def long_name_domain(top_data):
        df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        long_name_domain = df.sort_values(by="domain", key=lambda x: x.str.len(), ascending=False).head(1)['domain'].iat[0]
        return "Longest domain name is: " + long_name_domain

    @task()
    def where_is_airflow_com(top_data):
        df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        try:
            airflow_rank = '\nAirflow.com rank is: ' + str(df[df.domain.str.startswith('airflow.com')].head(1).values[0][0])
        except:
            airflow_rank = "\nThere is no 'airflow.com' domain in the list."
        return airflow_rank

    @task()
    def log_create(top_domain_zone, long_name_domain, where_is_airflow_com):
        with open('data_log.txt', 'w') as f:
            f.write('Top 10 domain zone:\n')
            f.write(top_domain_zone.to_csv(index=False, header=False))
            f.write(long_name_domain)
            f.write(where_is_airflow_com)
        print("ALL DONE")

    data = read_data()
    top_10_domain_zone = top_domain_zone(data)
    long_name_domain = long_name_domain(data)
    airflow_rank = where_is_airflow_com(data)
    log_create(top_10_domain_zone, long_name_domain, airflow_rank)


dag2_salomennikov = dag2_salomennikov()
