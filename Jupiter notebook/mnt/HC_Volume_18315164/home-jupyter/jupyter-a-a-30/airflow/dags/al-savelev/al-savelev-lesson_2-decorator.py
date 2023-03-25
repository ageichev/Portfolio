import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {
    'owner': 'al-savelev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 26),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = -620798068
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def top_domains():

    @task(retries=3)
    def get_data():
        top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
        zipfile = ZipFile(BytesIO(top_doms.content))
        top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
        return top_data

    @task(retries=4, retry_delay=timedelta(10))
    def get_top_10(top_data):
        data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_10 = data_df.domain.head(10)
        return top_10.to_csv(index=False, header=False)
    
    @task()
    def get_longest_name(top_data):
        data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        data_df['name_len'] = data_df.domain.apply(len)
        longest_name = data_df.query('name_len == name_len.max()').domain
        return longest_name.to_csv(index=False, header=False)

    @task()
    def get_airflow_rank(top_data):
        data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        if data_df.query('domain == "airflow.com"').empty:
            airflow_rank = 'airflow.com is not in top domains'
        else:
            airflow_rank = data_df.query('domain == "airflow.com"').rank
        return airflow_rank

    @task(on_success_callback=send_message)
    def print_data(get_top_10, get_longest_name, get_airflow_rank):
        
        context = get_current_context()
        date = context['ds']

        print(f'Top 10 domains for date {date}')
        print(get_top_10)

        print(f'The longest domain name for date {date}')
        print(get_longest_name)

        print(f'The airflow.com rank in top domains for date {date}')
        print(get_airflow_rank)
    

    top_data = get_data()
    get_top_10 = get_top_10(top_data)
    get_longest_name = get_longest_name(top_data)
    get_airflow_rank = get_airflow_rank(top_data)
    
    print_data(get_top_10, get_longest_name, get_airflow_rank)

top_domains = top_domains()