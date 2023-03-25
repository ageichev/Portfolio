import requests
import pandas as pd
import numpy as np
import datetime
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


default_args = {
    'owner': 'i.loladze',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2022, 11, 6),
    'schedule_interval': '0 14 * * *'
}

@dag(default_args=default_args, catchup=False)
def testing_i_loladze():
    @task(retries=2)
    def get_cbr_api_data(a, b, c):
        print('func began')
        try:
            cbr_api_valutes = pd.read_json('https://www.cbr-xml-daily.ru/daily_json.js') 
        except:
            return pd.DataFrame([])

        cbr_api_valutes_columns = cbr_api_valutes.columns
        if ('Date' not in cbr_api_valutes_columns or 'Valute' not in cbr_api_valutes_columns):
            return pd.DataFrame([])
        print(cbr_api_valutes.head())
        return cbr_api_valutes
    
    a = datetime.datetime.now()
    b = 1
    c = pd.DataFrame([])
    cbr_api_valutes = get_cbr_api_data(a, b, c)

testing_i_loladze = testing_i_loladze()