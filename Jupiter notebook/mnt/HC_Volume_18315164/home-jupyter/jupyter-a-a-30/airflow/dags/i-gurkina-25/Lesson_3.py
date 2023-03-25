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
    'owner': 'i-gurkina-25', #владелец
    'depends_on_past': False, #зависимость от прошлого запуска
    'retries': 2, #количество перезапусков
    'retry_delay': timedelta(minutes=5), #время между перезапусками
    'start_date': datetime(2022, 10, 20),
    'schedule_interval': '0 12 * * *'
    }

CHAT_ID = -853716684
try:
    BOT_TOKEN = '5587703033:AAEMlbAhiBq4xkry5huVeteyKGNvj3J7v5U'
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
def i_gurkina_25_lesson_3():
    @task(retries=3) #декоратор
    def get_data():
    # запрашиваем данные
        top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
        zipfile = ZipFile(BytesIO(top_doms.content))
    # получаем таблицу на выходе
        top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    # получаем таблицу на выходе
        return top_data
    
    @task(retries=4, retry_delay=timedelta(10))
    # нужно получить на вход таблицу, обработанную в get_data()
    def get_table_ru(top_data):
    # рейтинг доменов, заканчивающихся на .ru
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
        top_data_ru = top_data_ru
        return top_data_ru.to_csv(index=False) #переводим в csv, чтобы json мог считать данные

    @task()
    # нужно получить на вход таблицу, полученную в get_table_ru()
    def get_stat_ru(top_data_ru):
    # среднее и медианное значение рейтинга доменов .ru
        ru_df = pd.read_csv(StringIO(top_data_ru))
        ru_avg = int(ru_df['rank'].aggregate(np.mean))
        ru_median = int(ru_df['rank'].aggregate(np.median))
        return {'ru_avg':ru_avg, 'ru_median':ru_median}

    @task()
    # нужно получить на вход таблицу, обработанную в get_data()
    def get_table_com(top_data):
    # рейтинг доменов, заканчивающихся на .com
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_com = top_data_df[top_data_df['domain'].str.endswith('.ru')]
        top_data_com = top_data_com
        return top_data_com.to_csv(index=False) #переводим в csv, чтобы json мог считать данные

    @task()
    # нужно получить на вход таблицу, полученную в get_table_com()
    def get_stat_com(top_data_com):
    # среднее и медианное значение рейтинга доменов .com
        com_df = pd.read_csv(StringIO(top_data_com))
        com_avg = int(com_df['rank'].aggregate(np.mean))
        com_median = int(com_df['rank'].aggregate(np.median))
        return {'com_avg':com_avg, 'com_median':com_median}

    @task(on_success_callback=send_message)
    def print_data(ru_stat, com_stat):

        context = get_current_context()
        date = context['ds']
        
        ru_avg, ru_median = ru_stat['ru_avg'], ru_stat['ru_median']
        com_avg, com_median = com_stat['com_avg'], com_stat['com_median']

        print(f'''Data from .RU for date {date}
                  Avg rank: {ru_avg}
                  Median rank: {ru_median}''')
        
        print(f'''Data from .COM for date {date}
                  Avg rank: {com_avg}
                  Median rank: {com_median}''')
   
    top_data = get_data()
    
    top_data_ru = get_table_ru(top_data)
    ru_data = get_stat_ru(top_data_ru)
    
    top_data_com = get_table_com(top_data)
    com_data = get_stat_com(top_data_com)
    
    print_data(ru_data, com_data)
    
i_gurkina_25_lesson_3 = i_gurkina_25_lesson_3()