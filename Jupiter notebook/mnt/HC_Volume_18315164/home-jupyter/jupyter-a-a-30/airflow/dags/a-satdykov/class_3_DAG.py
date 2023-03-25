import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

# from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable 
import telegram 



TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


    
# зададим дефолтные параметры

default_args = {
    'owner': 'a-satdykov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 21),
    'schedule_interval': '0 25 * * *'
}

# создаём бот для отчётов в тг

CHAT_ID = -897432063   # это нужно для того, чтобы писла в ТГ
try: 
    BOT_TOKEN = Variable.get('telegram_secret')
except: 
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    
    if BOT_TOKEN != '':
        bot = telegram.Bot(token = BOT_TOKEN)
        bot.send_message(chat_id = CHAT_ID, text = message)
    else:
        pass



# Зададим даг через функцию

@dag(default_args = default_args, catchup = False)
def a_satdykov_top_10_airflow():
    
    # таски
    # первый таск - забирает таблицу

    @task(retries = 3) # так мы задаём таск, через декоратор
    def get_data():
        top_doms = requests.get(TOP_1M_DOMAINS, stream = True)
        zipfile = ZipFile(BytesIO(top_doms.content))
        top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

        return top_data

    # второй таск - соберём статистику - точнее берём её по РФ

    #@task(retries = 4, retry_delay = timedelta(10)) # так мы задаём таск, через декоратор
    #def get_stat_ru(top_data):
    #    top_data_df = pd.read_csv(StringIO(top_data), names = ['rank', 'domain'])
    #    top_data_top_10_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
    #    top_data_top_10_ru = top_data_top_10_ru.head(10)

    #    return top_data_top_10_ru.to_csv(index = False)
    
    @task(retries = 4, retry_delay = timedelta(10)) # так мы задаём таск, через декоратор
    def get_table_ru(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names = ['rank', 'domain'])
        top_data_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
        

        return top_data_ru.to_csv(index = False)    
    
    # а ещё одним таском мы пробуем откопать всю статистику по РФ
    
    @task()
    def get_stat_ru (top_data_ru):
        ru_df = pd.read_csv(StringIO(top_data_ru))
        ru_avg = int(ru_df['rank'].aggregate(np.mean))
        ru_median = int(ru_df['rank'].aggregate(np.median))
        
        return {'ru_avg': ru_avg, 'ru_median' : ru_median}
    
    
    
    # добавим ещё задачу посмотреть в зоне .COM и считаем по ним статистику 

    @task()
    def get_table_com(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names = ['rank', 'domain'])
        top_data_com = top_data_df[top_data_df['domain'].str.endswith('.com')]
        
        return top_data_com.to_csv(index = False)
    
    @task()
    def get_stat_com (top_data_com):
        com_df = pd.read_csv(StringIO(top_data_com))
        com_avg = int(com_df['rank'].aggregate(np.mean))
        com_median = int(com_df['rank'].aggregate(np.median))
        
        return {'com_avg': com_avg, 'com_median' : com_median}
    


    # ну и третье задание - полученный файл и напием лог с заголовком

    @task(on_success_callback = send_message)
    def print_data(ru_stat, com_stat):
        
        context = get_current_context()
        date = context['ds']
        
        
        ru_avg, ru_median = ru_stat['ru_avg'], ru_stat ['ru_median']
        com_avg, com_median = com_stat['com_avg'], com_stat ['com_median']
        
        
        print (f'''Data from .RU domain for {date}
                   Avg rank: {ru_avg}
                   Median rank: {ru_median}''')
        
        print (f'''Data from .COM domain for {date}
                   Avg rank: {com_avg}
                   Median rank: {com_median}''')


        # print(f'Top domains in .RU for date {date}')
        # print (top_data_top_10_ru)

        # print(f'Top domains in .COM for date {date}')
        # print (top_data_top_10_com)
        
        
    # и задаём последовательность тасков
    top_data = get_data ()
    
    top_data_ru = get_table_ru (top_data)
    ru_data = get_stat_ru (top_data_ru)
    
    top_data_com = get_table_com (top_data)
    com_data = get_stat_com (top_data_com)
    
    # top_data_top_10_ru = get_stat_ru (top_data)
    # top_data_top_10_com = get_stat_COM (top_data)
    print_data (ru_data, com_data)
    

# запускаем наш даг

a_satdykov_top_10_airflow = a_satdykov_top_10_airflow()


