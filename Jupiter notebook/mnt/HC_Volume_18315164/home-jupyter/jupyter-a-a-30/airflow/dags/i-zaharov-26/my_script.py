# блок импортов
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# блок доменов
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'
# Таски


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_stat_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['d_zone'] = top_data_df['domain'].str.split(".").str[-1]
    top_data_10_domain = top_data_df['d_zone'].value_counts().head(10)
    with open('top_data_10_domain.csv', 'w') as f:
        f.write(top_data_10_domain.to_csv(index=False, header=False))


def get_stat_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['d_name_len'] = top_data_df['domain'].apply(lambda x: len(x))
    top_data_len = top_data_df.sort_values('d_name_len', ascending=False).head(1)
    with open('top_data_len.csv', 'w') as f:
        f.write(top_data_len.to_csv(index=False, header=False))

        
def get_stat_air():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_air = top_data_df[top_data_df['domain'] == 'airflow.com'] 
    with open('top_data_air.csv', 'w') as f:
        f.write(top_data_air.to_csv(index=False, header=False))

        
        
def print_data(ds):  # передаем глобальную переменную airflow
    with open('top_data_10_domain.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_len.csv', 'r') as f:
        all_data_com = f.read()
    with open('top_data_air.csv', 'r') as f:
        all_data_com = f.read()
    date = ds

    print(f'Top 10 unique data {date}')
    print(all_data)

    print(f'data with large len {date}')
    print(all_data_com)
    
    print(f'Is "Airflow.com" in data {date}')
    print(all_data_com)
# Инициализируем DAG


default_args = {
    'owner': 'i-zaharov-26', # Владелец операции
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2022, 10, 15), # Дата начала выполнения DAG
    'schedule_interval': '0 12 * * *' # cron выражение, также можно использовать '@daily', '@weekly'
    #'schedule_interval': '@daily' переменные airflow
    #'schedule_interval': timedelta() параметр timedelta
    # default_args = {
    #     'owner': 'your_name', # Владелец операции
    #     'depends_on_past': False, # Зависимость от прошлых запусков
    #
    #     'schedule_interval': '0 12 * * *' # cron выражение, также можно использовать '@daily', '@weekly'
    #     #'schedule_interval': '@daily' переменные airflow
    #     #'schedule_interval': timedelta() параметр timedelta
    #
    #     'retries': 1, # Кол-во попыток выполнить DAG
    #     'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    #
    #     'email': '', # Почта для уведомлений
    #     'email_on_failure': '', # Почта для уведомлений при ошибке
    #     'email_on_retry': '', # Почта для уведомлений при перезапуске
    #
    #     'retry_exponential_backoff': '', # Для установления экспоненциального времени между перезапусками
    #     'max_retry_delay': '', # Максимальный промежуток времени для перезапуска
    #
    #     'end_date': '', # Дата завершения выполнения DAG
    #
    #     'on_failure_callback': '', # Запустить функцию если DAG упал
    #     'on_success_callback': '', # Запустить функцию если DAG выполнился
    #     'on_retry_callback': '', # Запустить функцию если DAG ушел на повторный запуск
    #     'on_execute_callback': '', # Запустить функцию если DAG начал выполняться
    #      # Задать документацию
    #     'doc': '',
    #     'doc_md': '',
    #     'doc_rst': '',
    #     'doc_json': '',
    #     'doc_yaml': ''

}
dag = DAG('i-zaharov-26-lesson_2', default_args=default_args)

# Инициализируем таски
t1 = PythonOperator(task_id='get_data', # Название таска
                    python_callable=get_data, # Название функции
                    dag=dag) # Параметры DAG

t2 = PythonOperator(task_id='get_stat_domain',
                    python_callable=get_stat_domain,
                    dag=dag)

t2_len = PythonOperator(task_id='get_stat_len',
                        python_callable=get_stat_len,
                        dag=dag)

t2_air = PythonOperator(task_id='get_stat_air',
                        python_callable=get_stat_air,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

# Задаем порядок выполнения
t1 >> [t2, t2_len, t2_air] >> t3

# Методы таска
# t1.set_downstream(t2)
# t2.set_downstream(t3)

# Для параллельного выполнения тасков ипользуется структура Python операторов в виде
#  A >> [B, C] >> D
# или  прописываются зависимости через методы таска
# A.set_downstream(B)
# A.set_downstream(C)
# B.set_downstream(D)
# C.set_downstream(D)
