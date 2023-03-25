import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

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


# Найти топ-10 доменных зон по численности доменов
def top_domain_zones():
    #     Считаем датафрейм
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    #     Сплитуем и забираем только последнюю часть
    df['domain_zone'] = df['domain'].apply(lambda x: x.split('.')[-1])

    # Группируем по зонам, сортируем по убыванию, берем 10
    top_10 = df.groupby('domain_zone', as_index=False) \
        .agg({'rank': 'count'}) \
        .rename(columns={'rank': 'quantity'}) \
        .sort_values('quantity', ascending=False) \
        .reset_index(drop=True) \
        .head(10)

    #     Пишем в файл csv file
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10.to_csv(index=False, header=False))


# Домены с самыми длинными именами
def max_name_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_length'] = df['domain'].apply(lambda x: len(x))  # Длина имени

    # Сортируем по длине домена (убывание) и по алфавиту
    max_name_domain = df.sort_values(['domain_length', 'domain'], ascending=[False, True]).iloc[0].domain

    # Пишем в текстовый файл
    with open('max_name_domain.txt', 'w') as f:
        f.write(str(max_name_domain))


# сайт "airflow" место
def rank_of_airflow():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    my_domain = 'airflow.com'

    #     print(df.query('domain == "airflow.com"'))

    if my_domain in df['domain'].values:
        airflow_rank = int(df.query('domain == "airflow.com"')['rank'])
    else:
        airflow_rank = "Домена airflow.com нет в списке"

    with open('airflow_rank.txt', 'w') as f:
        f.write(str(airflow_rank))


# Печатаем
def print_result():
    with open('top_10_domain_zones.csv', 'r') as file:
        print(f"Топ-10 доменных зон по численности доменов: \n{file.read()}")
    with open('max_name_domain.txt', 'r') as file:
        print(f"Домен с самым длинным именем: {file.read()}")
    with open('airflow_rank.txt', 'r') as file:
        print(f"Ранг домена airflow_rank.txt: {file.read()}")


default_args = {
    'owner': 'o_malashenko_19',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 4, 24),
    'schedule_interval': '00 * * * *'
}

dag = DAG('o_malashenko_19_lesson_2', default_args=default_args)

t1_read = PythonOperator(task_id='get_data',
                         python_callable=get_data,
                         dag=dag)

t2_top_10 = PythonOperator(task_id='top_domain_zones',
                           python_callable=top_domain_zones,
                           dag=dag)

t2_max_name = PythonOperator(task_id='max_name_domain',
                             python_callable=max_name_domain,
                             dag=dag)

t2_airflow = PythonOperator(task_id='rank_of_airflow',
                            python_callable=rank_of_airflow,
                            dag=dag)

t3_print = PythonOperator(task_id='print_result',
                          python_callable=print_result,
                          dag=dag)

t1 >> [t2_top_10, t2_max_name, t2_airflow] >> t3_print