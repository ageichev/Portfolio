'''
Необходимо скопировать DAG из лекции - к себе

Необходимо выполнить:

1) Поменять имена dag на уникальные (лучше всего как-то использовать свой логин).

Поставить новую дату начала DAG и новый интервал (все еще должен быть ежедневным)

2) Удалить таски get_stat и get_stat_com. Вместо них сделать свои собственные, которые
считают следующие:

Найти топ-10 доменных зон по численности доменов
Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
На каком месте находится домен airflow.com?

3) Финальный таск должен писать в лог результат ответы на вопросы выше

Если нужна помощь, пишите сюда
'''


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


'''
2) Удалить таски get_stat и get_stat_com. Вместо них сделать свои собственные, которые
считают следующие:

Найти топ-10 доменных зон по численности доменов
Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
На каком месте находится домен airflow.com?
'''


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_count():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_data_df_count = top_data_df.groupby('domain_zone', as_index= False)\
                        .agg({'domain':'count'})\
                        .rename(columns = {'domain':'domain_count'})\
                        .sort_values('domain_count', ascending = False)

    top_data_df_count = top_data_df_count.head(10)


    with open('top_data_df_count.csv', 'w') as f:
        f.write(top_data_df_count.to_csv(index=False, header=False))


def get_longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name'] = top_data_df['domain'].apply(lambda x: x.split('.')[0])
    top_data_df['len_domain_name'] = top_data_df['domain_name'].apply(len)

    df_longest_names = top_data_df.query('len_domain_name == len_domain_name.max()').sort_values(by = 'domain_name')
    longest_name = df_longest_names['domain_name'].values[0]


    with open('longest_name.txt', 'w') as f:
        f.write(longest_name)


def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain = 'airflow.com'
    if len(top_data_df.query("domain == @domain")) == 0:
        place = 'airflow.com не попал в список доменов'
    else:
        place = top_data_df.query("domain == @domain")['rank'].values[0]

    with open('airflow_place.txt', 'w') as f:
        f.write(place)


def print_data(ds):
    with open('top_data_df_count.csv', 'r') as f:
        top_data_df_count = f.read()
    with open('longest_name.txt', 'r') as f:
        longest_name = f.read()
    with open('airflow_place.txt', 'r') as f:
        airflow_place = f.read()

    date = ds

    print(f'Top biggest domains zones for date {date}')
    print(top_data_df_count)

    print(f'Longest domain name date {date}')
    print(longest_name)

    print(f'airflow.com rank for date {date}')
    print(airflow_place)

'''
1) Поменять имена dag на уникальные (лучше всего как-то использовать свой логин).
Поставить новую дату начала DAG и новый интервал (все еще должен быть ежедневным)
'''


default_args = {
    'owner': 'd.feklistov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 30),
}
schedule_interval = '30 7 * * *'

dag = DAG('d-feklistov-22', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_count',
                    python_callable=get_count,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
