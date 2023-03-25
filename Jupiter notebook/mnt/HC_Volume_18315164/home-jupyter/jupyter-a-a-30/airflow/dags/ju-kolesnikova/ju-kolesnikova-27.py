import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
import csv
from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df.domain = top_data_df.domain.astype(str)
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    top_zone = top_data_df.groupby('zone', as_index = False).agg({'domain':'count'})\
    .rename(columns = {'domain':'quant'}).sort_values('quant', ascending = False).head(10)
    with open('top_zone.csv', 'w') as f:
        f.write(top_zone.to_csv(index=False, header=False))


def longest():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df.domain = top_data_df.domain.astype(str)
    longest = sorted(top_data_df.domain.to_list(), key = lambda x: (len(x), x), reverse = True)[0]
    with open('longest.txt', 'w') as file:
        print(str(f'{longest}'), file = file)


def position():
    with open('position_airflow.txt', 'w') as file:
        try:
            position_airflow = top_data_df.query('domain == "airflow.com"').iloc[0]['rank']
            print(str(f'Домен "airflow.com" находится на {position_airflow} месте.'), file = file)
        except:
            print(str(f'Домен "airflow.com" не входит в Топ 1М.'), file = file)
            
            
def print_data(ds):
    with open('top_zone.csv', 'r') as f:
        all_data = list(csv.reader(f))
    with open('longest.txt', 'r') as f:
        longest = f.read()
    with open('position_airflow.txt', 'r') as f:
        position_airflow = f.read()
    date = ds

    print(f'Топ доменных зон на дату {date}:')
    for row in all_data:
        print(*row)
    print()
    print(f'Домен с самым длинным именем на дату {date}: {longest}')
    print(f'Позиция домена "airflow.com" на дату {date}:')
    print(f'{position_airflow}')


default_args = {
    'owner': 'yu.kolesnikova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 19),
}
schedule_interval = '0 12 * * *'

dag = DAG('lesson_2_yu_koles', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zone',
                    python_callable=get_zone,
                    dag=dag)

t2_longest = PythonOperator(task_id='longest',
                        python_callable=longest,
                        dag=dag)
t_2_position = PythonOperator(task_id='position',
                        python_callable=position,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_longest, t_2_position] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
