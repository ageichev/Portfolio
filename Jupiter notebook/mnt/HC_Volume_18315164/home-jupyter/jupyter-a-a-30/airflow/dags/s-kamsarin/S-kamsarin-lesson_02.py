import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
# задаем откуда берем данные и куда запишем(чтоб в дальнейшем обработать)
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 's_kamisar_lesson_2_top-1m.csv'

# Читаем данные и сохраняем в файл
def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# Найти топ-10 доменных зон по численности доменов
def get_stat_1():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_count_domain_zone = (top_data_df['domain'].apply(lambda x: x.split('.')[-1])
                              .value_counts().to_frame().reset_index().head(10))
    with open('top_10_count_domain_zone.csv', 'w') as f:
        f.write(top_10_count_domain_zone.to_csv(index=False, header=False)) #index=False, header=False ?? так принято?

# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_stat_2():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df['domain'].apply(lambda x: len(x))
    max_length_domain_str = (top_data_df.sort_values(['domain_len', 'domain'], 
                            ascending=[False, True]).reset_index().domain[0])
    with open('max_length_domain_str.txt', 'w') as f:
        f.write(str(max_length_domain_str))
        
#На каком месте находится домен airflow.com?
def get_stat_3():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    try:
        place_airflow = int(top_data_df.query('domain == "airflow.com"')['rank'])
    except:
        place_airflow = 'is Unknown'                  
    with open('place_airflow.txt', 'w') as f:
        f.write(str(place_airflow))        


def print_data(ds):
    with open('top_10_count_domain_zone.csv', 'r') as f:
        top_10_count_domain_zone = f.read()    
    with open('max_length_domain_str.txt', 'r') as f:
        max_length_domain_str = f.read()
    with open('place_airflow.txt', 'r') as f:
        place_airflow = f.read()
    date = ds

    print(f'top 10 domain zones by number for date {date}')
    print(top_10_count_domain_zone)
    
    print(f'the domain with the longest name for date {date} is :')
    print(max_length_domain_str)
    print()
    print(f'airflow.com domain for date {date} is in place No. ', end='')
    print(place_airflow)


default_args = {
    'owner': 's.kamsarin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 1),
}
schedule_interval = '0 12 * * *'

dag = DAG('s_kamisar_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_stat_1',
                    python_callable=get_stat_1,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_stat_2',
                        python_callable=get_stat_2,
                        dag=dag)

t2_3 = PythonOperator(task_id='get_stat_3',
                        python_callable=get_stat_3,
                        dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2, t2_3] >> t3

