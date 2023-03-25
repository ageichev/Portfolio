import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    '''
    Функция читает данные по ссылке TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
    и сохраняет в файл TOP_1M_DOMAINS_FILE = 'top-1m.csv'
    Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    '''
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
    f.close()

def get_top_10_domains():
    '''
    Функция находит топ-10 доменных зон по численности доменов
    '''
    top_doms = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'website'])
    top_doms[['name', 'domain']] = top_doms.website.str.split('.', expand=True, n=1)
    top_10_domains = top_doms \
        .groupby('domain', as_index=False) \
        .agg({'rank': 'count'}) \
        .sort_values('rank', ascending=False) \
        .rename(columns={'rank': 'number'}) \
        .head(10)
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))
    f.close()

def longest_domain_name():
    '''
    Функция находит домен с самым длинным именем (если их несколько,
    то взять только первый в алфавитном порядке)
    '''
    top_doms = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'website'])
    top_doms['name_length'] = top_doms.website.str.len()
    value = top_doms.sort_values('name_length', ascending=False).head(1).website
    longest_domain_name = value.get(value.keys()[0])
    with open('the_longest_domain.txt', 'w') as f:
        f.write(longest_domain_name)
    f.close()

def airflow_position():
    '''
    Функция должна показывать на каком месте находится домен airflow.com
    Однако в списке нет airflow.com
    Поэтому функция возвращает все сайты, которые начинаются с 'airflow'
    '''
    top_doms = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'website'])
    df_airflow = top_doms[top_doms.website.str.startswith('airflow')]
    with open('websites_airflow.csv', 'w') as f:
        f.write(df_airflow.to_csv(index=False, header=False))
    f.close()

def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        top_10_domains_output = f.read()
    with open('the_longest_domain.txt', 'r') as f:
        the_longest_domain_output = f.read()
    with open('websites_airflow.csv', 'r') as f:
        websites_airflow_output = f.read()
    date = ds
    print(f'Top 10 domains for date {date}')
    print(top_10_domains_output)
    print(f'The longest domain name for date {date}')
    print(the_longest_domain_output)
    print(f'The list of domains which contain "airflow" {date}')
    print(websites_airflow_output)
    f.close()


default_args = {
    'owner': 'v.khudokormov',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 29),
}
schedule_interval = '0 11 */1 * *'

dag = DAG('v_hudokormov_lesson_2_hw_dag'
          , default_args=default_args
          , schedule_interval=schedule_interval
          )

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domains',
                    python_callable=get_top_10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain_name',
                        python_callable=longest_domain_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_position',
                    python_callable=airflow_position,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
