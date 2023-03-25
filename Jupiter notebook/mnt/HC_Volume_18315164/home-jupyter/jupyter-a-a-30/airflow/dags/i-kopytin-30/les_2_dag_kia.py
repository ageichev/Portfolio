import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index = False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
# 1
# Найти топ-10 доменных зон по численности доменов
def get_top_10_dom_zones():
    '''
    Собственно, находит топ-10 доменных зон по численности доменов
    и записывает зоны в файл в формате '.<<zone>>'
    '''
    
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'], index_col = 0)
    
    df['zone'] = df.domain.apply(lambda s: '.' + s.split('.')[-1])
    
    top_10_doms = df.zone.value_counts().sort_values(ascending = False).head(10).index.values
    with open('top_10_dom.txt', 'w') as f:
        f.write('\n'.join(top_10_doms))

# 2
# Найти домен с самым длинным именем 
# если их несколько, то взять только первый в алфавитном порядке
def get_longest_dom():
    '''
    Находит домен с самым длинным именем
        (в случае если их несколько, то только первый в алфавитном порядке)
    и записывает домен в файл
    '''
    
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'], index_col = 0)
    
    first_longest_dom = df.domain.loc[df.domain.sort_values().apply(lambda s: len(s)).idxmax()]
    #
    # len('.'.join(s.split('.')[:-1])) если нужно без главного уровня
    #
    with open('the_longest_dom.txt', 'w') as f:
        f.write(first_longest_dom)

# 3 На каком месте находится домен airflow.com?
def get_airflow_com_rank():
    '''
    Находит текущий (?) ранг 'airflow.com' и записывает его в файл
        (если 'airflow.com' по какой-то причине не попал в топ-1М, записывает соответствующее сообщение)
    '''
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'], index_col = 0)
    
    try:
        airflow_com_rank = df[df.domain == 'airflow.com'].index[0]
    except IndexError:
        airflow_com_rank = '> 1M'
        
    with open('airflow_com_rank.txt', 'w') as f:
        f.write(str(airflow_com_rank))


def print_data(ds):
    
    with open('top_10_dom.txt', 'r') as f:
        top_10_dom = f.read()
    with open('the_longest_dom.txt', 'r') as f:
        the_longest_dom = f.read()
    with open('airflow_com_rank.txt', 'r') as f:
        airflow_com_rank = f.read()
        
    date = ds

    print(f'Top 10 domain zones for date {date}:')
    print(top_10_dom)

    print(f'\nThe longest domain among top-1M for date {date}:')
    print(the_longest_dom)
    
    print(f'\nThe rank of airflow.com for date {date}:')
    print(airflow_com_rank)


default_args = {
    'owner': 'i-kopytin-30',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 2),
    'start_date': datetime(2023, 2, 16),
                }

schedule_interval = '08 8 * * *'

dag = DAG('i-kopytin-30', default_args = default_args, schedule_interval = schedule_interval)

t1 = PythonOperator(task_id = 'get_data',
                    python_callable = get_data,
                    dag = dag)

t2_1 = PythonOperator(task_id = 'get_top_10_dom_zones',
                    python_callable = get_top_10_dom_zones,
                    dag = dag)

t2_2 = PythonOperator(task_id = 'get_longest_dom',
                    python_callable = get_longest_dom,
                    dag = dag)

t2_3 = PythonOperator(task_id = 'get_airflow_com_rank',
                    python_callable = get_airflow_com_rank,
                    dag = dag)

t3 = PythonOperator(task_id = 'print_data',
                    python_callable = print_data,
                    dag = dag)

t1 >> [t2_1, t2_2, t2_3] >> t3
#
#t1.set_downstream(t2_1)
#t1.set_downstream(t2_2)
#t1.set_downstream(t2_3)
#
#t2_1.set_downstream(t3)
#t2_2.set_downstream(t3)
#t2_3.set_downstream(t3)
