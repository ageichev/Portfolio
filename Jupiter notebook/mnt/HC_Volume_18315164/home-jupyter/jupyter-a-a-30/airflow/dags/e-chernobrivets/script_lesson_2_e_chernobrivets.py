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
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# Найти топ-10 доменных зон по численности доменов
def get_top_10_zones():
    table = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    table['zone'] = table.domain.str.split('\.')
    table['zone'] =  table['zone'].apply(lambda x : x[-1])
    top_zones = table.groupby('zone', as_index=False).domain.count()
    top_zones = top_zones.sort_values('domain', ascending=False).head(10)
    with open('echern_top_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False))


# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_the_longest_domain():
    table = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    table['domain_len'] = table['domain'].apply(lambda x: len(x))
    the_longest_domain = table.sort_values(['domain_len', 'domain'], ascending=[False, True]).reset_index().domain[0]                           
    with open('echern_the_longest_domain.csv', 'w') as f:
        f.write(the_longest_domain)
        

# На каком месте находится домен airflow.com?
def get_airflow():
    top_data_aifrlow = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_aifrlow = top_data_aifrlow[top_data_aifrlow.domain == 'airflow.com']
    if top_data_aifrlow.shape[0] == 0:
        top_data_aifrlow = pd.DataFrame()
        top_data_aifrlow['info'] = pd.Series(['No airflow.com'])
        with open('echern_top_data_aifrlow.csv', 'w') as f:
            f.write(top_data_aifrlow.to_csv(index=False, header=False))
    else:
        with open('echern_top_data_aifrlow.csv', 'w') as f:
            f.write(top_data_aifrlow.to_csv(index=False, header=False))     
            
            

def print_data(ds):
    with open('echern_top_zones.csv', 'r') as f:
        top_zones = f.read()
    with open('echern_the_longest_domain.csv', 'r') as f:
        the_longest_domain = f.read()
    with open('echern_top_data_aifrlow.csv', 'r') as f:
        airflow_position = f.read()    
           
    date = ds

    print(f'Top domain zones for date {date}')
    print(top_zones)

    print(f'The longest domain for date {date}')
    print(the_longest_domain)
    
    print(f'Airflow.com position for date {date}')
    print(airflow_position)


default_args = {
    'owner': 'e.chernobrivets',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 25),
}
schedule_interval = '0 10 * * *'

dag = DAG('e.chernobrivets_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_top_10_zones',
                    python_callable=get_top_10_zones,
                    dag=dag)

t2_len = PythonOperator(task_id='get_the_longest_domain',
                        python_callable=get_the_longest_domain,
                        dag=dag)

t2_air = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_len, t2_air] >> t3


#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
