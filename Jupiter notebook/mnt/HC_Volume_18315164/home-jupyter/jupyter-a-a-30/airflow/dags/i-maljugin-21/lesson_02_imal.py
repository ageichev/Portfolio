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


def top_10_zone(): # топ-10 доменных зон по численности доменов
    top_10_zones = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']) 
    top_10_zones = top_10_zones.domain.str.split('.', expand = True)[[0, 1]].rename(columns = {0 :'Domain', 1 :'Zone' })
    top_10_zones = top_10_zones.Zone.value_counts().head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))


def longest_name(): # домен с самым длинным именем(last_one)
    longest_name_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_name_domain['Len'] = longest_name_domain['domain'].apply(lambda x: len(x))
    longest_name_domain = longest_name_domain.sort_values('Len', ascending=False).head(1)['domain']
    with open('longest_name_domain.csv', 'w') as f:
        f.write(longest_name_domain.to_csv(index=False, header=False))
     
    
def where_air():  # На каком месте находится домен airflow.com? 
    where_air = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

    if where_air.query( 'domain == "airflow.com" ' ).shape[0] == 0:
        answer = 'None'
        answer = pd.Series(answer)
    else:
        answer = where_air.query( 'domain == "airflow.com" ' )
        
    with open('answer.csv', 'w') as f:
        f.write(answer.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10 = f.read()
    with open('longest_name_domain.csv', 'r') as f:
        longest_name = f.read()
    with open('answer.csv', 'r') as f:
        answer = f.read()
    date = ds
    

    print(f'Топ 10 доменных зон {date}')
    print(top_10)

    print(f'Домен с самым длинным именем {date}')
    print(longest_name)
              
    print(f'На каком месте airflow.com {date}')
    print(answer)


default_args = {
    'owner': 'i-maljugin-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 3),
    'start_date': datetime(2022, 6, 23),
}
schedule_interval = '11 21 */1 * *'

dag = DAG('imal_first_dag', default_args=default_args, schedule_interval=schedule_interval)
              
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_zone',
                    python_callable=top_10_zone,
                    dag=dag)

t3 = PythonOperator(task_id='longest_name',
                        python_callable=longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='where_air',
                    python_callable=where_air,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)