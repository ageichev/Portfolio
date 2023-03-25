import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def ralykov_get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        f.close()

#def get_stat():
#    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
#    top_data_top_10 = top_data_top_10.head(10)
#    with open('top_data_top_10.csv', 'w') as f:
#        f.write(top_data_top_10.to_csv(index=False, header=False))


#def get_stat_com():
#    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
#    top_data_top_10 = top_data_top_10.head(10)
#    with open('top_data_top_10_com.csv', 'w') as f:
#        f.write(top_data_top_10.to_csv(index=False, header=False))


# Найти топ-10 доменных зон по численности доменов
def ralykov_top_10():
    top_10_df = pd.read_csv(TOP_1M_DOMAINS, names = ['rank', 'domain'])
    top_10_df = top_10_df.domain.apply(lambda x: x.split('.')[-1])
    top_10_df = top_10_df.value_counts().head(10).reset_index()
    with open('ralykov_top_10.csv', 'w') as f:
        f.write(top_10_df.to_csv(index=False, header=False))
        f.close()
        
# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def ralykov_longest():
    df_longest = pd.read_csv(TOP_1M_DOMAINS, names = ['rank', 'domain'])
    df_longest = df_longest[df_longest.domain.str.len() == df_longest.domain.str.len().max()]
    
    with open('ralykov_longest_domain.csv', 'w') as f:
        f.write(df_longest.to_csv(index=False, header=False))
        f.close()

# На каком месте находится домен airflow.com?
def ralykov_airflow_place():
    df_airflow_place = pd.read_csv(TOP_1M_DOMAINS, names = ['rank', 'domain'])
    df_airflow_place = df_airflow_place.query('domain == "airflow.com"')
    df_airflow_place = 'this domain does not exist' if df_airflow_place.empty else str(df_airflow_place['rank'].index[0]) # str что бы записался в txt
    
    with open('ralykov_airflow_place.txt', 'w') as f:
        f.write(df_airflow_place)
        f.close()
        

    
def print_data(ds):
    with open('ralykov_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('ralykov_longest_domain.csv', 'r') as f:
        all_data_longest = f.read()
    with open('ralykov_airflow_place.txt', 'r') as f:
        all_data_airflow_place = f.read()
    date = ds

    print(f'Top domains count for date {date}')
    print(all_data)

    print(f'longest domain for date {date} is')
    print(all_data_longest)
    
    print(f'airflow.com domain rank place for date {date} is')
    print(all_data_airflow_place)


default_args = {
    'owner': 'r.alykov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 3),
}
schedule_interval = '*/5 * */1 * *'

dag = DAG('ralykov_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='ralykov_get_data',
                    python_callable=ralykov_get_data,
                    dag=dag)

t2 = PythonOperator(task_id='ralykov_top_10',
                    python_callable=ralykov_top_10,
                    dag=dag)

t2_longest = PythonOperator(task_id='ralykov_longest',
                        python_callable=ralykov_longest,
                        dag=dag)

t2_airflow_place = PythonOperator(task_id='ralykov_airflow_place',
                        python_callable=ralykov_airflow_place,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_longest, t2_airflow_place] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)