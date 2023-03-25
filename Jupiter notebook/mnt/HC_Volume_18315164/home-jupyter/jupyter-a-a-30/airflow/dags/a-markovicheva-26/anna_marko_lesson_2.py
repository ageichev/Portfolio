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




def get_top_10_domain_zones():
    """
    Найти топ-10 доменных зон по численности доменов.
    """
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.', 1)[1])
    top_10_domain_zones = top_data_df.groupby('domain_zone',as_index=False).agg({'domain': 'nunique'}).sort_values('domain', ascending=False).head(10)
    with open('top_10_dom_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))




def get_the_longest_domain():
    """
    Найти домен с самым длинным именем
    (если их несколько, то взять только первый в алфавитном порядке)
    
    """
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name_len'] = top_data_df['domain'].apply(lambda x: len(x))
    max_len = top_data_df['domain_name_len'].max()
    the_longest_domain = top_data_df[top_data_df.domain_name_len == max_len].domain.sort_values()
    with open('the_longest_domain.csv', 'w') as f:
        f.write(the_longest_domain.to_csv(index=False, header=False))


def get_airflow_com_rank():
    """
    Позиция домена airflow.com.
    """
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank = top_data_df[top_data_df.domain == 'airflow.com']
    if rank.empty:
        rank = 'not mentioned'
    else:
        rank = rank.iloc[0, 0]
    res = pd.DataFrame(data={'name': 'airflow.com', 'rank': rank}, index=[0])
    with open('airflow_rank.csv', 'w') as f:
        f.write(res.to_csv(index=False))



def print_data(ds):
    with open('top_10_dom_zones.csv', 'r') as f:
        top_10_dom_zones = f.read()
    with open('the_longest_domain.csv', 'r') as f:
        the_longest_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    
    date = ds

    print(f'Top-10 domain zones for date {date}')
    print(top_10_dom_zones)

    print(f'The longest domain for date {date}')
    print(the_longest_domain)
    
    print(f'The airflow\'s rank for date {date}')
    print(airflow_rank)
    
    



default_args = {
    'owner': 'a.markovicheva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 15),
}



schedule_interval = '0 10 * * *'



dag = DAG('anna_mar_26_lesson_2', default_args=default_args, schedule_interval=schedule_interval)




t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_10 = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=dag)

t2_long = PythonOperator(task_id='get_the_longest_domain',
                        python_callable=get_the_longest_domain,
                        dag=dag)

t2_air = PythonOperator(task_id='get_airflow_com_rank',
                        python_callable=get_airflow_com_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)



t1 >> [t2_10, t2_long, t2_air] >> t3

