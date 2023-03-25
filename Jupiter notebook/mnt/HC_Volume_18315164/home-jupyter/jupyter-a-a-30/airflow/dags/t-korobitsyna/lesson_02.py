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


#def get_stat():
    #top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    #top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
    #top_data_top_10 = top_data_top_10.head(10)
   #with open('top_data_top_10.csv', 'w') as f:
        #f.write(top_data_top_10.to_csv(index=False, header=False))


def get_top_10_domians_tkorobitsyna():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domians = top_data_df.domain.str.split(".").str[-1].value_counts().reset_index()['index']
    top_10_domians_tkorobitsyna = top_10_domians.head(10)
    with open('top_10_domians_tkorobitsyna.csv', 'w') as f:
        f.write(top_10_domians_tkorobitsyna.to_csv(index=False, header=False))


#def print_data(ds):
    #with open('top_data_top_10.csv', 'r') as f:
        #all_data = f.read()
    #with open('top_data_top_10_com.csv', 'r') as f:
        #all_data_com = f.read()
    #date = ds

    #print(f'Top domains in .RU for date {date}')
    #print(all_data)

    #print(f'Top domains in .COM for date {date}')
    #print(all_data_com)

def get_longest_domian_tkorobitsyna():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    maximum_length = top_data_df.domain.str.len().max()
    longest_domian_tkorobitsyna = top_data_df[top_data_df.domain.str.len() == maximum_length].sort_values(by=['domain']).head(1).reset_index().domain
    with open('longest_domian_tkorobitsyna.csv', 'w') as f:
        f.write(longest_domian_tkorobitsyna.to_csv(index=False, header=False))
        
        
def get_position_airflow_tkorobitsyna():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    position_airflow_tkorobitsyna = top_data_df.query('domain == "airflow.com"').count().iloc[0]
    if position_airflow_tkorobitsyna > 0:
        rank_domian = top_data_df.query('domain == "airflow.com"')['rank'].iloc[0]
    else:
        rank_domian = 'airflow.com нет в списке'
    
    with open('position_airflow_tkorobitsyna.csv', 'w') as f:
        f.write(rank_domian)
        
        
def print_results(ds):
    with open('top_10_domians_tkorobitsyna.csv', 'r') as f:
        top_10_dom = f.read()
    with open('longest_domian_tkorobitsyna.csv', 'r') as f:
        longest_domian = f.read()
    with open('position_airflow_tkorobitsyna.csv', 'r') as f:
        position = f.read()
    date = ds

    print(f'Top 10 domians by number of domains for date {date}')
    print(top_10_dom)
    
    print(f'Domian with the longest name for date {date}')
    print(longest_domian)
    
    print(f'The airflow.com domain is located for date {date} in')
    print(position)
    
    
default_args = {
    'owner': 't-korobitsyna',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 28),
}
schedule_interval = '0 11 * * *'

dag = DAG('t-korobitsyna', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domians_tkorobitsyna',
                    python_callable=get_top_10_domians_tkorobitsyna,
                    dag=dag)

t2_lname = PythonOperator(task_id='get_longest_domian_tkorobitsyna',
                        python_callable=get_longest_domian_tkorobitsyna,
                        dag=dag)

t2_af_pos = PythonOperator(task_id='get_position_airflow_tkorobitsyna',
                        python_callable=get_position_airflow_tkorobitsyna,
                        dag=dag)

t3 = PythonOperator(task_id='print_results',
                    python_callable=print_results,
                    dag=dag)

t1 >> [t2, t2_lname, t2_af_pos] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_lname)
#t1.set_downstream(t2_af_pos)
#t2.set_downstream(t3)
#t2_lname.set_downstream(t3)
#t2_af_pos.set_downstream(t3)