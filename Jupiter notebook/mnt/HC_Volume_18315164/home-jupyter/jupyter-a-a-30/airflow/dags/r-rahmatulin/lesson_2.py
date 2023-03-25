import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


#___________________


def get_data_rrahmatilin():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


#___________________

        
def get_top10_domain_rrahmatulin():
    #Найти топ-10 доменных зон по численности доменов
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top10_domain_rrahmatulin = top_data_df.domain.apply(lambda x: x.split('.')[-1]) \
                                .value_counts().head(10).reset_index() \
                                .rename(columns={'index':'domain', 'domain':'value'})
    
    with open('top10_domain_rrahmatulin.csv', 'w') as f:
        f.write(top10_domain_rrahmatulin.to_csv(index=False, header=False))
        


#___________________

        
          
def get_max_long_rrahmatulin():
    # Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    max_long_rrahmatulin = top_data_df.copy()
    max_long_rrahmatulin['len'] = max_long_rrahmatulin.domain.str.len()
    max_long_rrahmatulin = max_long_rrahmatulin.sort_values('len', ascending=False).head(1).domain.to_list()[0]
    
    with open('max_long_rrahmatulin.txt', 'w') as f:
        f.write(str(max_long_rrahmatulin))

        
#___________________  


def get_rank_airflow_rrahmatulin():
    #На каком месте находится домен airflow.com?
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    rank_airflow_rrahmatulin = top_data_df[top_data_df.domain == 'airflow.com']['rank'].iloc[0]
    if rank_airflow_rrahmatulin > 0:
        print(rank_airflow_rrahmatulin)
    else:
        print('Данного домена нет в списке')
    
    with open('rank_airflow_rrahmatulin.txt', 'w') as f:
        f.write(str(rank_airflow_rrahmatulin))
        

#___________________   
        
                
        
def print_data_rrahmatulin(ds):
    # передаем глобальную переменную airflow
    with open('top10_domain_rrahmatulin.csv', 'r') as f:
        top10_domain_rrahmatulin = f.read()
    with open('max_long_rrahmatulin.txt', 'r') as f:
        max_long_rrahmatulin = f.read()
    with open('rank_airflow_rrahmatulin.txt', 'r') as f:
        rank_airflow_rrahmatulin = f.read()
        
    date = ds

    print(f'Top 10 domains by quantity for date {date}')
    print(top10_domain_rrahmatulin)

    print(f'Longest domain for date {date}')
    print(max_long_rrahmatulin)

    print(f'airflow.com position for date {date}')
    print(rank_airflow_rrahmatulin) 


#___________________
   
    
default_args = {
    'owner': 'r.rahmatulin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 23),
}
schedule_interval = '0 9 * * *'


#___________________


dag = DAG('r-rahmatulin',
          default_args=default_args,
          schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data_rrahmatilin,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_domains',
                    python_callable=get_top10_domain_rrahmatulin,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_long',
                    python_callable=get_max_long_rrahmatulin,
                    dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow',
                    python_callable=get_rank_airflow_rrahmatulin,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data_rrahmatulin,
                    provide_context=True,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5

####
