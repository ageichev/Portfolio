import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

#исходные данные
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

#получение данных
def get_data():

    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# топ10 доменных зон
def get_top10_domzone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top10_domzone = top_data_df.domain.str.split(".").str[-1].value_counts().head(10).reset_index()['index']
    with open('top10_domzone.csv', 'w') as f:
        f.write(top10_domzone.to_csv(index=False, header=False))
# самое длинное имя
def get_lname():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    maxlen = top_data_df.domain.str.len().max()
    maxlendom = top_data_df[top_data_df.domain.str.len() == maxlen].sort_values(by=['domain']).head(1).reset_index().domain
    with open('maxlendom.csv', 'w') as f:
        f.write(maxlendom.to_csv(index=False, header=False))
  
# позиция Airflow 
def get_position():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_dom = top_data_df.query('domain == "airflow.com"').count().iloc[0]
    if df_dom > 0:
        rank_dom = top_data_df.query('domain == "airflow.com"')['rank'].iloc[0]
    else:
        rank_dom = 'airflow.com нет в списке'
    
    with open('position_af.csv', 'w') as f:
        f.write(rank_dom)
        
# вывод результатов
def output_info(ds):
    with open('top10_domzone.csv', 'r') as f:
        top_10_z = f.read()
    with open('maxlendom.csv', 'r') as f:
        lname = f.read()
    with open('position_af.csv', 'r') as f:
        position = f.read()
    date = ds

    print(f'ТОП 10 доменных зон на {date}')
    print(top_10_z)
    
    print(f'Максимально длинное имя на {date}')
    print(lname)
    
    print(f'Позиция сайта Airflow на {date} это')
    print(position)



default_args = {
    'owner': 'g-zigora-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 16),
}
schedule_interval = '0 10 * * *'

dag = DAG('g-zigora-23', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top10 = PythonOperator(task_id='get_top10_domzone',
                    python_callable=get_top10_domzone,
                    dag=dag)
                       
t2_lname = PythonOperator(task_id='get_lname',
                    python_callable=get_lname,
                    dag=dag)

t2_pos_af = PythonOperator(task_id='get_position',
                    python_callable=get_position,
                    dag=dag)


t3 = PythonOperator(task_id='output_info',
                    python_callable=output_info,
                    dag=dag)

t1 >> [t2_top10, t2_lname, t2_pos_af] >> t3


