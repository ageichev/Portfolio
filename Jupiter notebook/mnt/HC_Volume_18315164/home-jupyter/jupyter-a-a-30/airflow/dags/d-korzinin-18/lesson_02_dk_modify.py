import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# Запись нужных данных в отдельный файл
def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# топ 10 доменных зон по численности доменов
def get_top10_analysis():
    #создаем датафрейм
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    #определяем доменные зоны
    df2 = df.domain.str.rsplit('.',1)
    df_spl2 = pd.DataFrame(df2.tolist(), index = df.index, columns = ['dom','dom_zone'])
    #группируем по доменным зонам
    df_spl2 = df_spl2.groupby('dom_zone', as_index = False) \
        .agg({'dom': 'count'}) \
        .sort_values(by = 'dom', ascending = False) \
        .reset_index(drop = True) \
        .rename(columns = {'dom': 'count'}) \
        .head(10)
    with open('top10_dom_zones.csv', 'w') as f:
        f.write(df_spl2.to_csv(index=False, header=False))
    
# находим домен с самым длинным именем и позицию домена airflow.com
def get_domain_longest_name():
    #создаем датафрейм
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    #добавляем колонку с количеством символов
    df['count_symb'] = df['domain'].str.len()
    #сортируем по количеству символов и по имени домена
    df_name_len = df.sort_values(by = ['count_symb','domain'], ascending = False) \
        .reset_index(drop = True)
    longest_name = df_name_len.domain[0]
    with open('longest_name.txt', 'w') as f:
        f.write(longest_name)
    
    #определяем позицию домена airflow.com
    indexes = [index for index in range(len(df_name_len.domain)) if df_name_len.domain[index] == "airflow.com"]
    try:
        if indexes[0] >= 0:
            airflow_position = indexes[0]
    except IndexError:
        airflow_position = 'not contains'
    with open('airflow_position.txt', 'w') as f:
        f.write(airflow_position)
        
#записываем результаты в лог
def results_to_log(ds):
    
    with open('top10_dom_zones.csv', 'r') as f:
        top10_dom_zones = f.read()
    with open('longest_name.txt', 'r') as f:
        longest_name = f.read()
    with open('airflow_position.txt', 'r') as f:
        airflow_position = f.read()
    
    date = ds
    #выводим top10 по доменным зонам
    print(f'top10 by domain zone for date {date}')
    print(top10_dom_zones)  
    
    #выводим самое длинное доменное имя
    print(f'longest domain name for date {date} is')
    print(longest_name)
    
    #выводим позицию домена airflow.com
    print(f'index of airflow.com for date {date} is')
    print(airflow_position)

        
default_args = {
    'owner': 'd-korzinin-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 12),
}

schedule_interval = '30 15 * * *'

dag = DAG('korzinin_airfl_les2_step4_5', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_analysis',
                    python_callable=get_top10_analysis,
                    dag=dag)

t3 = PythonOperator(task_id='get_domain_longest_name',
                        python_callable=get_domain_longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='results_to_log',
                    python_callable=results_to_log,
                    dag=dag)

t1 >> [t2, t3] >> t4


    
    
