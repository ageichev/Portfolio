import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
import os.path

curr_date = datetime.today().strftime("%d-%m-%y %H:%M")

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
        
#Найти топ-10 доменных зон по численности доменов
def get_top10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df = top_data_df['domain'].str.split('.').apply(lambda x: x[-1]).value_counts()[:10]
    top_data_df = pd.DataFrame(top_data_df).reset_index().rename(columns={'domain':'counts','index':'domain'})
    return 'Top 10 domains by regions: \n' + str(top_data_df) + '\n'

#Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_max_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_length = top_data_df.domain.str.len().max()
    return 'Largest domain: \n' + str(list(top_data_df[top_data_df.domain.str.len() == max_length]
                                                             .domain.sort_values().values)[0])+'\n'

#На каком месте находится домен airflow.com?
def get_airflow_place():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df = top_data_df[top_data_df.domain == 'airflow.com']
    lines = ''
    if len(top_data_df) > 0:
        lines = str(top_data_df)+'\n'
    else:
        lines = 'airflow.com not in top \n'
    return lines
    
def final_print(ts = 'none'):
    curr_date = ts
    lines = 'Report '+ curr_date + '\n'
    lines = lines + str(get_top10())+'\n'
    lines = lines + str(get_max_length())+'\n'
    lines = lines + str(get_airflow_place())+'\n'
    
    output_hist = ''
    if os.path.isfile('log-test_DAG.txt'):
        with open('log-test_DAG.txt', 'r') as f:
            output_hist = f.readlines()

    with open('log-test_DAG.txt', 'w') as f:
        for i in range(len(output_hist)):
            f.write(str(output_hist[i]))
        f.write(str(lines))
    print(lines)


default_args = {
    'owner': 'm.pleshakov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2022, 9, 18),
}
schedule_interval = '*/30 */12 * * *'

dag = DAG('test_DAG_m-pleshakov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10',
                    python_callable=get_top10,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_length',
                        python_callable=get_max_length,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_place',
                        python_callable=get_airflow_place,
                        dag=dag)

t5 = PythonOperator(task_id='final_print',
                    python_callable=final_print,
                    dag=dag)

t1 >> t5
