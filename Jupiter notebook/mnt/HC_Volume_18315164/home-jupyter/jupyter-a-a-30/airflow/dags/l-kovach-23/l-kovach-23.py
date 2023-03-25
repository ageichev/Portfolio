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


# def get_stat(): #функция записывает в файл топ-10 доменов .ru
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))
        

# def get_stat_com(): #функция записывает в файл топ-10 доменов .com
#     top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
#     top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
#     top_data_top_10 = top_data_top_10.head(10)
#     with open('top_data_top_10_com.csv', 'w') as f:
#         f.write(top_data_top_10.to_csv(index=False, header=False))



# Найти топ-10 доменных зон по численности доменов

def get_zone_count(): 
    top_data_df_zone = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df_zone['zone'] =  top_data_df_zone.domain.apply(lambda x: x.rsplit('.')[1])
    top_data_df_zone = top_data_df_zone.groupby('zone').aggregate({'domain' : 'count'}).sort_values('domain', ascending=False).head(10) \
                    .reset_index().zone
    with open('top_data_df_zone.csv', 'w') as f:
        f.write(top_data_df_zone.to_csv(index=False, header=False))


# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest_name(): 
    top_data_df_longest = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df_longest['length'] =  top_data_df_longest.domain.apply(lambda x: len(x))
    top_data_df_longest = top_data_df_longest[['domain', 'length']].drop_duplicates()\
                                                .sort_values('length',ascending=False)\
                                                .sort_values('domain')\
                                                .head(1)\
                                                .reset_index()\
                                                .length
    with open('top_data_df_longest.csv', 'w') as f:
        f.write(top_data_df_longest.to_csv(index=False, header=False))
       
        
# На каком месте находится домен airflow.com?
def get_airflow_rank():
    try:
        df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        airflow_rank = str(df[df.domain == 'airflow.com']['rank'].values[0])
    except:
        airflow_rank = 'No info'
    with open('airflow_rank.txt', 'w') as f:
        f.write(airflow_rank)





def print_data(ds):
    with open('top_data_df_zone.csv', 'r') as f:
        get_zone_count = f.read()
    with open('top_data_df_longest.csv', 'r') as f:
        top_data_df_longest = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Топ-10 доменных зон по численности доменов на {date}')
    print(get_zone_count)

    print(f'Домен с самым длинным именем (если их несколько, то только первый в алфавитном порядке) на {date}')
    print(top_data_df_longest)
    
    print(f'Домен airflow.com на {date} находится на ')
    print(airflow_rank, 'месте')
    



default_args = {
    'owner': 'l.kovach',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 25),
}
schedule_interval = '0 13 * * *'

dag = DAG('lidiakovach', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zone_count',
                    python_callable=get_zone_count,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)