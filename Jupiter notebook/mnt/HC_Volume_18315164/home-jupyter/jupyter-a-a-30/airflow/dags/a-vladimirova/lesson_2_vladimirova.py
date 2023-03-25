#Блок импортов
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


#Подгружаем данные
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

#Блок кода
#Здесь вы прописываете ваши функции

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# Найти топ-10 доменных зон по численности доменов
def get_stat_domain_zone():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['zone'] = top_doms.domain.str.split('.').str[-1]
    top_data_top_10 = top_doms.zone.value_counts().head(10).reset_index().rename(columns={'index':'zone', 'zone':'count'})
    top_data_top_10.index += 1
    top_data_top_10 = top_data_top_10.zone
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_stat_domain_zone_max_len():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['len_domain'] = top_doms.domain.str.len()
    top_data_max_len = top_doms.query('len_domain == len_domain.max()').sort_values('domain', ascending = True)
    top_data_max_len = top_data_max_len.drop(['rank', 'len_domain'], axis=1)
    with open('top_data_max_len.csv', 'w') as f:
        f.write(top_data_max_len.to_csv(index=False, header=False))
        
# На каком месте находится домен airflow.com?
def get_stat_domain_place():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['len_domain'] = top_doms.domain.str.len()
    top_doms = top_doms.sort_values('len_domain', ascending= False).reset_index()
    domen_place = top_doms.query("domain == 'airflow.com'").reset_index()
    domen_place = domen_place[['domain', 'level_0']].rename(columns = {'level_0':'place'})
    if domen_place.empty:
        result = 'Sorry, domain airflow.com is not in the list'
    else:
        result = domen_place.place
    print(result)
    with open('domen_place.txt', 'w') as f:
        f.write(str(result))


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('top_data_max_len.csv', 'r') as f:
        max_len = f.read()
    with open('domen_place.txt', 'r') as f:
        domen_place = f.read()
    date = ds

    print(f'Top 10 domains zone on the date {date}:')
    print(top_10)

    print(f'The longest domain name on the date {date}:')
    print(max_len)
    
    print(f'"airflow.com" position by lenght on the date {date}:')
    print(domen_place)

#Блок инициализации
#Задаем параметры в DAG:

#Инициализируем DAG

default_args = {
    'owner': 'a-vladimirova',  # Владелец операции
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=10), # Промежуток между перезапусками
    'start_date': datetime(2023, 1, 27), # Дата начала выполнения DAG
}
schedule_interval = '10 14 * * *' # cron выражение, также можно использовать '@daily', '@weekly'
a_vladimirova_dag = DAG('a-vladimirova', default_args=default_args, schedule_interval=schedule_interval)


#Инициализируем таски:

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=a_vladimirova_dag)
t2 = PythonOperator(task_id='get_stat_domain_zone',
                    python_callable=get_stat_domain_zone,
                    dag=a_vladimirova_dag)

t2_max_len = PythonOperator(task_id='get_stat_domain_zone_max_len',
                        python_callable=get_stat_domain_zone_max_len,
                        dag=a_vladimirova_dag)

t2_domain_place = PythonOperator(task_id='get_stat_domain_place',
                    python_callable=get_stat_domain_place,
                    dag=a_vladimirova_dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=a_vladimirova_dag)

t1 >> [t2, t2_max_len, t2_domain_place] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)


# In[ ]:
