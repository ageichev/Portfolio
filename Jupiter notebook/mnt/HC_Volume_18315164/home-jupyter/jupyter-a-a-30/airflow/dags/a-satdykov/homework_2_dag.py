import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# задаём таски: 
# первый таск - забираем таблицу

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream = True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    
    with open(TOP_1M_DOMAINS_FILE,'w') as f:
        f.write(top_data)

# второй таск - найти топ-10 доменных зон по численности доменов

def get_10_top_domains ():
    top_10_domains_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    top_10_domains_df['dom_type'] = top_10_domains_df['domain'].apply(lambda x: x.split('.')[1])
    top_10_domain_zones = top_10_domains_df['dom_type'].value_counts().head(10)
    
    with open ('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index = True, header = False))

# третий таск - найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)

def get_longest_domain ():
    longest_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    longest_domain_df['dom_len'] = longest_domain_df['domain'].apply(lambda x: len(x))
    longest_domain = longest_domain_df.sort_values(['dom_len', 'domain'], ascending = False).head(1).domain
    
    with open ('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index = False, header = False))

# четвёртый таск - найти на каком месте находится домен airflow.com?

def get_airflow_rank ():
    search_airflow_rank_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    
    if search_airflow_rank_df.query("domain == 'airflow.com'").shape[0] != 0:
            get_rank_airflow = search_airflow_rank_df.query("domain == 'airflow.com'").iloc[0,0]
    else:
        get_rank_airflow = pd.DataFrame({'status': ['was not found in the list']})

    with open('get_rank_airflow.csv', 'w') as f:
        f.write(get_rank_airflow.to_csv(index=False, header=False))

# ну и пятый таск -  напием логи с заголовком и датой 

def print_data(ds):
    with open ('top_10_domain_zones.csv', 'r') as f:
        top_10_domain_zones = f.read()
   
    with open ('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
        
    with open ('get_rank_airflow.csv', 'r') as f:
        get_rank_airflow = f.read()    
        
    date = ds
    
    print(f'Top 10 domain zones for date {date} are')
    print (top_10_domain_zones)
    
    print(f'The longest domain name for date {date} is')
    print (longest_domain)
    
    print(f'The airflow.com domain position in the rank for date {date} is')
    print (get_rank_airflow)
        

# создаём теперь даг под именем a-satdykov_airflow_HW_2, который также будет выполняться ежедневно в 7:40 утра

default_args = {
    'owner': 'a-satdykov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2023, 1, 21),
    'schedule_interval': '40 07 * * *'
}


dag = DAG('a-satdykov_airflow_HW_2', default_args = default_args)

# создаём таски внутри дага

task_1 = PythonOperator(task_id = 'get_data', python_callable = get_data, dag = dag)

task_2 = PythonOperator(task_id = 'get_10_top_domains', python_callable = get_10_top_domains, dag = dag)

task_3 = PythonOperator(task_id = 'get_longest_domain', python_callable = get_longest_domain, dag = dag)

task_4 = PythonOperator(task_id = 'get_airflow_rank', python_callable = get_airflow_rank, dag = dag)

task_5 = PythonOperator(task_id = 'print_data', python_callable = print_data, dag = dag)

# задаём зависимости между тасками для формирования графа

task_1 >> [task_2, task_3, task_4]  >> task_5
        
        
        
