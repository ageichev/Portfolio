# Задание по второму уроку airflow

# импорт библиотек
import requests 
from zipfile import ZipFile 
from io import BytesIO 
import pandas as pd 
from datetime import timedelta 
from datetime import datetime 
from airflow import DAG 
from airflow.operators.python import PythonOperator

# данные
TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# получение данных
def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# Найти топ-10 доменных зон по численности доменов        
def get_top_10():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rang', 'domain'])
    df['top_dom'] = df['domain'].str.split('.').str[-1]
    df_top_10 = df.top_dom.value_counts().sort_values(ascending=False).reset_index().head(10)
    df_top_10.rename(columns = {'index' : 'domain', 'top_dom':'qty'}, inplace = True)
    with open('df_top_10.csv', 'w') as f:
        f.write(df_top_10.to_csv(index=False, header=False))
        
# Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest():  
#     df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rang', 'domain'])  
#     the_longest = max([(len(x),x) for x in (df['domain'])])  
#     with open('the_longest.csv', 'w') as f:  
#         f.write(the_longest.to_csv(index=False, header=False)) 
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rang', 'domain'])
    get_longest = df.loc[df.domain.str.len() == df.domain.str.len().max()]
    with open('get_longest.csv', 'w') as f:
        f.write(get_longest.to_csv(index=False, header=False))               
        
# На каком месте находится домен airflow.com? 
def where_airflow(): 
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rang', 'domain']) 
    air = df.query('domain == "airflow.com"') 
    with open('air.csv', 'w') as f: 
        f.write(air.to_csv(index=False, header=False))  

# Вывести все данные 
def print_data(ds): 
    with open('df_top_10.csv', 'r') as f: 
        df_top_10 = f.read() 
    with open('the_longest.csv', 'r') as f: 
        the_longest = f.read() 
    with open('air.csv', 'r') as f: 
        air = f.read()     
    date = ds

    print(f'Top domains zone by domain counts for date {date}')
    print(df_top_10)
    print(f'The longest domail name for date {date} is {the_longest}')
    print(f'Airfow rank for date {date} is {air}')

# DAG    
default_args = {
    'owner': 'i-bezhentsev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 11),
    'schedule_interval': '0 11 * * *'
}
dag = DAG('i-bezhentsev', default_args=default_args)

# таски
t1 = PythonOperator(task_id='get_data', 
                    python_callable=get_data, 
                    dag=dag) 
 
t2 = PythonOperator(task_id='get_top_10', 
                    python_callable=get_top_10, 
                    dag=dag) 
 
t3 = PythonOperator(task_id='get_longest', 
                        python_callable=get_longest, 
                        dag=dag) 
 
t4 = PythonOperator(task_id='where_airflow', 
                        python_callable=where_airflow, 
                        dag=dag) 
 
t5 = PythonOperator(task_id='print_data', 
                    python_callable=print_data, 
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

# конец решения