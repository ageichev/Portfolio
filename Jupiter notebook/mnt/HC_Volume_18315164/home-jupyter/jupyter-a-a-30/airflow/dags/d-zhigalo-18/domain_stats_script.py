# Импорт библиотек
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ссылка на данные
alexa_static = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'

# Файл с данными
domains_file = 'top-1m.csv'


# Функция считывает данные и записывает в файл
def get_data(path, file_name):
    data = pd.read_csv(path)
    data_to_csv = data.to_csv(index=False)
    with open(file_name, 'w') as f:
        f.write(data_to_csv)

        
# Функция возвращает 10 наиболее встречаемых доменных зон в порядке убывания         
def top_domain_zones(file_name):
    df = pd.read_csv(file_name, names=['rank', 'domain'])
    df['d_zone'] = df.domain.str.split('.').str[-1]
    print('Top domain zones:')
    return [f'{k} - {v}' for k, v in (zip(df.d_zone.value_counts()[:10].index, df.d_zone.value_counts()[:10].values))]


# Функция возвращает самое длинное из имен домена в данных в алфавитном порядке
def get_longest_name(file_name):
    df = pd.read_csv(file_name, names=['rank', 'domain'])
    df['name_length'] = df.domain.str.split('.').str[0].str.len()
    name = df[df.name_length == df.name_length.max()].sort_values('domain').domain.values[0].split('.')[0]
    return f'The longest name - {name}'


# Функция возвращает ранг указываемого домена
def get_rank(file_name, domain_name):
    df = pd.read_csv(file_name, names=['rank', 'domain'])
    try:
        rank = df[df.domain == domain_name]['rank'].values[0]
        return f'{domain_name} rank - {rank}'
    except:
        return f'There\'s no {domain_name} in the data. Try another name!' 


# Дефолтные аргументы
default_args = {
    'owner': 'd.zhigalo',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.today().strftime('%Y-%m-%d')
}


# Инервал выполнения
schedule_interval = '0 8 * * *'


# Определяем DAG
dag = DAG('domain_stats_script', default_args=default_args, schedule_interval=schedule_interval)

# Определяем задачи
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data(alexa_static, domains_file),
                    dag=dag)

t2 = PythonOperator(task_id='top_domain_zones',
                    python_callable=top_domain_zones(domains_file),
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name(domains_file),
                        dag=dag)

t4 = PythonOperator(task_id='get_rank',
                    python_callable=get_rank(domains_file, 'airflow.com'),
                    dag=dag)


# Определяем последовательность
t1 >> [t2, t3, t4]
