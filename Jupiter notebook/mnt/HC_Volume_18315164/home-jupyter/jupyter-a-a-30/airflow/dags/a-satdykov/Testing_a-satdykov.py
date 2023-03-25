# Третье задание состоит в том, чтобы создать формулу, которая будет автоматически подгружать информацию из файла и пересчитывать метрики. Для этого напишем DAG, который будет совершать соответствующее действие. Предположительно он должен состоять из следующих задач

# подгрузка четырёх датасетов с Гугл диска
# формирование датасетов, на основе которых можно проводить вычисления
# расчёт метрик
# вывод метрик
# графики на основе метрик

# сам DAG размещён по ссылке https://airflow-da.lab.karpov.courses/tree?dag_id=a_satdykov_FP_DAG 

# в ходе тестирования возникают  некоторые проблемы с двумя ссылками - https://disk.yandex.ru/d/UhyYx41rTt3clQ и https://disk.yandex.ru/d/5Kxrz02m3IBUwQ - из-за каких-то внутренних ограничений Яндекс диска на скачивание файлов. Утром работает, во второй половине дня уже нет - наступает какой-то лимит на скачивания что ли

import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import requests
from urllib.parse import urlencode
import json


from airflow.operators.python import get_current_context
from airflow import DAG
from airflow.operators.python import PythonOperator

import seaborn as sns
import matplotlib.pyplot as plt

# Для начала зададим ссылки для скачивания датасетов с яндекс-диска


base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?' # базовая ссылка Яндекс-диска

groups_key = 'https://disk.yandex.ru/d/UhyYx41rTt3clQ'  # ссылка на основной датасет
active_studs_key = 'https://disk.yandex.ru/d/Tbs44Bm6H_FwFQ' # ссылка на датасет с пользователями на платформе 
checks_key = 'https://disk.yandex.ru/d/pH1q-VqcxXjsVA' # ссылка на датасет с оплатившими пользователями

groups_add_key = 'https://disk.yandex.ru/d/5Kxrz02m3IBUwQ' # ссылка на дополнительный датасет к основному

# первый таск - получаем ссылки на скачивание датасетов
  
def get_links(): 
    
    # Получим загрузочную ссылку на основной датасет
    url_1 = base_url + urlencode(dict(public_key=groups_key))
    response = requests.get(url_1)
    groups_url = response.json()['href']

    # Получим загрузочную ссылку на датасет с пользователями на платформе
    url_2 = base_url + urlencode(dict(public_key=active_studs_key))
    response = requests.get(url_2)
    active_studs_url = response.json()['href']

    # Получим загрузочную ссылку на датасет с оплатившими пользователями
    url_3 = base_url + urlencode(dict(public_key=checks_key))
    response = requests.get(url_3)
    checks_url = response.json()['href']

    # Получим загрузочную ссылку на дополнительный к основному датасет
    url_4 = base_url + urlencode(dict(public_key=groups_add_key))
    response = requests.get(url_4)
    groups_add_url = response.json()['href']

    return groups_url, active_studs_url, checks_url, groups_add_url
    
# второй таск - прочтём полученные датасеты. По ходу будем менять заголовки, так как по условию они могут варьироваться 
        
def get_dfs():
                
    # прочтём основной датасет
    df_groups =  pd.read_csv(groups_url, sep = ';', header = 0, names = ['id', 'grp'])

    # прочтём датасет с пользователями на платформе
    df_active_studs =  pd.read_csv(active_studs_url, sep = ';', header = 0, names = ['id'])

    # прочтём датасет с оплатившими пользователями
    df_checks =  pd.read_csv(checks_url, sep = ';', header = 0, names = ['id', 'rev'])

    # прочтём дополнительный к основному датасет
    df_groups_add =  pd.read_csv(groups_add_url, header = 0, names = ['id', 'grp'])

    return df_groups, df_active_studs, df_checks, df_groups_add
    
# третий таск - подготовим датасеты для дальнейшего вычисления необходимых нам метрик 
     
def get_prep_dfs():
        
    # прежде всего объединим основной и дополнительнй датасеты
    df_groups = pd.concat([df_groups, df_groups_add])

    # в датасет к пользователям, зашедшим на платформу, добавим их соотнесение по группам - тестовой и целевой
    df_active_studs_gr = df_active_studs.merge(df_groups, how = 'left', on = 'id')

    # добавим распределение потребителей, совершивших покупку, по целевой и контрольной группам  
    df_checks_gr_2 = df_checks.merge(df_groups, how = 'left', on = 'id')

    # общее количество пользователей
    users = df_groups.groupby('grp').agg({'id':'count'})

    # общее количество зашедших на платформу
    visitors = df_active_studs_gr.groupby('grp').agg({'id':'count'})

    # общее количество оплативших пользователей
    customers = df_checks_gr_2.groupby('grp').agg({'id':'count'})

    return users, visitors, customers, df_checks_gr_2
           
# пятый таск - рассчитаем значения метрик  
     
def get_metrics ():
                
    # Итак, рассчитаем конверсию в посещение по группам 
    visitors_a = (visitors.iloc[0] / users.iloc[0] * 100)[0].round(2)
    visitors_b = (visitors.iloc[1] / users.iloc[1] * 100)[0].round(2)

    # Посмотрим на конверсию в оплату 
    customers_a = (customers.iloc[0] / users.iloc[0] * 100)[0].round(2)
    customers_b = (customers.iloc[1] / users.iloc[1] * 100)[0].round(2)

    # Посмотрим теперь на значение ARPU
    arpu_a = (df_checks_gr_2.loc[df_checks_gr_2.grp == 'A'].rev.sum() / users.iloc[0])[0].round(2)
    arpu_b = (df_checks_gr_2.loc[df_checks_gr_2.grp == 'B'].rev.sum() / users.iloc[1])[0].round(2)

    # Посмотрим на значение ARPPU
    arppu_a = df_checks_gr_2.groupby('grp', as_index = False).agg({'rev':'mean'}).rename(columns = {'rev':'ARPPU'}).round(2).ARPPU[0]
    arppu_b = df_checks_gr_2.groupby('grp', as_index = False).agg({'rev':'mean'}).rename(columns = {'rev':'ARPPU'}).round(2).ARPPU[1]

    return visitors_a, visitors_b, customers_a, customers_b, arpu_a, arpu_b, arppu_a, arppu_b 
    
# предпоследний таск - печатаем полученные нами результаты 
    
def print_data():
        
    print(f'Конверсия в посещение в контрольной группе соствляет {visitors_a}')
    print(f'Конверсия в посещение в целевой группе соствляет {visitors_b}')

    print(f'Конверсия в оплату в контрольной группе соствляет {customers_a}')
    print(f'Конверсия в оплату в целевой группе соствляет {customers_b}')

    print(f'ARPU в контрольной группе соствляет {arpu_a}')
    print(f'ARPU в целевой группе соствляет {arpu_b}')

    print(f'ARPU в контрольной группе соствляет {arppu_a}')
    print(f'ARPU в целевой группе соствляет {arppu_b}')
    
# последние таски - нарисуем графики

def plot_CR ():
    
    data_CR = {'Group': ['A', 'B'],
        'CR': [customers_a, customers_b]
        }
    df_CR = pd.DataFrame(data_CR)

    fig, ax = plt.subplots(figsize = (10, 6))
    ax = sns.barplot(data=df_CR, x = 'Group', y = 'CR', ax=ax)
    ax.set_title('Конверсия в покупку (%)', fontsize=16)
    
def plot_ARPU ():
    
    data_ARPU = {'Group': ['A', 'B'],
        'ARPU': [arpu_a, arpu_b]
        }
    df_ARPU = pd.DataFrame(data_ARPU)

    fig, ax = plt.subplots(figsize = (10, 6))
    ax = sns.barplot(data=df_ARPU, x = 'Group', y = 'ARPU', ax=ax)
    ax.set_title('ARPU (RUB)', fontsize=16)

def plot_ARPPU ():
    
    data_ARPPU = {'Group': ['A', 'B'],
        'ARPPU': [arppu_a, arppu_b]
        }
    df_ARPPU = pd.DataFrame(data_ARPPU)

    fig, ax = plt.subplots(figsize = (10, 6))
    ax = sns.barplot(data=df_ARPPU, x = 'Group', y = 'ARPPU', ax=ax)
    ax.set_title('ARPPU (RUB)', fontsize=16)  
    
    
# зададим дефолтные параметры

default_args = {
    'owner': 'a-satdykov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 7),
    'schedule_interval': '7 40 * * *'
}


# Зададим даг 

dag = DAG('a_satdykov_FP_DAG', default_args = default_args)

# создаём таски   

task_1 = PythonOperator(task_id = 'get_links', python_callable = get_links, dag = dag)

task_2 = PythonOperator(task_id = 'get_dfs', python_callable = get_dfs, dag = dag)

task_3 = PythonOperator(task_id = 'get_prep_dfs', python_callable = get_prep_dfs, dag = dag)

task_4 = PythonOperator(task_id = 'get_metrics', python_callable = get_metrics, dag = dag)

task_5 = PythonOperator(task_id = 'print_data', python_callable = print_data, dag = dag)

task_6 = PythonOperator(task_id = 'plot_CR', python_callable = plot_CR, dag = dag)

task_7 = PythonOperator(task_id = 'plot_ARPU', python_callable = plot_ARPU, dag = dag)

task_8 = PythonOperator(task_id = 'plot_ARPPU', python_callable = plot_ARPPU, dag = dag)


# задаём зависимости между тасками для формирования графа

task_1 >> task_2 >> task_3 >> task_4 >> [task_5, task_6, task_7, task_8]
 