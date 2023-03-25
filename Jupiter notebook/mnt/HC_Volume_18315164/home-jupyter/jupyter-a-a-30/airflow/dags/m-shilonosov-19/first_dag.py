#!/usr/bin/env python
# coding: utf-8

# In[3]:


from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишет такси в питоне
from datetime import datetime


# In[ ]:


def hello_world():
    print('hello_world')


# In[ ]:


def i_am():
    print('I am Max')


# In[ ]:


default_args = {
    'owner': 'your_name', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков

    'schedule_interval': '30 15 * * *' # cron выражение, также можно использовать '@daily', '@weekly'
    #'schedule_interval': '@daily' переменные airflow
    #'schedule_interval': timedelta() параметр timedelta

    'retries': 1, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками

    'email': 'nesferus1@yandex.ru', # Почта для уведомлений 
    'email_on_failure': '', # Почта для уведомлений при ошибке
    'email_on_retry': '', # Почта для уведомлений при перезапуске

    'retry_exponential_backoff': '', # Для установления экспоненциального времени между перезапусками
    'max_retry_delay': '', # Максимальный промежуток времени для перезапуска

    'start_date': '', # Дата начала выполнения DAG
    'end_date': '', # Дата завершения выполнения DAG

    'on_failure_callback': '', # Запустить функцию если DAG упал
    'on_success_callback': '', # Запустить функцию если DAG выполнился
    'on_retry_callback': '', # Запустить функцию если DAG ушел на повторный запуск
    'on_execute_callback': '', # Запустить функцию если DAG начал выполняться
     # Задать документацию
    'doc': '',
    'doc_md': '',
    'doc_rst': '',
    'doc_json': '',
    'doc_yaml': ''
}
t1=pythonOperator

dag = DAG('HelloWorld', default_args=default_args)


# In[ ]:


t1 = PythonOperator(task_id='hello', # Название таска
                    python_callable=hello_world, # Название функции
                    dag=dag) # Параметры DAG

t2 = PythonOperator(task_id='i_am', # Название таска
                    python_callable=i_am, # Название функции
                    dag=dag) # Параметры DAG


# In[ ]:


t1>>t2

