#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import timedelta
from datetime import datetime

import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# In[ ]:


vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'd-andreev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 22),
    'schedule_interval': '0 13 * * *'
}

year = 1994 + hash(f'd-andreev') % 23


# In[ ]:


CHAT_ID = 390702961
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Done! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        print('Бот протестирован, токен удалён')


# In[ ]:


@dag(default_args=default_args, catchup=False)
def d_andreev_airflow_2():

    @task()
    def get_data():
        top_data = pd.read_csv(vgsales)
        top_data = top_data.query('Year == @year')
        return top_data

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_best_sales(top_data):
        best_sales = top_data                             .groupby(['Name'])                             .agg({'Global_Sales':'sum'})                             .sort_values('Global_Sales', ascending = False)                             .idxmax()[0]
        return best_sales

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_best_genre(top_data):
        best_genre = top_data                             .groupby(['Genre'])                             .agg({'EU_Sales':'sum'})                             .sort_values('EU_Sales', ascending = False)                             .idxmax()[0]
        return best_genre

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task()
    def get_best_platform(top_data):
        best_platform = top_data                                 .query('NA_Sales > 1')                                 .groupby('Platform')                                 .agg({'Name':'count'})                                 .sort_values('Name', ascending = False)                                 .idxmax()[0]
        return best_platform

    # У какого издателя самые высокие средние продажи в Японии?
    # Перечислить все, если их несколько
    @task()
    def get_best_publisher(top_data):
        top_data_mean = top_data                                 .groupby('Publisher', as_index = False)                                 .agg({'JP_Sales':'mean'})                                 .sort_values('JP_Sales', ascending = False)
        
        mean_max = top_data_mean.max()[1]
        
        best_publisher = top_data_mean                                         .query('JP_Sales == @mean_max')                                         .Publisher                                         .tolist()
        return best_publisher

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_best_E_vs_J(top_data):
        best_E_vs_J = top_data                                 .groupby('Name', as_index = False)                                 .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})                                 .query('EU_Sales > JP_Sales')                                 .Name                                 .count()
        return best_E_vs_J

    # Вывод результата и отправка сообщения
    @task(on_success_callback=send_message)
    def print_data(best_sales,
                   best_genre,
                   best_platform,
                   best_publisher,
                   best_E_vs_J):


        print(f'Самая продаваемая игра в {year} году была {best_sales}\n'
              f'Самые продаваемые игры в {year} году были в жанре {best_genre}\n'
              f'Больше всего игр в Северной Америке в {year} году, продаваемых тиражом более 1 млн. копий, вышли на платформе {best_platform}\n'
              f'Самые высокие средние продажи в Японии в {year} году были у {", ".join(best_publisher)}\n'
              f'В {year} году в Европе лучше продалось {best_E_vs_J} игр чем в Японии')

    top_data       = get_data()
    best_sales     = get_best_sales(top_data)
    best_genre     = get_best_genre(top_data)
    best_platform  = get_best_platform(top_data)
    best_publisher = get_best_publisher(top_data)
    best_E_vs_J    = get_best_E_vs_J(top_data)

    print_data(best_sales, best_genre, best_platform, best_publisher, best_E_vs_J)
        
d_andreev_airflow_2 = d_andreev_airflow_2()        

