#!/usr/bin/env python
# coding: utf-8

# In[46]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
# import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[47]:


vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'


# In[48]:


default_args = {
    'owner': 'e-shovgenova-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 12, 11),
    'schedule_interval': '0 * * * *'
}


# In[49]:


y = 1994 + hash(f'e-shovgenova-21') % 23


# In[53]:


@dag(default_args=default_args, catchup=False)
def e_shovgenova_HW_3():
    @task()
    def get_data():
        df = pd.read_csv(vgsales)
        return df
    #  Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def best_selling_game(df):
        return (df.query('Year==@y').groupby('Name').sum().Global_Sales.idxmax())
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def most_selling_genre_EU(df):
        return (df.query('Year==@y').groupby('Genre').sum().EU_Sales.idxmax())
    @task()
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def most_succ_platform_NA(df):
        return (df.query("Year == @y & NA_Sales > 1").groupby('Platform').sum().NA_Sales.idxmax())
    @task()
    # У какого издателя самые высокие средние продажи в Японии?
    def highest_mean_sales_JP(df):
        return (df.query('Year==@y').groupby('Publisher').mean().JP_Sales.idxmax())
    @task()
    # Сколько игр продались лучше в Европе, чем в Японии?
    def number_games_EU_over_JP(df):
        return (df.query("Year==@y").EU_Sales > df.query("Year==@y").JP_Sales).sum()
    
    # Вывести на печать таски
    @task()
    def print_results(best_selling_game, most_selling_genre_EU, most_succ_platform_NA, highest_mean_sales_JP, number_games_EU_over_JP):
        print(f"{best_selling_game} была самой продаваемой игрой в {y} во всем мире;                 {most_selling_genre_EU} были самыми продаваемыми жанрами в Европе;                 На {most_succ_platform_NA} платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке;                 У {highest_mean_sales_JP} самые высокие средние продажи в Японии;                 {number_games_EU_over_JP} игр продались лучше в Европе, чем в Японии.")
        
        
    df = get_data()
    top_game = best_selling_game(df)
    eu_genre = most_selling_genre_EU(df)
    na_platform = most_succ_platform_NA(df)
    jp_publisher = highest_mean_sales_JP(df)
    games_eu_vs_jp = number_games_EU_over_JP(df)
    
    print_results(top_game, eu_genre, na_platform, jp_publisher, games_eu_vs_jp)

    
e_shovgenova_HW_3 = e_shovgenova_HW_3()
    


# In[ ]:




