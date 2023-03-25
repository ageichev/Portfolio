#!/usr/bin/env python
# coding: utf-8

# In[3]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[4]:


df = pd.read_csv('vgsales.csv')


# In[5]:


df.head()


# In[ ]:





# In[7]:


frame = 'vgsales.csv'


# In[8]:


default_args = {
    'owner':'s-klukvina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 4),
    'schedule_interval': '0 12 * * *' #timedelta(days=1)
}


# In[9]:


@dag(default_args=default_args, catchup=False)
def airflow_lesson3():
    
    @task()
    def get_year():
        df = pd.read_csv(frame)
        year = 1994 + hash(f'{s-klukvina}') % 23 #определяем год
        return year
    
    @task()
    def get_data(year): #считывание данных
        df = pd.read_csv(frame)
        df_year = df.loc[df.Year == year]
        return df_year
        
    @task() 
    def popular_game(df_year): #Какая игра была самой продаваемой в этом году во всем мире?
        popular_game = df_year.groupby('Name', as_index=False).agg({'Global_Sales':'sum'}).Global_Sales.max()
        return popular_game     
        
    @task()
    def popular_game_EU(df_year): #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
        popular_game_EU = df_year.groupby(['Name', 'Genre'], as_index = False).agg({'EU_Sales':sum}).nlargest(2, 'EU_Sales')
        return popular_game_EU
    
    @task() #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def the_best_platform_NA(df_year):
        the_best_platform_NA = df.query('NA_Sales > 1').groupby(['NA_Sales', 'Platform'], as_index=False).agg({'Rank':'sum'}).nlargest(1, 'Rank', keep='all').Platform.to_list()
        return the_best_platform_NA
    
    @task() #У какого издателя самые высокие средние продажи в Японии?
    def the_best_mean_sale_JP(df_year):
        the_best_mean_sale_JP = df.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'}).sort_values(by='JP_Sales', ascending = False).nlargest(1, 'JP_Sales').Publisher.to_list()
        return the_best_mean_sale_JP
    
    @task() # Сколько игр продались лучше в Европе, чем в Японии?
    def better_sale_EU_than_JP(df_year):
        better_sale_EU_than_JP = df.groupby('Name', as_index=False).agg({'EU_Sales':'sum', 'JP_Sales':'sum'}).query('EU_Sales>JP_Sales').Name.count()
        return better_sale_EU_than_JP
    
    @task()
    def print_data(df_year, popular_game, popular_game_EU, the_best_platform_NA, the_best_mean_sale_JP, better_sale_EU_than_JP):
        print(f'my dataset is {df_year}')
        print(f'data for the year {year}')
        print(f'popular_game is {popular_game}')
        print(f'popular game in Europe is {popular_game_EU}')
        print(f'the best platform in North America is {the_best_platform_NA}')
        print(f'the best mean sale in Japan is {the_best_mean_sale_JP}')
        print(f'better sale in Europe than in Japan is {better_sale_EU_than_JP}')
    


# In[ ]:





# In[ ]:




