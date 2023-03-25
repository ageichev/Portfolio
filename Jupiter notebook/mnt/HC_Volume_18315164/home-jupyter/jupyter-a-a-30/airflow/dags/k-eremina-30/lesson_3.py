#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO


# In[ ]:


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[ ]:


path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'


# In[2]:


year = 1994 + hash(f'{"k-eremina-30"}') % 23


# In[ ]:


default_args = {
    'owner': 'k-eremina-30',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 3, 1)
}


# In[ ]:


@dag(default_args=default_args, schedule_interval='0 13 * * *', catchup=False)
def k_eremina_30_airflow_3():
    
    @task()
    def get_vgsales_data():
        df_vg = pd.read_csv(path)
        df_vg = df_vg.query('Year == @year')
        return df_vg


    @task() #Какая игра была самой продаваемой в этом году во всем мире?
    def get_bs_game_in_w(df_vg):
        game_bs = df_vg.groupby('Name', as_index = False)             .agg({'Global_Sales':'sum'})             .sort_values('Global_Sales', ascending = False)            .head(1)
        return game_bs.to_csv(index = False)
    
    @task() #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def get_genre_eu(df_vg):
        eu_sales_by_genre = df_vg.groupby('Genre', as_index = False)                             .agg({'EU_Sales':'sum'})
        eu_sales_max = eu_sales_by_genre.EU_Sales.max()
        top_genre_eu = eu_sales_by_genre.query('EU_Sales == @eu_sales_max')
        return top_genre_eu.to_csv(index = False)
    
    @task() #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def get_platform_na(df_vg):
        na_sales_by_platform = df_vg.query('NA_Sales > 1')                                     .groupby('Platform', as_index = False)                                     .agg({'NA_Sales':'sum'})
        na_sales_max = na_sales_by_platform.NA_Sales.max()
        top_platform_na = na_sales_by_platform.query('NA_Sales == @na_sales_max')
        return top_platform_na.to_csv(index = False)
    
    @task() #У какого издателя самые высокие средние продажи в Японии?
    def get_publisher_jp(df_vg):
        jp_mean_sales_by_publisher = df_vg.groupby('Publisher', as_index = False)                                             .agg({'JP_Sales':'mean'})
        jp_sales_max = jp_mean_sales_by_publisher.JP_Sales.max()
        top_publisher_jp = jp_mean_sales_by_publisher.query('JP_Sales == @jp_sales_max')
        return top_publisher_jp.to_csv(index = False)
    
    @task() #Сколько игр продались лучше в Европе, чем в Японии?
    def get_compare_eu_jp(df_vg):
        games_eu_jp = df_vg.groupby('Name', as_index = False)                             .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})                             .query('EU_Sales > JP_Sales').shape[0]
        return games_eu_jp
    
    @task()
    def print_data(game_bs, top_genre_eu, top_platform_na, top_publisher_jp, games_eu_jp):
        context = get_current_context()
        date = context['ds']
        print(f'The best selling game in {year} in the world {game_bs}') 
              
        print(f'The best selling genre in {year} in EU {top_genre_eu}') 
              
        print(f'The best selling platform in {year} in NA {top_platform_na}') 
      
        print(f'The best selling publisher in {year} in JP {top_publisher_jp}') 
               
        print(f'The number of games sold in {year} in EU is more than in JP {games_eu_jp}')
    
    df_vg=get_vgsales_data()
    game_bs=get_bs_game_in_w(df_vg)
    top_genre_eu=get_genre_eu(df_vg)
    top_platform_na=get_platform_na(df_vg)
    top_publisher_jp=get_publisher_jp(df_vg)
    games_eu_jp=get_compare_eu_jp(df_vg)
    
    print_data(game_bs, top_genre_eu, top_platform_na, top_publisher_jp, games_eu_jp)

k_eremina_30_airflow_3 = k_eremina_30_airflow_3()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




