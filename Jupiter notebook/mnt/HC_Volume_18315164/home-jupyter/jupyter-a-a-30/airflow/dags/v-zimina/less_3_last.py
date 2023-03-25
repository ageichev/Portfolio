#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO


# In[2]:


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[3]:


file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'


# In[4]:


year = 1994 + hash(f'{"v-zimina"}') % 23


# In[21]:


default_args = {
    'owner': 'v_zimina',
    'depends_on_past': False,
    'retries': 2, 
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 27), 
    'schedule_interval': '50 22 * * *' 
} 
@dag(default_args=default_args, catchup=False)
def v_zim_airflow(): 
    @task() 
    def find_my_df(): 
        df = pd.read_csv(file).query('Year == @year')
        return df
    
    #Какая игра была самой продаваемой в этом году во всем мире?
    @task() 
    def max_glsales(df): 
        name = df.groupby('Name',as_index=False).agg({'Global_Sales':'sum'})
        maxsal_game = name[name['Global_Sales']==name.Global_Sales.max()].Name.iloc[0]
        return maxsal_game 
    
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task() 
    def max_eusales(df): 
        genre = df.groupby('Genre',as_index=False).agg({'EU_Sales':'sum'})
        max_genre = genre[genre.EU_Sales == genre.EU_Sales.max()].Genre.iloc[0]
        return max_genre
    
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    #Перечислить все, если их несколько
    @task() 
    def num_platform(df): 
        platf = df[df.NA_Sales > 1].groupby('Platform',as_index=False).agg({'NA_Sales':'count'})
        max_pl = platf[platf.NA_Sales == platf.NA_Sales.max()].Platform.iloc[0]
        return max_pl
    
    #У какого издателя самые высокие средние продажи в Японии?
    #Перечислить все, если их несколько
    @task() 
    def publisher(df): 
        pub = df.groupby('Publisher',as_index=False).agg({'JP_Sales':'mean'})
        mean_pb = pub[pub.JP_Sales == pub.JP_Sales.max()].Publisher.iloc[0]
        return mean_pb
    
    #Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def eu_vs_jp(df):
        all_ = df.groupby('Name',as_index=False).agg({'EU_Sales':'sum','JP_Sales':'sum'})
        name_count = all_[all_.EU_Sales > all_.JP_Sales].Name.count()
        return name_count
    
    
    
    @task() 
    def show_res(maxsal_game, max_genre, max_pl, mean_pb, name_count): 
        context = get_current_context() 
        date = context['ds'] 
        
        print(f'Данные представленны за {year}')
        print(f'Самой продаваемой игрой стала {maxsal_game}')
        print(f'Игры жанра {max_genre} были самыми продаваемыми в Европе')
        print(f'На платформе {max_pl} было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке')
        print(f'Самые высокие средние продажи в Японии у издателя {mean_pb}')
        print(f'{name_count} игр продались лучше в Европе, чем в Японии')
        
    
    df = find_my_df()
    
    maxsal_game = max_glsales(df)                            
    max_genre   = max_eusales(df) 
    max_pl      = num_platform(df) 
    mean_pb     = publisher(df) 
    name_count  = eu_vs_jp(df) 
    
    show_res(maxsal_game, max_genre, max_pl, mean_pb, name_count)
             
v_zim_airflow = v_zim_airflow()  


# In[ ]:




