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


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

default_args = {
    'owner': 'nat_gorbacheva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 2)}


@dag(default_args=default_args, catchup=False)
def nat_gorbacheva():
    @task()
    def get_data():
        # читаем данные  за год
        file_name = 'var/lib/airflow.git/dags/a.batalov/vgsales.csv'
        file_name = 'vgsales.csv'
        vgsales = pd.read_csv(file_name)
        year = 1994 + hash(f'{"n-gorbacheva-21"}') % 23 # Определяем год в соответствии с логином
        vgsales = vgsales.query("Year == @year")
        return vgsales  
   
    @task()
    def get_top_sales(vgsales): # Cамая продаваемая игра
        top_sales = vgsales
        top_games_GS = top_sales.groupby("Name",as_index=False).agg({"Global_Sales":"sum"}).sort_values("Global_Sales",ascending=False).iloc[0].Name
        return top_games_GS  
    
    @task()
    def get_top_Genre_EU(vgsales): # Cамый популярный жанр в Европе
        top_sales = vgsales
        top_GenreEU  = best_sales.groupby("Genre",as_index=False).agg({"EU_Sales":"sum"}).sort_values("EU_Sales",ascending=False).Genre.reset_index().head(3)
        return top_GenreEU   
    
    @task()
    def get_Platforma_NA(vgsales): #На какой платформе было больше всего игр в Северной Америке?
        top_sales = vgsales
        top_Platform_NA = best_sales.query("NA_Sales>1").groupby("Platform",as_index=False).agg({"NA_Sales":"sum"})                               .sort_values("NA_Sales",ascending=False).head(1).reset_index().Platform.iloc[0]
        return top_Platform_NA
    @task()
    def get_PublisherJP(vgsales):# Издатель с самыми высокими средними продажами в Японии
        top_sales = vgsales
        PublisherJP = top_sales.groupby("Publisher",as_index=False).agg({"JP_Sales":"mean"}).sort_values("JP_Sales",ascending=False).head(1).reset_index().Publisher.iloc[0]
        return PublisherJP  
    @task()
    def get_salesEU_JP(vgsales):#Сколько игр продались лучше в Европе, чем в Японии?
        top_sales = vgsales
        SalesEU_JP = vgsales.query('EU_Sales > JP_Sales').Name.count()
        return SalesEU_JP 
    @task()
    def print_data(top_games_GS, top_GenreEU, top_Platform_NA, PublisherJP, SalesEU_JP):
        year = 1994 + hash(f'{"n-gorbacheva-21"}') % 23

        print(f"Самая продаваемая игра за {year} год, была {top_games_GS}")
        print(f"Жанр игр с лучшими продажами в Европе за {year} год это - {top_GenreEU}")
        print(f"Самая популярная платформа в Северной америке за {year} год - {top_Platform_NA}")
        print(f"Самые высокие средние продажи в Японии за {year} год , у {PublisherJP}")
        print(f"Количество игр, которые продались  в Европе лучше чем в Японии за {year} - {SalesEU_JP}")
    vgsales = get_data()
    top_games_GS = get_top_sales(vgsales)
    top_GenreEU = get_top_Genre_EU(vgsales)
    top_Platform_NA = get_Platforma_NA(vgsales)
    PublisherJP  = get_PublisherJP(vgsales)
    SalesEU_JP  = get_salesEU_JP(vgsales)
    print_data(top_games_GS, top_GenreEU, top_Platform_NA, PublisherJP, SalesEU_JP)
nat_gorbacheva = nat_gorbacheva()


# In[ ]:




