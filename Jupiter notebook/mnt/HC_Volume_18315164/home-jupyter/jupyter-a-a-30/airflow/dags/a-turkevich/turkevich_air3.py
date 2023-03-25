#!/usr/bin/env python
# coding: utf-8

# In[9]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from airflow.models import Variable


# In[2]:


default_args = {
    'owner': 'a.turkevich',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '0 18 * * *'
}


# In[8]:


@dag(default_args=default_args, catchup=False)
def turkevich_air3():
    @task()
    def get_data():
        # читаем данные
        file_name = 'vgsales.csv'
        vgsales = pd.read_csv(file_name)
        year = 1994 + hash(f'{"a-turkevich"}') % 23 #
        vgsales = vgsales.query("Year == @year")
        return vgsales  
    @task()
    def get_best_sales(vgsales):
        # Какая игра была самой продаваемой в этом году во всем мире?
        best_sales = vgsales
        best = best_sales.groupby("Name",as_index=False).agg({"Global_Sales":"sum"})                               .sort_values("Global_Sales",ascending=False).iloc[0].Name
        return best  
    @task()
    def get_best_Genre_EU(vgsales):
        #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
        best_sales = vgsales
        best_Genre_EU = best_sales.groupby("Genre",as_index=False).agg({"EU_Sales":"sum"})                               .sort_values("EU_Sales",ascending=False).head(1).Genre.reset_index().Genre.iloc[0]
        return best_Genre_EU   
    @task()
    def get_PlatformNA(vgsales):
        #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
        best_sales = vgsales
        best_PlatformNA = best_sales.query("NA_Sales>1").groupby("Platform",as_index=False).agg({"NA_Sales":"sum"})                               .sort_values("NA_Sales",ascending=False).head(1).reset_index().Platform.iloc[0]
        return best_PlatformNA
    @task()
    def get_Publisher_jp(vgsales):
        #У какого издателя самые высокие средние продажи в Японии?Перечислить все, если их несколько
        best_sales = vgsales
        Publisher_jp = best_sales.groupby("Publisher",as_index=False).agg({"JP_Sales":"mean"})                               .sort_values("JP_Sales",ascending=False).head(1).reset_index().Publisher.iloc[0]
        return Publisher_jp  
    @task()
    def get_sales_jp_eu(vgsales):
        #Сколько игр продались лучше в Европе, чем в Японии?
        best_sales = vgsales
        sales_jp_eu = best_sales.groupby("Name",as_index=False).agg({"JP_Sales":"sum","EU_Sales":"sum"})                               .sort_values("EU_Sales",ascending=False).query("EU_Sales>JP_Sales")
        return len(sales_jp_eu)  
    @task()
    def print_data( best, best_Genre_EU, best_PlatformNA, Publisher_jp, sales_jp_eu):
        year = 1994 + hash('a-turkevich') % 23

        print(f"Самая продаваемая игра за {year} год, была {best}")
        print(f"Жанр игр с лучшими продажами в Европе за {year} год это - {best_Genre_EU}")
        print(f"Самая популярная платформа с более чем миллионым тиражом в Северной америке за {year} год - {best_PlatformNA}")
        print(f"Самые высокие средние продажи в Японии за {year} год , у {Publisher_jp}")
        print(f"Количество игр, которые продались  в Европе лучше чем в Японии за {year} - {sales_jp_eu}")
    vgsales = get_data()
    best = get_best_sales(vgsales)
    best_Genre_EU = get_best_Genre_EU(vgsales)
    best_PlatformNA = get_PlatformNA(vgsales)
    Publisher_jp  = get_Publisher_jp(vgsales)
    sales_jp_eu  = get_sales_jp_eu(vgsales)
    print_data(best, best_Genre_EU, best_PlatformNA, Publisher_jp, sales_jp_eu)
turkevich_air3 = turkevich_air3()

