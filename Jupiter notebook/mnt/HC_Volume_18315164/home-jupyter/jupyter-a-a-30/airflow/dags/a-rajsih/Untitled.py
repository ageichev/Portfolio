#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task


# In[5]:


DATA_GAMES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
YEAR = 1994 + hash(f'a-rajsih') % 23


# In[80]:


default_args = {
    'owner': 'a.rajsih',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 30),
    'schedule_interval': '0 12 * * *'
}


# In[81]:


@dag(default_args=default_args, catchup=False)
def aer_lesson_3():
    @task()
    def get_data():
        dg = pd.read_csv(DATA_GAMES)
        dg = dg[dg.Year == YEAR].reset_index()
        return dg

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def get_name_global(dg):
        dgnw = dg.groupby('Name').agg({'Global_Sales': 'sum'}).reset_index()
        name_global = dgnw.Name[dgnw.Global_Sales == dgnw.Global_Sales.max()]
        return name_global.to_string(index=False)

   # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_genre_eu(dg):
        dgge = dg.groupby('Genre').agg({'EU_Sales': 'sum'}).reset_index()
        genre_eu = dgge.Genre[dgge.EU_Sales == dgge.EU_Sales.max()]
        return genre_eu.to_string(index=False)
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task()
    def get_name_1_na(dg):
        dgnpa = dg.groupby(['Name', 'Platform']).agg({'NA_Sales': 'sum'}).reset_index()
        dgnpa = dgnpa[dgnpa.NA_Sales >= 1]
        dgnpa = dgnpa.groupby('Platform').agg({'NA_Sales': 'sum'}).reset_index()
        name_1_na = dgnpa.Platform[dgnpa.NA_Sales == dgnpa.NA_Sales.max()]
        return name_1_na.to_string(index=False)

    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_pub_jp(dg):
        dgpj = dg.groupby('Publisher').agg({'JP_Sales': 'mean'}).reset_index()
        pub_jp = dgpj.Publisher[dgpj.JP_Sales == dgpj.JP_Sales.max()]
        return pub_jp.to_string(index=False)

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_eu_better_jp(dg):
        ej = dg.groupby('Name').agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        eu_better_jp = ej[ej.EU_Sales > ej.JP_Sales].shape[0]
        return eu_better_jp

    @task()
    def print_data(name_global, genre_eu, name_1_na, pub_jp, eu_better_jp):
        print(f'''Data of game sales for {YEAR} year
                  Best game: {name_global}
                  Best genre in Europe: {genre_eu}
                  Best platform in NA: {name_1_na}
                  Best publisher in JP: {pub_jp}
                  Count games EU more than JP: {eu_better_jp}''')
    
        
    dg = get_data()
    name_global = get_name_global(dg)
    genre_eu = get_genre_eu(dg)
    name_1_na = get_name_1_na(dg)
    pub_jp = get_pub_jp(dg)
    eu_better_jp = get_eu_better_jp(dg)
    print_data(name_global, genre_eu, name_1_na, pub_jp, eu_better_jp)

aer_lesson_3 = aer_lesson_3()


# In[74]:


# dg = pd.read_csv('vgsales.csv')
# YEAR = 1994 + hash(f'a-rajsih') % 23
# dg = dg[dg.Year == YEAR].reset_index()
# dg


# In[75]:


# Какая игра была самой продаваемой в этом году во всем мире?
# dgnw = dg.groupby('Name').agg({'Global_Sales': 'sum'}).reset_index()
# dgnw.Name[dgnw.Global_Sales == dgnw.Global_Sales.max()]


# In[76]:


# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
# dgge = dg.groupby('Genre').agg({'EU_Sales': 'sum'}).reset_index()
# dgge.Genre[dgge.EU_Sales == dgge.EU_Sales.max()]


# In[77]:


# На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
# Перечислить все, если их несколько
# dgnpa = dg.groupby(['Name', 'Platform']).agg({'NA_Sales': 'sum'}).reset_index()
# dgnpa = dgnpa[dgnpa.NA_Sales >= 1]
# dgnpa = dgnpa.groupby('Platform').agg({'NA_Sales': 'sum'}).reset_index()
# dgnpa.Platform[dgnpa.NA_Sales == dgnpa.NA_Sales.max()]


# In[78]:


# У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
# dgpj = dg.groupby('Publisher').agg({'JP_Sales': 'mean'}).reset_index()
# dgpj.Publisher[dgpj.JP_Sales == dgpj.JP_Sales.max()]


# In[79]:


# Сколько игр продались лучше в Европе, чем в Японии?
# ej = dg.groupby('Name').agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
# ej[ej.EU_Sales > ej.JP_Sales].shape[0]

