#!/usr/bin/env python
# coding: utf-8

# In[27]:


import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task


# In[29]:



vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'd.lazarev-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 8),
    'schedule_interval': '0 10 * * *'
}

y = 1994 + hash(f'd.lazarev-23') % 23



# In[31]:


@dag(default_args=default_args, catchup=False)


def d_lazarev_23_dag_lesson3():
    @task()
    def read_data():
        df = pd.read_csv(vgsales)
        return df

    @task()
    def top_name(df):
        return df.query("Year == @y").groupby('Name').sum().Global_Sales.idxmax()

    @task()
    def genre_eu(df):
        return df.query("Year == @y").groupby('Genre').sum().EU_Sales.idxmax()

    @task()
    def platform_na(df):
        return df.query("Year == @y & NA_Sales > 1").groupby('Platform').sum().NA_Sales.idxmax()

    @task()
    def publisher_jp(df):
        return df.query("Year == @y").groupby('Publisher').mean().JP_Sales.idxmax()

    @task()
    def games_eu_vs_jp(df):
        return (df.query("Year == @y").EU_Sales > df.query("Year == @y").JP_Sales).sum()

    @task()
    def print_data(top_name, genre_eu, platform_na, jp_publisher, games_eu_vs_jp):
        print(f'''
            Top sales game worldwide in {y}: {top_name}
            Top genre in EU in {y}: {genre_eu}
            Top platform in North America in {y}: {platform_na}
            Top publisher in Japan in {y}: {publisher_jp}
            Number of Games EU vs. JP in {y}: {games_eu_vs_jp}''')

    df = read_data()

    top_name = top_name(df)
    genre_eu = genre_eu(df)
    platform_na = platform_na(df)
    publisher_jp = publisher_jp(df)
    games_eu_vs_jp = games_eu_vs_jp(df)

    print_data(top_name, genre_eu, platform_na, publisher_jp, games_eu_vs_jp)


d_lazarev_23_dag_lesson3 = d_lazarev_23_dag_lesson3()

