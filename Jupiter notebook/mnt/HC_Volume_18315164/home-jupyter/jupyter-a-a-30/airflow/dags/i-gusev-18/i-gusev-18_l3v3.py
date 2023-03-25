#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task


# In[2]:


PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'i-gusev-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 13),
    'schedule_interval': '0 12 * * *'
}
@dag(default_args=default_args, catchup=False)
def i_gusev_18_les3v2():
    @task(retries=2)
    def get_data():
        path_to_file = PATH
        df = pd.read_csv(path_to_file)
        year_to_analys = 1994 + hash('{}'.format('i-gusev-18')) % 23
        df_year = df[df['Year'] == year_to_analys]
        return df_year

    @task()
    def game_of_year(df_year):
        top_game = df_year             .groupby('Name', as_index=False)             .agg({'Global_Sales': 'sum'})             .sort_values('Global_Sales', ascending=False)             .reset_index().loc[0]['Name']
        return top_game

    @task()
    def top_genre(df_year):
        top_genres = df_year             .groupby('Genre', as_index=False)             .agg({'EU_Sales': 'sum'})             .sort_values('EU_Sales', ascending=False)             .reset_index()
        top_genre = top_genres[top_genres['EU_Sales'] == top_genres.loc[0]['EU_Sales']].loc[0]['Genre']
        return top_genre

    @task()
    def top_1m(df_year):
        platforms_1M = df_year             .groupby('Platform', as_index=False)             .agg({'NA_Sales': 'sum'})             .sort_values('NA_Sales', ascending=False)             .query('NA_Sales >= 1')             .reset_index()
        top_1M_platform = platforms_1M[platforms_1M['NA_Sales'] == platforms_1M.loc[0]['NA_Sales']].loc[0]['Platform']
        return top_1M_platform

    @task()
    def top_JP(df_year):
        top_JP_publishers = df_year             .groupby('Publisher')             .agg({'JP_Sales': 'mean'})             .sort_values('JP_Sales', ascending=False)             .reset_index()
        top_JP_avg = top_JP_publishers[top_JP_publishers['JP_Sales'] == top_JP_publishers.loc[0]['JP_Sales']].loc[0]['Publisher']
        return top_JP_avg

    @task()
    def jp_above_eu(df_year):
        sales_by_year = df_year             .groupby('Name', as_index=False)             .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        jp_above_eu_sales = sales_by_year[sales_by_year['EU_Sales'] > sales_by_year['JP_Sales']]['Name'].count()
        return jp_above_eu_sales

    @task()
    def print_results(top_game, top_genre, top_1M_platform, top_JP_avg, jp_above_eu_sales):
        print('Самой продаваемой в этом году во всем мире игрой стала {}'.format(top_game))
        print('Cамыми продаваемыми в Европе играми стали игры жанра {}'.format(top_genre))
        print('Больше всего игр, которые продались более чем миллионным тиражом в Северной Америке у платформы {}'.format(top_1M_platform))
        print('Самые высокие средние продажи в Японии у издателя {}'.format(top_JP_avg))
        print('{} игр продались в Европе лучше чем в Японии'.format(jp_above_eu_sales))

    df_year = get_data()
    top_game = game_of_year(df_year)
    top_genre = top_genre(df_year)
    top_1M_platform = top_1m(df_year)
    top_JP_avg = top_JP(df_year)
    jp_above_eu_sales = jp_above_eu(df_year)
    print_results(top_game, top_genre, top_1M_platform, top_JP_avg, jp_above_eu_sales)

i_gusev_18_les3v2 = i_gusev_18_les3v2()


# In[ ]:




