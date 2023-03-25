#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

from airflow.decorators import dag, task

from datetime import timedelta, datetime


# In[3]:


default_args = {
    'owner': 'v.tjavin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 17),
    'schedule_interval': '0 12 * * *'}


login = 'v-tjavin'
year = 1994 + hash(f'{login}') % 23

# In[4]:


@dag(default_args = default_args, schedule_interval = '0 12 * * *', catchup = False)
def airflow_v_tjavin():
    @task
    def get_data():
        path =  '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        games_data = pd.read_csv(path).query('Year == @year')
        return games_data
    
    #Какая игра была самой продаваемой в этом году во всем мире?
    @task
    def game_top_1_GS(games_data):
        top_1_GS_game_data = games_data.groupby('Name', as_index=False)                                     .agg({'Global_Sales': 'sum'})                                     .sort_values('Global_Sales', ascending=False)
        maximum = top_1_GS_game_data.Global_Sales.max()
        top_1_GS_game = top_1_GS_game_data.loc[top_1_GS_game_data.Global_Sales == maximum].Name.tolist()
        res = {39: None, 91: None, 93: None}
        top_1_GS_game = str(top_1_GS_game).translate(res)
        return top_1_GS_game
    
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task
    def genre_top_EU(games_data):
        top_EU_genre_data = games_data.groupby('Genre', as_index=False)                                     .agg({'EU_Sales': 'sum'})                                     .sort_values('EU_Sales', ascending=False)
        maximum = top_EU_genre_data.EU_Sales.max()
        top_EU_genre = top_EU_genre_data.loc[top_EU_genre_data.EU_Sales == maximum].Genre.tolist()
        res = {39: None, 91: None, 93: None}
        top_EU_genre = str(top_EU_genre).translate(res)
        return top_EU_genre
    
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task
    def platform_top_NA(games_data):
        top_NA_platform_data = games_data.query('NA_Sales > 1')             .groupby('Platform', as_index=False)             .agg({'Name': 'nunique'})             .sort_values('Name', ascending=False)
        maximum = top_NA_platform_data.Name.max()
        top_NA_platform = top_NA_platform_data.loc[top_NA_platform_data.Name == maximum].Platform.tolist()
        res = {39: None, 91: None, 93: None}
        top_NA_platform = str(top_NA_platform).translate(res)
        return top_NA_platform
    
    #У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task
    def JP_top_mean(games_data):
        top_mean_JP_data = games_data.groupby('Publisher', as_index=False)             .agg({'JP_Sales': 'mean'})             .sort_values('JP_Sales', ascending=False)
        maximum = top_mean_JP_data.JP_Sales.max()
        top_mean_JP = top_mean_JP_data.loc[top_mean_JP_data.JP_Sales == maximum].Publisher.tolist()
        res = {39: None, 91: None, 93: None}
        top_mean_JP = str(top_mean_JP).translate(res)
        return top_mean_JP
    
    #Сколько игр продались лучше в Европе, чем в Японии?
    @task
    def EU_vs_JP_top_game(games_data):
        top_game_EU_vs_JP_data = games_data.query('EU_Sales > JP_Sales')             .groupby('Name', as_index=False)             .agg({'Publisher': 'count'})             .sort_values('Publisher', ascending=False)
        top_game_EU_vs_JP = top_game_EU_vs_JP_data.Name.count()
        res = {39: None, 91: None, 93: None}
        top_game_EU_vs_JP = str(top_game_EU_vs_JP).translate(res)
        return top_game_EU_vs_JP
    
    @task
    def print_data(top_1_GS_game, top_EU_genre, top_NA_platform, top_mean_JP, top_game_EU_vs_JP):
        print(f'Самая продаваемой игрой во всем мире за {year} год, была {top_1_GS_game}.')
        print(f'Самые продаваемые жанры в Европе за {year} год: {top_EU_genre}.')
        print(f'Самые популярная платформа с более чем миллионым тиражом в Северной америке за {year} год: {top_NA_platform}.')
        print(f'Самые высокие средние продажи в Японии за {year} год пришлись на {top_mean_JP}.')
        print(f'{top_game_EU_vs_JP} игр продались лучше в Европе, чем в Японии за {year}.')

    games_data = get_data()
    top_1_GS_game = game_top_1_GS(games_data)
    top_EU_genre = genre_top_EU(games_data)
    top_NA_platform = platform_top_NA(games_data)
    top_mean_JP  = JP_top_mean(games_data)
    top_game_EU_vs_JP  = EU_vs_JP_top_game(games_data)
    print_data(top_1_GS_game, top_EU_genre, top_NA_platform, top_mean_JP, top_game_EU_vs_JP)

airflow_v_tjavin = airflow_v_tjavin()
    


# In[ ]:




