#!/usr/bin/env python
# coding: utf-8

# In[58]:


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
from airflow.operators.python import get_current_context
import numpy as np
from airflow.models import Variable


# In[76]:


default_args = {
    'owner': 'v-scherbak-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 8),
    'schedule_interval': '0 12 * * *'
}


# In[78]:


CHAT_ID = -5254940365
BOT_TOKEN = Variable.get('telegram_secret')


# In[79]:


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f"Well Done! Dag {dag_id} completed on {date}. It's fine."
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)


# In[84]:


@dag(default_args=default_args, catchup=False)
def lesson3_v_scherbak_20():
    
    @task(retries=3)
    def get_data():
        year = 1994 + hash(f'{"v-scherbak-20"}') % 23
        games = pd.read_csv("vgsales.csv")
        games = games.query("Year == @year")
        return games
    
    @task(retries=4)
    def top_game(games):
        top_game_df = games.groupby("Name").agg({"Global_Sales": "sum"}).idxmax()[0]
        return top_game_df
    
    @task(retries=4)
    def top_genres(games):
        top_genres_df = games.groupby("Genre").agg({"EU_Sales": "sum"}).idxmax()[0]
        return top_genres_df
    
    @task(retries=4)
    def top_platform_na(games):
        top_platform_na_df = games.groupby(["Platform", "Name"]).agg({"NA_Sales": "sum"}).query("NA_Sales > 1").idxmax()[0][0]
        return top_platform_na_df
    
    @task(retries=4)
    def top_publisher_japan(games):
        top_publisher_japan_df = games.groupby("Publisher").agg({"JP_Sales": "mean"}).idxmax()[0]
        return top_publisher_japan_df
    
    @task(retries=4)
    def europe_bigger_japan(games):
        europe_bigger_japan_df = games.groupby("Name").agg({"EU_Sales": "sum", "JP_Sales": "sum"})         .query("EU_Sales > JP_Sales").shape[0]
        return europe_bigger_japan_df

    @task(on_success_callback=send_message)
    def print_data(top_game_df, top_genres_df, top_platform_na_df, top_publisher_japan_df, europe_bigger_japan_df):
        context = get_current_context()
        date = context['ds']
        year = 1994 + hash(f'{"v-scherbak-20"}') % 23

        print(f"The most popular game in {year} is {top_game_df}")
        print(f"The most popular genres in Europe in {year} is {top_genres_df}")
        print(f"The most popular platform with the biggest amount of games which have been sold more than 1 millions copies in {year} in NA is {top_platform_na_df}")
        print(f"The best publisher of mean sales in Japan in {year} is {top_publisher_japan_df}")
        print(f"{europe_bigger_japan_df} were sold more in Europe than in Japan in {year}")
        

    data = get_data()
    world_top_game = top_game(data)
    top_genres_eu = top_genres(data)
    top_platform_na_1m = top_platform_na(data)
    top_publisher_japan_sales = top_publisher_japan(data)
    number_europe_bigger_japan = europe_bigger_japan(data)
    print_data(world_top_game, top_genres_eu, top_platform_na_1m, top_publisher_japan_sales, number_europe_bigger_japan)
    
lesson3_v_scherbak_20 = lesson3_v_scherbak_20()


# In[ ]:




