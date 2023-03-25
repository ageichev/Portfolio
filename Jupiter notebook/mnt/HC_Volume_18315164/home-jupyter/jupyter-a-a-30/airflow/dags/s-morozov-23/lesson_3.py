import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

path = "/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv"


login = "s-morozov-23"
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 's-morozov-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 24)
}

schedule_interval = '25 22 * * *'

CHAT_ID = 481657437
try:
    BOT_TOKEN = '5300110864:AAETVAu6n2uJ2tZkH3W_b_SF91eSp3xkUoc'
except:
    BOT_TOKEN = ''
                   

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f' Каким то чудом, но этот даг{dag_id} сработал {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False, schedule_interval = schedule_interval)
def s_morozov_23():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(path).query('Year == @year')
        return df.to_csv(index=False)
                   
    
    @task()
    def top_game(df):
        df = pd.read_csv(StringIO(df))
        top_game = df.sort_values("Global_Sales", ascending=False).head(1).Name.values[0]
        return top_game
                   
    @task()
    def top_genre_EU(df):
        df = pd.read_csv(StringIO(df))
        tb = df.groupby("Genre", as_index=False).agg({"EU_Sales" : "sum"})
        EU_Genre = tb[tb.EU_Sales == tb.EU_Sales.max()].Genre.to_list()
        return ",".join(EU_Genre)
                                      
    @task()
    def NA_plat(df):
        df = pd.read_csv(StringIO(df))
        hl = df.query("NA_Sales > 1").groupby("Platform", as_index=False).agg({"Name" : "count"})
        Top_platform = ",".join(hl[hl.Name == hl.Name.max()].Platform.to_list())
        return Top_platform
                   
    @task()
    def AVG_top_jp(df):
        df = pd.read_csv(StringIO(df))
        Top_avg_jp = df.groupby("Publisher", as_index=False) \
                       .agg({"JP_Sales" : "mean"}) \
                       .sort_values("JP_Sales", ascending=False) \
                       .head(1).Publisher.values[0]
        return Top_avg_jp
                   
    @task()
    def EU_better_jp(df):
        df = pd.read_csv(StringIO(df))
        Num = df.query("EU_Sales > JP_Sales").shape[0]
        return Num
     
    @task(on_success_callback=send_message)
    def print_result(top_game,
                    top_genre_EU,
                    NA_plat,
                    AVG_top_jp,
                    EU_better_jp):
        
        context = get_current_context()
                  
        date = context["ds"]
        
                   
        print(f'''Данные на {date} за {year}
        
        Самая продаваемая игра {top_game}
        Лучший(ие) жанр(ы) Европы {top_genre_EU}
        Платформа с наибольшими миллионниками в NA {NA_plat}
        Лучшие средние продажи Японии {AVG_top_jp}
        {EU_better_jp} игр продалось в Европе лучше, чем в Японии''')
                   
                   
    df = get_data()
    top_game = top_game(df)
    top_genre_EU = top_genre_EU(df)
    NA_plat = NA_plat(df)
    AVG_top_jp = AVG_top_jp(df)
    EU_better_jp = EU_better_jp(df)

    print_result(top_game,
                 top_genre_EU,
                 NA_plat,
                 AVG_top_jp,
                 EU_better_jp)
                  
s_morozov_23 = s_morozov_23()