import pandas as pd
import numpy as np

from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

import telegram 

random_year = 1994 + hash(f'p-kulakova-18') % 23

default_args = {
    'owner': 'p-kulakova-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 17),
    'schedule_interval': '0 9 * * *'
}

CHAT_ID = -776855283
BOT_TOKEN = Variable.get('telegram_secret')

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Success! Dag {dag_id} is completed on {date}'
    if CHAT_ID != 0:
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass


@dag(default_args=default_args, catchup=False)
def dag_lesson3_p_kulakova_18():
        @task()
        def get_data():
            df1 = pd.read_csv('vgsales.csv').query('Year == @random_year')
            return df1
        
        
        @task()
        def get_most_sold_game_Global(df1):
            most_sold_game_table = df1.groupby('Name').agg({'Global_Sales':'sum'}).sort_values('Global_Sales', ascending=False).reset_index()
            most_sold_game_Global = most_sold_game_table['Name'].iloc[0]
            return most_sold_game_Global
        
        
        @task()
        def get_most_sold_genre_EU(df1):
            most_sold_genre_EU_table = df1.groupby('Genre').agg({'EU_Sales':'sum'}).sort_values('EU_Sales', ascending=False).reset_index()
            str_most_sold_genre_EU = most_sold_genre_EU_table[most_sold_genre_EU_table['EU_Sales']==most_sold_genre_EU_table['EU_Sales'].max()]
            most_sold_genre_EU = str_most_sold_genre_EU['Genre'].iloc[0]
            return most_sold_genre_EU
        
        
        @task()
        def get_NA_Games(df1):
            NA_Table = df1.groupby('Platform').agg({'NA_Sales':'sum'}).sort_values('NA_Sales', ascending=False).reset_index()
            NA_Games = NA_Table.query('NA_Sales > 100').Platform.to_string(index=False)
            if NA_Games == 'Series([], )':
                return 'Games that have sold over a million copies in North America are missing'
            else:
                return NA_Games
        
        
        @task()
        def get_Top_Publisher_JP(df1):
            JP_Table = df1.groupby('Publisher').agg({'JP_Sales':'sum'}).sort_values('JP_Sales', ascending=False).reset_index()
            Top_Publisher_JP = JP_Table['Publisher'].iloc[0]
            return Top_Publisher_JP
        
        
        @task()
        def get_EU_JP(df1):
            EU = df1.groupby('Name').agg({'EU_Sales':'sum'}).sort_values('EU_Sales', ascending=False).reset_index()
            JP = df1.groupby('Name').agg({'JP_Sales':'sum'}).sort_values('JP_Sales', ascending=False).reset_index()
            EU_JP = EU.merge(JP, how = 'left')
            EU_JP['diff'] = EU_JP.EU_Sales - EU_JP.JP_Sales
            EU_JP_final = EU_JP.query('diff > 0').count()
            EU_JP_final_2 = EU_JP_final['Name']
            return EU_JP_final_2
        
        
        @task(on_success_callback = send_message)
        def print_data(most_sold_game_Global, most_sold_genre_EU, NA_Games, Top_Publisher_JP, EU_JP_final_2):
            context = get_current_context()
            date = context['ds']
            print(f'''Data for {random_year} for {date}
                    Bestseller Global Game: {most_sold_game_Global}
                    Top EU Genres: {most_sold_genre_EU}
                    Top NA Platforms: {NA_Games}
                    Top JP Publishers: {Top_Publisher_JP}
                    The number of games with sales in the EU is greater than in JP: {EU_JP_final_2}''')
        
        df1 = get_data()
        
        most_sold_game_Global = get_most_sold_game_Global(df1)
        most_sold_genre_EU = get_most_sold_genre_EU(df1)
        NA_Games = get_NA_Games(df1)
        Top_Publisher_JP = get_Top_Publisher_JP(df1)
        EU_JP_final_2 = get_EU_JP(df1)
        
        print_data(most_sold_game_Global, most_sold_genre_EU, NA_Games, Top_Publisher_JP, EU_JP_final_2)

dag_lesson3_p_kulakova_18 = dag_lesson3_p_kulakova_18()





