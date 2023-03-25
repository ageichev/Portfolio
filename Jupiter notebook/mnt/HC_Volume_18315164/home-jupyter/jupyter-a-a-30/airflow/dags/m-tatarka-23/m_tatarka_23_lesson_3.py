#%% import
import pandas as pd
import io
import requests
from io import BytesIO, StringIO
from datetime import timedelta, datetime
import telegram

#%%
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


#%%
GAME_SALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
my_data_year = 1994 + hash(f'm-tatarka-23') % 23


#%%


CHAT_ID = 2091429441
try:
    BOT_TOKEN = '5349096206:AAG3YfVcoJD8W6Q3hqJhY5hptVdUPPH9oTY'
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

# %%
default_args = {
    'owner': 'm.tatarka',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 21),
    'schedule_interval': '0 10 * * *'
}
# %%
@dag(default_args=default_args, catchup=False)
def m_tatarka_23_game_sales_lesson_3():
    @task(retries=3)
    def get_game_data():
        #req = requests.get(link)
        # #data = req.content
        # #df = pd.read_csv(BytesIO(data), sep=',')
        game_data = pd.read_csv(GAME_SALES, sep=',')

        
        return game_data.query("Year == @my_data_year").to_csv(index=False)

    @task(retries=4)
    def top_sales_game(game_data):
        game_data = pd.read_csv(StringIO(game_data), sep=",")
        top_sales_game = game_data.groupby('Name', as_index=False)['Global_Sales']\
                .sum()\
                .sort_values( "Global_Sales",ascending=False)\
                .head(1)['Name'].values[0]
        return top_sales_game

    @task(retries=5)
    def  top_genre_game(game_data):
        game_data = pd.read_csv(StringIO(game_data), sep=",")
        df_Genre = game_data\
            .groupby('Genre', as_index=False)['EU_Sales']\
            .sum()\
            .sort_values("EU_Sales", ascending=False)
        top_Genre = list(df_Genre[df_Genre["EU_Sales"] == df_Genre["EU_Sales"][0]]['Genre'])
        return {"Top_Genre":top_Genre}

    @task(retries=6)
    def  top_platform_game(game_data):
        game_data = pd.read_csv(StringIO(game_data), sep=",")
        top_platform = game_data\
            .groupby('Platform', as_index=False)['NA_Sales']\
            .sum()\
            .sort_values("NA_Sales", ascending=False).query("NA_Sales > 1")
        top_platform_list = list(top_platform["Platform"])
        return {"top_platform":top_platform_list}

    @task(retries=7)
    def  top_publisher_jp(game_data):
        game_data = pd.read_csv(StringIO(game_data), sep=",")
        Publisher_avg_max =  game_data\
            .groupby('Publisher', as_index=False)['JP_Sales']\
            .mean()\
            .sort_values("JP_Sales", ascending=False).round(3)
        
        Max_avg_sales_publisher_in_jp = Publisher_avg_max[Publisher_avg_max['JP_Sales'] == Publisher_avg_max.head(1)['JP_Sales'].values[0]]['Publisher'].to_list()
        return {"Max_avg_sales_publisher_in_jp":Max_avg_sales_publisher_in_jp}

    @task(retries=7)
    def sales_eu_jp_equal(game_data):
        game_data = pd.read_csv(StringIO(game_data), sep=",")
        game_sales_eu_jp =  game_data\
            .groupby('Name',as_index=False)\
            .agg({"EU_Sales":"sum", "JP_Sales":"sum"})\
            .apply(lambda x: x['EU_Sales'] > x['JP_Sales'], axis=1).sum()
        return game_sales_eu_jp



    @task(retries=7,on_success_callback=send_message)
    def print_data(top_sales_game, top_Genre, top_Platform, Max_avg_sales_publisher_in_jp, game_sales_eu_jp):
        top_platform = top_Platform['top_platform']
        top_genre = top_Genre['Top_Genre']
        Max_avg_sales_publisher_in_jp = Max_avg_sales_publisher_in_jp['Max_avg_sales_publisher_in_jp']
        print(f'****\nTop sale game in {my_data_year}: {top_sales_game}\n****')
        print(f'****\nTop genre game in {my_data_year}: {top_genre}\n****')
        print(f'****\nPlatforms with over 1 million sales in {my_data_year}: {top_platform}\n****')
        print(f'****\nMax average sales publisher Jp in {my_data_year}: {Max_avg_sales_publisher_in_jp}\n****')
        print(f'****\nHow many games sold better in Europe than in Japan in {my_data_year}: {game_sales_eu_jp}\n****')


    # get data 
    game_data = get_game_data()	
    # tasks
    top_sales_game = top_sales_game(game_data)
    top_genre_game = top_genre_game(game_data)
    top_platform_game = top_platform_game(game_data)
    top_publisher_jp = top_publisher_jp(game_data)
    sales_eu_jp_equal = sales_eu_jp_equal(game_data)
    # print result
    print_data(top_sales_game, top_genre_game, top_platform_game, top_publisher_jp,sales_eu_jp_equal)

m_tatarka_23_game_sales_lesson_3 = m_tatarka_23_game_sales_lesson_3()