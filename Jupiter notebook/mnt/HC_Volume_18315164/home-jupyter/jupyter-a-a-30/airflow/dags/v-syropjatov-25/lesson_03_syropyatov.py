import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import numpy as np


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

games_path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
games_file = 'vgsales.csv'

default_args = {
    'owner': 'v.syropjatov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 31),
    'schedule_interval': '0 14 * * *'
}

# CHAT_ID = -620798068
# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''

# def send_message(context):
#     date = context['ds']
#     dag_id = context['dag'].dag_id
#     message = f'Huge success! Dag {dag_id} completed on {date}'
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass

@dag(default_args=default_args, catchup=False)
def v_syropyatov_air2():
    @task(retries=3)
    def get_data(games_path):
        games_df = pd.read_csv(games_path)
        return games_df

    @task(retries=4, retry_delay=timedelta(10))
    def get_best_sales_game(games_path):
        games_df = pd.read_csv(games_path)
        games_df = games_df[games_df.Year.notna()]
        games_df['Year'] = games_df['Year'].astype(pd.np.int64)
        ff = 'v-syropjatov-25'
        x = 1994 + hash(f'{ff}') % 23
        games_df_gr = games_df[games_df.Year == x]
        games_df_gr = games_df_gr.groupby('Name',as_index=False).Global_Sales.sum()\
        .sort_values('Global_Sales',ascending=False).reset_index()
        games_df_gr = games_df_gr.drop('index', axis=1).head(1)
        games_df_gr = games_df_gr.Name
        return games_df_gr.to_csv(index=False, header=False)
 

    @task()
    def get_best_genre(games_path):
        games_df = pd.read_csv(games_path)
        games_df = games_df[games_df.Year.notna()]
        games_df['Year'] = games_df['Year'].astype(pd.np.int64)
        ff = 'v-syropjatov-25'
        x = 1994 + hash(f'{ff}') % 23
        games_df_gr = games_df[games_df.Year == x]
        games_df_gr = games_df_gr.groupby(['Name','Genre'],as_index=False).EU_Sales.sum()\
        .sort_values('EU_Sales',ascending=False).reset_index()
        games_df_gr = games_df_gr.drop('index', axis=1).head(10)
        games_best_genre = games_df_gr[games_df_gr.EU_Sales == games_df_gr.EU_Sales.max()]
        return games_best_genre.to_csv(index=False, header=False)
    
    @task()
    def get_platfroms_1mln(games_path):
        games_df = pd.read_csv(games_path)
        games_df = games_df[games_df.Year.notna()]
        games_df['Year'] = games_df['Year'].astype(pd.np.int64)
        ff = 'v-syropjatov-25'
        x = 1994 + hash(f'{ff}') % 23
        games_df_gr = games_df[games_df.Year == x]
        games_df_gr = games_df_gr.groupby(['Name', 'Platform'],as_index=False).NA_Sales.sum()\
        .sort_values('NA_Sales',ascending=False).reset_index()
        games_df_gr = games_df_gr.drop('index', axis=1)
        games_platfrom_more_1mln = games_df_gr[games_df_gr.NA_Sales > 1]
        games_platfrom_more_1mln = games_platfrom_more_1mln.groupby('Platform',as_index=False).Name.nunique()\
        .sort_values('Name',ascending=False).reset_index()
        games_platfrom_more_1mln_top = games_platfrom_more_1mln[games_platfrom_more_1mln.Name == games_platfrom_more_1mln.Name.max()]
        games_platfrom_more_1mln_top = games_platfrom_more_1mln_top.drop('index', axis=1) 
        return games_platfrom_more_1mln_top.to_csv(index=False, header = False)

    @task()
    def games_publisher_avg_Sales(games_path):
        games_df = pd.read_csv(games_path)
        games_df = games_df[games_df.Year.notna()]
        games_df['Year'] = games_df['Year'].astype(pd.np.int64)
        ff = 'v-syropjatov-25'
        x = 1994 + hash(f'{ff}') % 23
        games_df_gr = games_df[games_df.Year == x]
        games_df_gr = games_df_gr.groupby('Publisher',as_index=False).JP_Sales.mean()\
        .sort_values('JP_Sales',ascending=False).reset_index()
        games_df_gr = games_df_gr.drop('index', axis=1)
        games_publisher_avg_Sales = games_df_gr[games_df_gr.JP_Sales == games_df_gr.JP_Sales.max()]
        games_publisher_avg_Sales = games_publisher_avg_Sales.Publisher
#         return {'games_publisher_avg_Sales':games_publisher_avg_Sales}
        return games_publisher_avg_Sales.to_csv(index=False, header = False)
    
    @task()
    def games_EU_more_JP_quant(games_path):
        games_df = pd.read_csv(games_path)
        games_df = games_df[games_df.Year.notna()]
        games_df['Year'] = games_df['Year'].astype(pd.np.int64)
        ff = 'v-syropjatov-25'
        x = int( 1994 + hash(f'{ff}') % 23)
        games_df_gr = games_df[games_df.Year == x]
        games_df_gr = games_df_gr.groupby('Name',as_index=False).agg({'JP_Sales':'sum','EU_Sales':'sum'})\
        .sort_values('EU_Sales',ascending=False).reset_index()
        games_df_gr = games_df_gr.drop('index', axis=1)
        games_df_gr['diff_EU_JP'] = games_df_gr.EU_Sales - games_df_gr.JP_Sales
        games_EU_more_JP = games_df_gr[games_df_gr.diff_EU_JP >0].sort_values('diff_EU_JP', ascending=False)
        games_EU_more_JP_quant = int(games_EU_more_JP.Name.nunique())
        return {'games_EU_more_JP_quant':games_EU_more_JP_quant,'x': x}

    @task()
    def print_data(games_df_gr,games_best_genre,games_platfrom_more_1mln_top,games_publisher_avg_Sales,x_stat):

        context = get_current_context()
        date = context['ds']

        games_EU_more_JP_quant, x = x_stat['games_EU_more_JP_quant'], x_stat['x']


        print(f'''Game with the best sales in {x} year is ''')
        print(games_df_gr)
        
        print(f'''Genre with the best sales in Europe in {x} year is ''')
        print(games_best_genre)
        
        print(f'''Platfroms with the most quantity of games with more than 1 mln Sales in NorthAmerica in {x} year are ''')
        print(games_platfrom_more_1mln_top)
        
        print(f'''The publishers with the biggest average Sales in Japan in {x} year are ''')
        print(games_publisher_avg_Sales)

        print(f'''Quantity of games with better sales in Europe than in Japan in {x} year is {games_EU_more_JP_quant}''')
      
    
    games_df = get_data(games_path)
    games_df_gr = get_best_sales_game(games_path)
    games_best_genre = get_best_genre(games_path)
    games_platfrom_more_1mln_top = get_platfroms_1mln(games_path)
    games_publisher_avg_Sales = games_publisher_avg_Sales(games_path)
    x_stat = games_EU_more_JP_quant(games_path)
    
    
    print_data(games_df_gr,games_best_genre,games_platfrom_more_1mln_top,games_publisher_avg_Sales,x_stat)
    
v_syropyatov_air2 = v_syropyatov_air2()
