import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import os
import requests
from io import StringIO
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
path = os.path.join(__location__, 'vgsales.csv')

default_args = {
    'owner': 'e.tokarev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 14)
}

def send_message(context):
    channel_id = '-1001827013885'
    BOT_TOKEN = '5945035852:AAG8KYQZCmtS9EE5LNnYAxuudUesmrkDSFY'
    apiURL = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'

    date = context['ts']
    dag_id = context['dag'].dag_id
    message = f'It is a notification that the DAG named "{dag_id}" succesfully completed on {date}. Skynet wins!'
    response = requests.post(apiURL, json={'chat_id': channel_id, 'text': message})
    print(response.text)

@dag(default_args=default_args, schedule_interval = '0 12 * * *', catchup=False)
def e_tokarev_lesson_03():

    @task()
    def get_data():
        login = 'e-tokarev'
        target_year  = 1994 + hash(f'{login}') % 23
        data_sales = pd.read_csv(path,low_memory=True)
        data_sales.Year = data_sales.Year.fillna(0.0).apply(np.int64) # prepare data
        data_sales = data_sales.query(f'Year == {target_year}')

        return data_sales.to_csv(index=False)


    @task()
    def get_top_game(data_sales):
        #  Какая игра была самой продаваемой в этом году во всем мире?
        top_game_df = pd.read_csv(StringIO(data_sales)).sort_values(by='Global_Sales', ascending=False)
        top_game = top_game_df.head(1).Name.to_list()[0] # sort top game on target year
        top_game_value = top_game_df.head(1).Global_Sales.to_list()[0]  # get the sales value for it
        year  = int(top_game_df.iat[0,3])

        return {'top_game': top_game, 'top_game_value': top_game_value, 'year' : year}

    @task()
    def get_top_eu(data_sales):
        # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
        top_eu_df = pd.read_csv(StringIO(data_sales)).groupby('Genre', as_index=False).agg({'EU_Sales':'sum'})
        top_eu_max_sales = top_eu_df.EU_Sales.max()  # Calculate max value
        top_eu_genre = str(top_eu_df.query(f'EU_Sales == {top_eu_max_sales}').Genre.to_list())[1:-1].replace("'", "") # search genres on max value
        top_eu_g_sales = str(top_eu_df.query(f'EU_Sales == {top_eu_max_sales}').EU_Sales.to_list())[1:-1].replace("'", "")
        
        return {'top_eu_g_sales': top_eu_g_sales, 'top_eu_genre': top_eu_genre}

    @task()
    def get_top_na_platform(data_sales):
        # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
        na_df = pd.read_csv(StringIO(data_sales)).query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'Name':'count'})
        top_na_platform_max = na_df.Name.max()  # Calculate max value
        top_na_platform = str(na_df.query(f'Name == {top_na_platform_max}').Platform.to_list())[1:-1].replace("'", "") # search platform on max value
        top_na_platform_value = str(na_df.query(f'Name == {top_na_platform_max}').Name.to_list())[1:-1].replace("'", "") # search Name on max value
    
        return {'top_na_platform': top_na_platform, 'top_na_platform_value': top_na_platform_value}
       
    @task()
    def get_top_avg_jp_publisher(data_sales):
        # У какого издателя самые высокие средние продажи в Японии? 
        top_avg_jp_df = pd.read_csv(StringIO(data_sales)).groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).sort_values(by='JP_Sales', ascending=False)
        top_avg_jp_publisher_max = top_avg_jp_df.JP_Sales.max() # Calculate max value
        top_avg_jp_publisher = str(top_avg_jp_df.query(f'JP_Sales == {top_avg_jp_publisher_max}').Publisher.to_list())[1:-1].replace("'", "") # search Publisher on max value

        return top_avg_jp_publisher

    @task()
    def eu_jp_comp(data_sales):
        # Сколько игр продались лучше в Европе, чем в Японии? EU_Sales > JP_Sales
        eu_jp_comp = pd.read_csv(StringIO(data_sales)).query('EU_Sales > JP_Sales').shape[0]

        return eu_jp_comp


    @task(on_success_callback=send_message)
    def print_data(top_game_data_stat, top_eu_data_stat, top_na_platform_platform_stat, get_top_avg_jp_publisher_data, eu_jp_comp_data):

        context = get_current_context()
        date = context['ds']

        top_game_name, top_game_sales, year = top_game_data_stat['top_game'], top_game_data_stat['top_game_value'], top_game_data_stat['year']
        top_genre_name, top_genre_sales = top_eu_data_stat['top_eu_genre'], top_eu_data_stat['top_eu_g_sales']
        top_na_platform_name, top_na_platform_sales = top_na_platform_platform_stat['top_na_platform'], top_na_platform_platform_stat['top_na_platform_value']

        print(f''' Дата отчета: {date}''')

        print(f''' Cамая продаваемая игра в {year} году во всем мире : {top_game_name}, продано {top_game_sales} миллионов копий''')

        print(f''' Жанр игр {top_genre_name} был(и) самым(и) продаваемым(и) в Европе в {year} году, продано:  {top_genre_sales} миллионов копий''')

        print(f''' На платформе {top_na_platform_name} было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {year} году, продано:  {top_na_platform_sales} миллионов копий''')   

        print(f''' В {year} году самые высокие средние продажи в Японии были у издателя {get_top_avg_jp_publisher_data}''')

        print(f''' В {year} году {eu_jp_comp_data} игр продались лучше в Европе, чем в Японии ''')
                                                                

    data_sales = get_data()
    top_game_data  = get_top_game(data_sales)
    top_eu_data = get_top_eu(data_sales)
    top_na_platform_platform = get_top_na_platform(data_sales)
    get_top_avg_jp_publisher_data = get_top_avg_jp_publisher(data_sales)
    eu_jp_comp_data = eu_jp_comp(data_sales)
    print_data(top_game_data, top_eu_data, top_na_platform_platform, get_top_avg_jp_publisher_data, eu_jp_comp_data)

e_tokarev_lesson_03 = e_tokarev_lesson_03()
