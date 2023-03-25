import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import requests
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

data_link = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
data_file = 'vgsales.csv'
login='i-grebenjuk-23'
my_year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'i-grebenjuk-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 2),
}


#CHAT_ID = -747944099

#try:
#    BOT_TOKEN = Variable.get('telegram_secret')
#except:
#    BOT_TOKEN = ''
    
#def send_message(context):
#    date = context['ds']
#    dag_id = context['dag'].dag_id
#    message = f'Huge success! Dag {dag_id} completed on {date}'
#    if BOT_TOKEN != '':
#        bot = telegram.Bot(token=BOT_TOKEN)
#        bot.send_message(chat_id=CHAT_ID, message=message)
#    else:
#        pass

@dag(default_args=default_args, catchup=False)
def lesson_3_IG_airflow_2():
    # 1. Read and filter data as per the condition
    @task(retries=3)    
    def get_and_add_data():
        df = pd.read_csv(data_link) # Считали данные
        df = df.query('Year==@my_year')
        return df

    # 2. Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def most_sold_game(df):
        max_sales = df.Global_Sales.max()
        the_most_sold_game_year = df.query('Global_Sales==@max_sales').Name.values[0]
        return the_most_sold_game_year

    # 3. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    #
    # Из условия непонятно, надо ли перечислять все игры самого популярного жанры или 
    # все самые популярные жанры с одинаковыми продажами
    # Поэтому, поскольку вероятность того, что продажи по разным жанрам совпадут маленькая, 
    # то ниже перечислены все игры одного самого популярного жанра
    @task()
    def most_popular_genre_europe(df):
        most_popular_genre_EU = df.groupby('Genre',as_index=False).agg({'EU_Sales':'sum'}).sort_values('EU_Sales',ascending=False).head(1).Genre.values[0]                
        list_games_EU = df.query('Genre == @most_popular_genre_EU & EU_Sales!=0').Name.unique()
        return {'most_popular_genre_EU': most_popular_genre_EU, 'list_games_EU':list_games_EU}

    # 4. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    # Перечислить все, если их несколько
    #
    # Аналогично задаче 2 неоднозначное условие: поэтому перечисляем все игры платформы, на которой было больше всего
    # игр "носорогов" с тиражами более миллиона штук
    @task()
    def games_best_platform_NA(df):
        NA_top_platform = df.groupby(['Platform','Name'],as_index=False).agg({'NA_Sales':'sum'}).query('NA_Sales>1.0').groupby('Platform',as_index=False).agg({'Name':'count'}).sort_values('Name',ascending=False).head(1).Platform.values.tolist()[0]
        rhino_games_NA_top_platform = df.query('Platform==@NA_top_platform').groupby('Name', as_index=False)                                         .agg({'NA_Sales':'sum'}).query('NA_Sales>1.0').Name.unique()
        return {'NA_top_platform':NA_top_platform, 'rhino_games_NA_top_platform':rhino_games_NA_top_platform}

    # 5. У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    # Находим самые высокие продажи и отбираем всех издателей, у которых продажи равны самым высоким продажам
    @task()
    def best_publishers(df):
        max_avg_sales_JP =  df.query('JP_Sales!=0').groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'})                                   .sort_values('JP_Sales', ascending=False).JP_Sales.head(1).values.tolist()[0]
        success_publishers = df.query('JP_Sales!=0').groupby('Publisher',as_index=False).agg({'JP_Sales':'mean'}).sort_values('JP_Sales',ascending=False).query('JP_Sales==@max_avg_sales_JP').Publisher.values.tolist()
        return {'max_avg_sales_JP':max_avg_sales_JP, 'success_publishers':success_publishers}

    # 6. Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def N_games(df):
        df_EU = df.query('EU_Sales!=0').groupby('Name', as_index=False).agg({'EU_Sales':'sum'})
        df_JP = df.query('JP_Sales!=0').groupby('Name', as_index=False).agg({'JP_Sales':'sum'})
        df_JP_EU = df_EU.merge(df_JP,on='Name', how='outer').fillna(0)
        N_games_EU_more_JP = sum(df_JP_EU.EU_Sales>df_JP_EU.JP_Sales)
        return N_games_EU_more_JP

    # 7. All prints
    @task()
    def all_prints(the_most_sold_game_year, 
                   popular_genre_EU, 
                   top_platform_NA, 
                   best_publishers_JP, 
                   N_games_EU_more_JP):
        print(f'The most sold game in the world in {my_year} was "{the_most_sold_game_year}"')
        
        most_popular_genre_EU, list_games_EU = popular_genre_EU['most_popular_genre_EU'], popular_genre_EU['list_games_EU']
        print(f'The list of games of the most popular genre - "{most_popular_genre_EU}" in Europe in {my_year}:')
        for x in list_games_EU.tolist():
            print(x)
        
        NA_top_platform, rhino_games_NA_top_platform = top_platform_NA['NA_top_platform'], top_platform_NA['rhino_games_NA_top_platform']
        print(f'The platform with biggest number of games exceeding 1+ Mln sold items is {NA_top_platform}')
        print(f'The list of those games include:')
        for x in rhino_games_NA_top_platform.tolist():
            print(x)

        max_avg_sales_JP, success_publishers = best_publishers_JP['max_avg_sales_JP'], best_publishers_JP['success_publishers']
        print(f'List of Publishers with the highest equal average sales {max_avg_sales_JP} in {my_year}:')
        for x in success_publishers:
            print(x)

        print(f'The number of games in Europe sold more than in Japan is equal to {N_games_EU_more_JP} in {my_year} year')
    
    df = get_and_add_data()
    the_most_sold_game_year = most_sold_game(df)
    popular_genre_EU = most_popular_genre_europe(df)
    top_platform_NA = games_best_platform_NA(df)
    best_publishers_JP = best_publishers(df)
    N_games_EU_more_JP = N_games(df)
    
    all_prints(the_most_sold_game_year, 
                   popular_genre_EU,
                   top_platform_NA, 
                   best_publishers_JP, 
                   N_games_EU_more_JP)
    
lesson_3_IG_airflow_2 = lesson_3_IG_airflow_2()

