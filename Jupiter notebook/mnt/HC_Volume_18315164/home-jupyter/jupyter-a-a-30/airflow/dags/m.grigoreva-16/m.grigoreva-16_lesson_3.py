import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

games_data_url = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'm.grigoreva-16', 
    'depends_on_past': False, 
    'retries': 2, 
    'retry_delay': timedelta(minutes=5), 
    'start_date': datetime(2022, 1, 17), 
    'schedule_interval':'0 12 * * *'
}

#отправка сообщения в телеграм
CHAT_ID = 486183495
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''
    
def send_message(context):
    date = context['ds']
    dag_id = contex['dag'].dag_id
    message = f'Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass
    
@dag(default_args=default_args, catchup=False)
def mgrigoreva_16_games_analytics():
    #Task 1 - Getting data
    @task(retries=3)
    def get_games_data():
        login = 'm.grigoreva-16'
        year = 1994 + hash(f'{login}') % 23

        games_data = pd.read_csv(games_data_url)
        games_data = games_data.query('Year == @year')
        return games_data


    #Task 2 - The game than was sold the most 
    @task(retries=4, retry_delay=timedelta(seconds=10))
    def get_most_sold_game(games_data):
        most_sold_game = games_data.groupby('Name', as_index=False)\
                                   .agg({'Global_Sales':'sum'})\
                                   .sort_values('Global_Sales', ascending=False).reset_index()\
                                   .loc[0, 'Name']
        return most_sold_game

    #Task 3 - Popular genre in Europe
    @task(retries=4, retry_delay=timedelta(seconds=10))
    def get_popular_genre(games_data):
        popular_genre = games_data.groupby('Genre', as_index=False)\
                                  .agg({'EU_Sales':'sum'})\
                                  .sort_values('EU_Sales', ascending=False)

        max_eu_sales = popular_genre.EU_Sales.max()
        popular_genre_list = list(popular_genre.loc[popular_genre.EU_Sales == max_eu_sales]['Genre'])
        return popular_genre_list

    #Task 4 - Most sold platforms in NA
    @task(retries=4, retry_delay=timedelta(seconds=10))
    def get_most_sold_platform_na(games_data):
        most_sold_platform_na = games_data.query('NA_Sales > 1')\
                                      .groupby('Platform', as_index=False)\
                                      .agg({'Name':'count'})\
                                      .rename(columns={'Name':'Number_of_games'})\
                                      .sort_values('Number_of_games', ascending=False)
        most_sold_platform_na_list = list(most_sold_platform_na[most_sold_platform_na.Number_of_games == most_sold_platform_na.Number_of_games.max()].Platform)
        return most_sold_platform_na_list

    #Task 5 - Publisher with highest avg. sales in Japan
    @task(retries=4, retry_delay=timedelta(seconds=10))
    def get_highest_avg_sales_publisher_jp(games_data):
        highest_avg_sales_jp = games_data.groupby('Publisher', as_index=False)\
                                         .agg({'JP_Sales':'mean'})\
                                         .sort_values('JP_Sales', ascending=False)
        max_avg_sales_jp = highest_avg_sales_jp.JP_Sales.max()
        max_avg_sales_jp_list = list(highest_avg_sales_jp[highest_avg_sales_jp.JP_Sales == max_avg_sales_jp]['Publisher'])
        return max_avg_sales_jp_list


    #Task 6 - Games that sold better in EU then in JP
    @task(retries=4, retry_delay=timedelta(seconds=10))
    def get_eu_games(games_data):
        sales_jp_eu = games_data.groupby('Name', as_index=False)\
                                .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        sales_jp_eu['better_in_EU'] = sales_jp_eu.EU_Sales > sales_jp_eu.JP_Sales
        eu_games = sales_jp_eu.better_in_EU.sum()
        return eu_games

    #Task 7 - Print the data 
    @task(on_success_callback=send_message)
    def print_data(most_sold_game, popular_genre_list, most_sold_platform_na_list, max_avg_sales_jp_list, eu_games):
        login = 'm.grigoreva-16'
        year = 1994 + hash(f'{login}') % 23

        print(f'''Data for year {year}:
                  1. The game that was sold the most in the world: {most_sold_game}.
                  2. Most sold genres in Europe: {popular_genre_list}. 
                  3. Platforms that have maximum amount of games with more than 1M sales in North America: {most_sold_platform_na_list}. 
                  4. Publishers than have highest average sales in Japan: {max_avg_sales_jp_list}.
                  5. Number of games that have been sold better in Europe than in Japan {eu_games}.''')
        
    
    #последовательность запусков    
    games_data = get_games_data()
    
    most_sold_game = get_most_sold_game(games_data)
    popular_genres = get_popular_genre(games_data)
    most_sold_platforms_na = get_most_sold_platform_na(games_data)
    max_avg_sales_jp = get_highest_avg_sales_publisher_jp(games_data)
    eu_games = get_eu_games(games_data)
    
    print_data(most_sold_game, popular_genres, most_sold_platforms_na, max_avg_sales_jp, eu_games)
    
    #запуск дага 
    mgrigoreva_16_games_analytics = mgrigoreva_16_games_analytics()

   