import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'a.sahabutdinov',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 10, 28),
}

CHAT_ID = -620798068
BOT_TOKEN = '5631401758:AAEjPvqwVVrsIBcyxXEv-qx7Y55XhS3voY8'

# 1994 + hash('a-sahabutdinov-25') % 23 = 2015
my_year = 2015
path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

def send_message():
    message = f'Huge success! Dag completed on {datetime.now()}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, message=message)

@dag(default_args=default_args, catchup=False)
def games_2015_sales_analysis_by_Aidar():
    
    # Getting Data
    @task
    def get_data(path):
        vgsales = pd.read_csv(path)

        return vgsales
    
    # 1
    @task
    def most_sold_global_game(vgsales, my_year):
        most_sold_global_game_data = vgsales[(vgsales['Year'] == my_year)].groupby('Name', as_index=False) \
                                                                          .agg({'Global_Sales': 'sum'}) \
                                                                          .sort_values('Global_Sales', ascending=False)
        
        most_sold_global_game_data = most_sold_global_game_data['Name'].values[0]

        return most_sold_global_game_data
    
    # 2
    @task
    def most_sold_genre_EU(vgsales, my_year):
        most_sold_genre_EU_max = vgsales[(vgsales['Year'] == my_year)].groupby('Genre', as_index=False) \
                                                                      .agg({'EU_Sales': 'sum'})['EU_Sales'].max()

        most_sold_genre_EU = vgsales[(vgsales['Year'] == my_year)].groupby('Genre', as_index=False) \
                                                                       .agg({'EU_Sales': 'sum'}) \
                                                                       .sort_values('EU_Sales', ascending=False)

        most_sold_genre_EU_data = most_sold_genre_EU[most_sold_genre_EU['EU_Sales'] == most_sold_genre_EU_max]['Genre'].values

        return most_sold_genre_EU_data
    
    # 3
    @task
    def platform_with_most_NA_games(vgsales, my_year):    
        NA_sales_2015 = vgsales[(vgsales['Year'] == my_year)].groupby('Name', as_index=False) \
                                                             .agg({'NA_Sales': 'sum'})

        NA_games_sold_over_1m = NA_sales_2015[NA_sales_2015['NA_Sales'] > 1]['Name'].values

        NA_most_sold_games_2015_by_platform_max = vgsales[(vgsales['Year'] == my_year) & (vgsales['Name'].isin(NA_games_sold_over_1m))] \
                                                                                           .groupby('Platform', as_index=False) \
                                                                                           .agg({'Name': pd.Series.nunique})['Name'].max()

        NA_most_sold_games_2015_by_platform = vgsales[(vgsales['Year'] == my_year) & (vgsales['Name'].isin(NA_games_sold_over_1m))] \
                                                                                           .groupby('Platform', as_index=False) \
                                                                                           .agg({'Name': pd.Series.nunique})

        platform_with_most_NA_games_data = NA_most_sold_games_2015_by_platform[NA_most_sold_games_2015_by_platform['Name'] == NA_most_sold_games_2015_by_platform_max]['Platform'].values

        return platform_with_most_NA_games_data

    # 4
    @task
    def most_avg_JP_sales(vgsales, my_year):
        most_high_avg_sales_JP_2015_max = vgsales[(vgsales['Year'] == my_year)].groupby('Publisher', as_index=False) \
                                         .agg({'JP_Sales': 'mean'})['JP_Sales'].max()

        most_high_avg_sales_JP_2015 = most_high_avg_sales_JP_2015 = vgsales[(vgsales['Year'] == my_year)] \
                                         .groupby('Publisher', as_index=False) \
                                         .agg({'JP_Sales': 'mean'})

        most_avg_JP_sales_data = most_high_avg_sales_JP_2015[most_high_avg_sales_JP_2015['JP_Sales'] == most_high_avg_sales_JP_2015_max]['Publisher'].values

        return most_avg_JP_sales_data
    
    # 5
    @task
    def games_EU_vs_JP(vgsales, my_year):
        EU_JP_2015_sales = vgsales[(vgsales['Year'] == my_year)].groupby('Name', as_index=False) \
                                                                .agg({'EU_Sales': 'sum',
                                                                      'JP_Sales': 'sum'})

        games_EU_vs_JP_data = len(EU_JP_2015_sales[EU_JP_2015_sales['EU_Sales'] > EU_JP_2015_sales['JP_Sales']])

        return games_EU_vs_JP_data
    
    # Showing result
    @task(on_success_callback=send_message)
    def print_data(task_1, task_2, task_3, task_4, task_5, my_year):
        
        context = get_current_context()
        date = context['ds']
        
        print(f'Data analysis was performed on {date}')
        
        print(f'The most sold game(-s) throughout the World in {my_year}: {task_1}')
        
        print(f'The most sold genre(-s) in EU:')
        print(*task_2)
        
        print(f'Platform(-s) with the most games sold over 1 million in copies in NA in {my_year}:')
        print(*task_3)
        
        print(f'Publisher(-s) with the highest average sales in JP in {my_year}:')
        print(*task_4)
        
        print(f'Number of games sold better in EU than JP in {my_year}: {task_5}')
        
    
    vgsales = get_data(path)
    
    most_sold_global_game_data = most_sold_global_game(vgsales, my_year)
    most_sold_genre_EU_data = most_sold_genre_EU(vgsales, my_year)
    platform_with_most_NA_games_data = platform_with_most_NA_games(vgsales, my_year)
    most_avg_JP_sales_data = most_avg_JP_sales(vgsales, my_year)
    games_EU_vs_JP_data = games_EU_vs_JP(vgsales, my_year)
    
    print_data(most_sold_global_game_data,
               most_sold_genre_EU_data,
               platform_with_most_NA_games_data,
               most_avg_JP_sales_data,
               games_EU_vs_JP_data,
               my_year)
    
games_2015_sales_analysis_by_Aidar = games_2015_sales_analysis_by_Aidar()