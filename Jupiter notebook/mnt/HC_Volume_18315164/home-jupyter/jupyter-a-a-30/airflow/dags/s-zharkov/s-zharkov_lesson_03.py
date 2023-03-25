import pandas as pd
import telegram

from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task




my_year = 1994 + hash(f'{"s-zharkov"}') % 23
default_args = {
    'owner': 's-zharkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 31),
    'schedule_interval': '0 23 * * *'
}



token = '5347788703:AAEHfgFIr3OhoUfS2hD-vUzcySuJVPd4IVc'
chat_id = -656244254

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token = token)
    bot.send_message(chat_id = chat_id, text = message)
    
    
    

@dag(default_args=default_args, catchup=False)
def s_zharkov_les3():
    
    
    @task
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv').query("Year == @my_year")
        return df
    
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task
    def get_most_popular(df):
        game = df.groupby('Name', as_index=False)\
        .agg({'Global_Sales': 'sum'})\
        .sort_values('Global_Sales', ascending=False)\
        .iloc[0, 0]
        return game
    
    
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task 
    def get_most_popular_genre_EU(df):
        genre_sales = df.groupby('Genre')\
        .agg({'EU_Sales': 'sum'})\
        .sort_values('EU_Sales', ascending=False)\
        .head(5)\
        .reset_index()
        return genre_sales
    
    
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    #Перечислить все, если их несколько
    @task 
    def get_most_popular_Platform_NA(df):
        platform_count = df.query('NA_Sales > 1')\
        .groupby('Platform')\
        .agg({'Name': 'count'})\
        .rename(columns={'Name': 'Count'})\
        .sort_values('Count', ascending=False)\
        .reset_index()
        return platform_count
    
    #У какого издателя самые высокие средние продажи в Японии?
    #Перечислить все, если их несколько
    @task
    def get_mean_sales_JP(df):
        meanJPsales = df.groupby('Publisher')\
        .agg({'JP_Sales': 'mean'})\
        .rename(columns={'JP_Sales': 'mean_JP_Sales'})\
        .sort_values('mean_JP_Sales', ascending=False)\
        .head(5)\
        .reset_index()
        return meanJPsales

    #Сколько игр продались лучше в Европе, чем в Японии?
    @task
    def get_JP_or_EU(df):
        eu_sales = df.groupby('Name')\
        .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})\
        .query('EU_Sales > JP_Sales')\
        .shape[0]
        return eu_sales
    
    @task(on_success_callback=send_message)
    def print_data(most_popular, most_popular_genre_EU, most_popular_Platform_NA, mean_sales_JP, JP_or_EU):          
        print(f'The worlds most popular game of {my_year}: {most_popular}')
        print(f'The most popular game genres of {my_year} in Europe:')
        print(most_popular_genre_EU)
        print(f'Most Played Gaming Platforms of {my_year} in North America:')
        print(most_popular_Platform_NA)
        print(f'Highest average sales in Japan:')
        print(mean_sales_JP)
        print(f'{JP_or_EU} games sold better in Europe than in Japan')
        

    df = get_data()       
    most_popular = get_most_popular(df)
    most_popular_genre_EU = get_most_popular_genre_EU(df)
    most_popular_Platform_NA = get_most_popular_Platform_NA(df)
    mean_sales_JP = get_mean_sales_JP(df)
    JP_or_EU = get_JP_or_EU(df)
    print_data(most_popular, most_popular_genre_EU, most_popular_Platform_NA, mean_sales_JP, JP_or_EU)

    
    
    
s_zharkov_les3 = s_zharkov_les3()    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        
