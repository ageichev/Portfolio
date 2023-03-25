import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'd-soziashvili-22'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'd-soziashvili-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
    'schedule_interval': '0 17 * * *'
}

CHAT_ID = 1858821652
try:
    BOT_TOKEN = Variable.get('telegram_secret')
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
    
@dag(default_args=default_args, catchup=False)
def d_soziashvili_airflow_hw_3():
    
    @task(retries=3)
    def get_data():
        sales_data = pd.read_csv(path)
        
        return sales_data.query('Year == @year')
    
    @task()
    def get_best_selling_game(sales_data):

        return sales_data.iloc[sales_data.Global_Sales.idxmax()].Name  
    
    @task()
    def get_best_selling_genre_EU(sales_data):  
        sales_by_genre = sales_data.groupby('Genre', as_index=False).EU_Sales.sum().sort_values('EU_Sales', ascending=False)
        max_sales = sales_by_genre.head(1).EU_Sales.values[0]
        
        return sales_by_genre.loc[sales_by_genre.EU_Sales == max_sales].Genre 
    
    @task()
    def get_most_1M_selling_platform_NA(sales_data):
        over_1M_selling = sales_data.query('NA_Sales > 1').groupby('Platform', as_index=False).NA_Sales.count().sort_values('NA_Sales', ascending=False)
        max_count = over_1M_selling.head(1).NA_Sales.values[0]
        
        return over_1M_selling.loc[over_1M_selling.NA_Sales == max_count].Platform
        
    @task()
    def get_avg_selling_publisher_JP(sales_data):       
        avg_sales_by_publisher = sales_data.groupby('Publisher', as_index=False).JP_Sales.mean().sort_values('JP_Sales', ascending=False)
        max_avg = avg_sales_by_publisher.head(1).JP_Sales.values[0]
        
        return avg_sales_by_publisher.loc[avg_sales_by_publisher.JP_Sales == max_avg].Publisher 
        
    @task()
    def get_EU_more_JP_seller(sales_data):
        
        return sales_data.query('EU_Sales > JP_Sales').Name.nunique()
    
    @task(on_success_callback=send_message)
    def print_data(best_seller, best_selling_genre, most_1M_selling_platform, avg_selling_publisher, EU_more_JP_seller):
        context = get_current_context()
        date = context['ds']
        
        best_selling_genre = best_selling_genre.values[len(best_selling_genre) - 1]
        most_1M_selling_platform = most_1M_selling_platform.values[len(most_1M_selling_platform) - 1]
        avg_selling_publisher = avg_selling_publisher.values[len(avg_selling_publisher) - 1]
        
        print(f'''Now {date}''')
        print(f'''Game sales statistics for {year}''')
        print(f'''The best selling game in the world: {best_seller}
                  Best-selling game genre in Europe: {best_selling_genre}
                  The platform with the most games that have sold over a 1M units in NA: {most_1M_selling_platform}
                  Publisher with highest average sales in Japan: {avg_selling_publisher}
                  Games sold in Europe are better than in Japan: {EU_more_JP_seller}''')

    sales_data = get_data()
    
    best_seller = get_best_selling_game(sales_data)
    best_selling_genre = get_best_selling_genre_EU(sales_data)
    most_1M_selling_platform = get_most_1M_selling_platform_NA(sales_data)
    avg_selling_publisher = get_avg_selling_publisher_JP(sales_data)
    EU_more_JP_seller = get_EU_more_JP_seller(sales_data)
    
    print_data(best_seller, best_selling_genre, most_1M_selling_platform, avg_selling_publisher, EU_more_JP_seller)

d_soziashvili_airflow_hw_3 = d_soziashvili_airflow_hw_3()