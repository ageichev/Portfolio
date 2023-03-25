import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

vgsales = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'


default_args = {
    'owner': 'm-ajvazjan-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 7),
    'schedule_interval': '0 15 * * *'
}

y = 1994 + hash(f'm-ajvazjan-23') % 23 #выбираем год, за который будем считывать данные 

CHAT_ID =  -620483392
BOT_TOKEN = '5132002536:AAGXuAWYxE3cSvpim9trM7VmrQhCLnoHE_0'


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)          #наш даг
def m_ajvazjan_23_airflow_3():
    @task(retries=3)                                    
    def get_data():                                   
        data = pd.read_csv(vgsales)
        df = data[data.Year == y]
        return df
    
  #найдем самую продаваемую игру в мире 
    @task()
    def most_sold_global(df):
        top_global = df[df.Global_Sales == df.Global_Sales.max()].iloc[0].Name
        return top_global
    
  #самые продаваемые жанры игр в Европе 
    @task()
    def most_sold_EU(df):
        top_EU = df.groupby('Genre', as_index=False)\
                           .agg({'EU_Sales':'sum'})\
                           .sort_values('EU_Sales', ascending=False)\
                           .head(1).iloc[0].Genre
        return top_EU
    
  #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task()
    def most_sold_NA(df):
        top_na = df.query('NA_Sales > 1')\
                   .groupby('Platform', as_index=False)\
                   .agg({'NA_Sales':'sum'})\
                   .sort_values('NA_Sales', ascending=False)
        top_NA = ', '.join(top_na.Platform.tolist())
        return top_NA
    
   #издатель с самыми высокими средними продажами в Японии?
    @task()
    def mean_sales_JP(df):
        mean_JP = df.groupby('Publisher', as_index=False)\
                    .agg({'JP_Sales':'mean'})\
                    .sort_values('JP_Sales', ascending=False)\
                    .head(1).iloc[0].Publisher
        return mean_JP
    
   #Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def EU_JP(df):
        eu_jp = df.groupby('Name',as_index=False)\
                  .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})
        number_of_games = eu_jp[eu_jp.EU_Sales > eu_jp.JP_Sales].shape[0]
        return number_of_games
    
    @task(on_success_callback=send_message)
    def print_data(top_global, top_EU, top_NA, mean_JP, number_of_games):
        
        context = get_current_context()
        date = context['ds']
        
        print(f'Game sales analysis on {date}')
        
        print(f'''The best-selling game in the world in {y} is {top_global}
              The best-selling genre in Europe in {y} is {top_EU}
              Top platform with over million sales in NA in {y} is {top_NA}
              Publishers in Japan (avg sales) in {y} are: {mean_JP}
              The number of games sold better in Europe than in Japan in {y}: {number_of_games}''')
       
    df = get_data()

    top_global = most_sold_global(df)
    top_EU = most_sold_EU(df)
    top_NA = most_sold_NA(df)
    mean_JP = mean_sales_JP(df)
    number_of_games = EU_JP(df)
    
    print_data(top_global, top_EU, top_NA, mean_JP, number_of_games)
    

m_ajvazjan_23_airflow_3 = m_ajvazjan_23_airflow_3()
