import requests
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


CHAT_ID = 5170386396  
BOT_TOKEN ='5451757443:AAE-2lK4x_TNg7H4CRJja7jM8nvdPM8_uaQ'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=message)


link ='/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login='a-nikonova-24'
year = 1994 + hash(f'{login}')%23

default_args = {
    'owner': 'a-nikonova-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 19),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def a_nikonova_24_lesson_3():
    
    @task()
    def get_data():
        vgsales = pd.read_csv(link)
        sales_year = vgsales[vgsales['Year']==year]
        return sales_year
    
    @task() # Какая игра была самой продаваемой в этом году во всем мире?
    def get_the_best_selling_game(sales_year):
        the_best_selling_game = sales_year[sales_year['Global_Sales'] == sales_year['Global_Sales'].max()]['Name']
        return the_best_selling_game.to_csv(index=False, header=False)
    
    @task() # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def get_popular_eu_genres(sales_year):
        popular_genres = sales_year.groupby('Genre',as_index = False).agg({'EU_Sales':'sum'})
        the_most_popular_genres = popular_genres[popular_genres['EU_Sales']==popular_genres['EU_Sales'].max()]['Genre']
        return the_most_popular_genres.to_csv(index=False, header=False)  
    
    @task() # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def get_popular_na_platform(sales_year):
        platforms = sales_year.query('NA_Sales > 1')\
                              .groupby('Platform', as_index = False)\
                              .agg({'Name':'count'})\
                              .rename(columns ={'Name':'games_number'})
        popular_platforms = platforms[platforms['games_number']== platforms['games_number'].max()]['Platform']
        return popular_platforms.to_csv(index=False, header=False)  
    
    @task() # У какого издателя самые высокие средние продажи в Японии?
    def get_the_best_jp_publisher(sales_year):
        jp_publisher = sales_year.groupby('Publisher',as_index = False)\
                                 .agg({'JP_Sales':'mean'})\
                                 .rename(columns={'JP_Sales':'JP_AVG_Sales'})
        the_best_jp_publisher = jp_publisher[jp_publisher['JP_AVG_Sales']== jp_publisher['JP_AVG_Sales'].max()]['Publisher']
        return the_best_jp_publisher.to_csv(index=False, header=False) 
    
    @task() # Сколько игр продались лучше в Европе, чем в Японии?
    def get_popular_eu_games_number(sales_year):
        popular_eu_games_number = sales_year.query('EU_Sales > JP_Sales').shape[0]
        return popular_eu_games_number
    
    @task(on_success_callback=send_message)
    def print_data(the_best_selling_game, the_most_popular_genres, popular_platforms, the_best_jp_publisher, popular_eu_games_number):
        context = get_current_context()
        date = context['ds']

        print(f'''Year {year}:\n
                  The best selling game in the world: {the_best_selling_game} \n
                  The most popular genre in Europe: {the_most_popular_genres} \n
                  The most popular platform in North America: {popular_platforms} \n
                  The publisher with the highest average sales in Japan: {the_best_jp_publisher} \n
                  Number of games that more popular in Europe than in Japan: {popular_eu_games_number} ''')

    sales_year = get_data()
    
    the_best_selling_game = get_the_best_selling_game(sales_year)
    the_most_popular_genres = get_popular_eu_genres(sales_year)
    popular_platforms = get_popular_na_platform(sales_year)
    the_best_jp_publisher = get_the_best_jp_publisher(sales_year)
    popular_eu_games_number = get_popular_eu_games_number(sales_year)

    print_data(the_best_selling_game, the_most_popular_genres, popular_platforms, the_best_jp_publisher, popular_eu_games_number)


a_nikonova_24_lesson_3 = a_nikonova_24_lesson_3()



