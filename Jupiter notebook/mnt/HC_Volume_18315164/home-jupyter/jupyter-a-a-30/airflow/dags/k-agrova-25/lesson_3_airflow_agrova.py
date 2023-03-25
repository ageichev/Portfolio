import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year=1994+hash(f'{"k-agrova-25"}')%23

default_args = {
    'owner': 'k.agrova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 17),
    'schedule_interval': '0 12 * * *'
}



@dag(default_args=default_args, catchup=False)
def agrova_airflow_2():
   
    @task(retries=3)
    def get_data():
        file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        year=1994+hash(f'{"k-agrova-25"}')%23 
        games_df = pd.read_csv(file)
        games_by_year = games_df.query('Year==@year')
        return games_by_year.to_csv(index=False)

    @task(retries=4, retry_delay=timedelta(10))
    # Какая игра была самой продаваемой в этом году во всем мире?
    def question_1(games_by_year):
        question_1 = pd.read_csv(StringIO(games_by_year))
        question_1 = question_1.groupby('Name').agg({'Global_Sales':'sum'}).idxmax()[0]
        return question_1

    @task()
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько.
    def question_2(games_by_year):
        games_by_year_df = pd.read_csv(StringIO(games_by_year))
        eu_max_sales = games_by_year_df.groupby('Genre').agg({'EU_Sales':'sum'})['EU_Sales'].max()
        question_2 = games_by_year_df.groupby('Genre').agg({'EU_Sales':'sum'}).query('EU_Sales==@eu_max_sales').index[0]
        return question_2
    

    @task()
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    def question_3(games_by_year):
        games_by_year_df = pd.read_csv(StringIO(games_by_year))
        platform_max_bestsellers_games = games_by_year_df.query('NA_Sales>1').groupby('Platform').agg({'Name':'nunique'})['Name'].max()
        question_3 = games_by_year_df.query('NA_Sales>1').groupby('Platform').agg({'Name':'nunique'}).query('Name==@platform_max_bestsellers_games').index[0]
        return question_3

    @task()
    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    def question_4(games_by_year):
        games_by_year_df = pd.read_csv(StringIO(games_by_year))
        max_mean_jp_sales = games_by_year_df.groupby('Publisher').agg({'JP_Sales':'mean'})['JP_Sales'].max()
        question_4 = games_by_year_df.groupby('Publisher').agg({'JP_Sales':'mean'}).query('JP_Sales==@max_mean_jp_sales').index[0]
        return question_4
    
    @task()
    #Сколько игр продались лучше в Европе, чем в Японии?
    def question_5(games_by_year):
        games_by_year_df  = pd.read_csv(StringIO(games_by_year))
        question_5 = len(games_by_year_df.query('EU_Sales>JP_Sales').index)
        return question_5

    @task()
    def print_data(question_1, question_2, question_3, question_4, question_5):

        context = get_current_context()
        date = context['ds']

        print(f' 1. {question_1} была самой продаваемой игрой в {year}')
        print(f' 2. {question_2} самый продаваемый жанр в Европе')
        print(f' 3. На {question_3} было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке')
        print(f' 4. У {question_4} самые высокие средние продажи в Японии')
        print(f' 5. Всего {question_5} игр продались лучше в Европе, чем в Японии ')

        return True

    games_by_year = get_data()
    question_1 = question_1(games_by_year)
    question_2 = question_2(games_by_year)
    question_3 = question_3(games_by_year)
    question_4 = question_4(games_by_year)
    question_5 = question_5(games_by_year)

    print_data(question_1, question_2, question_3, question_4, question_5)

agrova_airflow_2 = agrova_airflow_2()
