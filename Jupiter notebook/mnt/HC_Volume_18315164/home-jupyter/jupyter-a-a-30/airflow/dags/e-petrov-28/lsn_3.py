from io import BytesIO, StringIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


FILE_LINK = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

YEAR = 1994 + hash(f'e-petrov-28') % 23

default_args = {
    'owner': 'e.petrov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2023, 1, 25)
}



@dag(default_args=default_args, schedule_interval='* 12 * * *', catchup=False)
def e_petrov_28_analytics_task():

    # Собираем данные
    @task()
    def get_data():
        df = pd.read_csv(FILE_LINK).query("Year == @YEAR")
        return df


    # Какая игра была самой продаваемой в этом году во всем мире
    @task()
    def best_selling_game(df):
        top_sell_game = df.loc[df.Global_Sales == df.Global_Sales.max()].Name.values[0]
        return top_sell_game


    # Игры какого жанра были самыми продаваемыми в Европе
    @task()
    def best_selling_EU_genres(df):
        top_genre_EU = df.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        top_genre_EU = top_genre_EU.loc[top_genre_EU.EU_Sales == top_genre_EU.EU_Sales.max()].Genre.to_list()
        return top_genre_EU

    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке
    @task()
    def best_selling_NA_platforms(df):
        top_platform_NA = df.query("NA_Sales > 1").groupby('Platform', as_index=False).Name.count()
        top_platform_NA = top_platform_NA.loc[top_platform_NA.Name == top_platform_NA.Name.max()].Platform.to_list()
        return top_platform_NA
    
    
    # У какого издателя самые высокие средние продажи в Японии?
    @task()
    def best_selling_JP_publishers(df):
        top_publisher_JP = df.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'})
        top_publisher_JP = top_publisher_JP.loc[top_publisher_JP.JP_Sales == top_publisher_JP.JP_Sales.max()].Publisher.to_list()
        return top_publisher_JP
    

    # Сколько игр продались лучше в Европе, чем в Японии
    @task()
    def EU_sales_better_JP(df):
        EU_better_JP = df.loc[df.EU_Sales > df.JP_Sales].Name.count()
        return EU_better_JP
    
    
    # Вывод результатов
    @task()
    def print_data(top_sell_game, top_genre_EU, top_platform_NA, top_publisher_JP, EU_better_JP):

        context = get_current_context()
        date = context['ds']

        print(f'''---Data for {YEAR} year for {date}---
                  Best selling game: {top_sell_game}
                  Best Europe genres: {top_genre_EU}
                  Best North America platforms: {top_platform_NA}
                  Best Japan publishers: {top_publisher_JP}
                  How many games sold better in Europe than in Japan: {EU_better_JP}''')
        return
    
    
    df = get_data()
    
    top_sell_game = best_selling_game(df)
    top_genre_EU = best_selling_EU_genres(df)
    top_platform_NA = best_selling_NA_platforms(df)
    top_publisher_JP = best_selling_JP_publishers(df)
    EU_better_JP = EU_sales_better_JP(df)
    
    print_data(top_sell_game, top_genre_EU, top_platform_NA, top_publisher_JP, EU_better_JP)
    
    
e_petrov_28_analytics_task = e_petrov_28_analytics_task()

