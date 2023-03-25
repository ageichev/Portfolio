import urllib.request
from io import StringIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task


default_args = {
    'owner': 'andrew.stalin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 18),
    'schedule_interval': '0 3 * * *'
}

YEAR = 1994 + hash('andrew.stalin') % 23

@dag(default_args=default_args)
def lesson_3_andrew_stalin():
    
    @task()
    def load_data():
        response = urllib.request.urlopen('https://drive.google.com/uc?id=1eY0KdZLSEx17igns-KiXsLvEQ1T4TPsm&export=download')
        data = response.read()
        csv = data.decode('utf-8')

        df = pd.read_csv(StringIO(csv), sep=',').query('Year == @YEAR')
        return df

    # Какая игра была самой продаваемой в этом году во всем мире
    @task()
    def get_world_best_selling_game(df):
        best_game = df[df.Global_Sales == df.Global_Sales.max()].Name.values[0]
        return best_game

    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_europe_best_selling_genre(df):
        genres = df[df.EU_Sales == df.EU_Sales.max()].Genre.to_list()
        return genres
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task()
    def get_america_best_selling_platform(df):
        platforms = df.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'NA_Sales': 'count'})
        best_platforms = platforms[platforms.NA_Sales == platforms.NA_Sales.max()].Platform.to_list()
        return best_platforms

    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def get_japan_best_selling_publisher(df):
        publishers = df.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'})
        best_publishers = publishers[publishers['JP_Sales'] == publishers.JP_Sales.max()].Publisher.to_list()
        return best_publishers

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_europe_better_selling_than_japan(df):
        sales = df.groupby('Name', as_index=False).agg({'EU_Sales':sum, 'JP_Sales':sum})
        count = sales.query('EU_Sales > JP_Sales').shape[0]
        return count

    @task()
    def print_data(
        world_best_selling_game,
        europe_best_selling_genre, 
        america_best_selling_platform, 
        japan_best_selling_publisher,
        europe_better_selling_than_japan,
    ):
        print(f'The best-selling game in {YEAR} Worldwide: {world_best_selling_game}')
        print(f'The best-selling genre in {YEAR} in Europe: {europe_best_selling_genre}')
        print(f'The best-selling platform in {YEAR} in America: {america_best_selling_platform}')
        print(f'The best-selling publisher in {YEAR} in Japan: {japan_best_selling_publisher}')
        print(f'Europe_better-selling games count greater than Japan in {YEAR}: {europe_better_selling_than_japan}')

    df = load_data()
    world_best_selling_game = get_world_best_selling_game(df)
    europe_best_selling_genre = get_europe_best_selling_genre(df)
    america_best_selling_platform = get_america_best_selling_platform(df)
    japan_best_selling_publisher = get_japan_best_selling_publisher(df)
    europe_better_selling_than_japan = get_europe_better_selling_than_japan(df)   
    print_data(
        world_best_selling_game,
        europe_best_selling_genre, 
        america_best_selling_platform, 
        japan_best_selling_publisher,
        europe_better_selling_than_japan
    )
    
lesson_3_andrew_stalin = lesson_3_andrew_stalin()
