import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from io import StringIO

default_args = {
    'owner': 'm-rossihinapirozhkova-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 15),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args = default_args)

def airflow_task2_rossihinapirozhkova():
    
    @task(retries = 3)
    def read_file():
        
        #first task
        #read file
        game = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        # filtred Year
        game_1997 = game[game.Year == 1997]
        # change type of 'Year columns'
        game_1997 = game_1997.astype({'Year':'int'})
        return game_1997.to_csv(index = False)
    
    @task(retries = 3, retry_delay=timedelta(10))
    def get_best_game(game_1997):
        
        # second task
        # Which game was a bestseller in the 1997 year?
        game_1997 = pd.read_csv(StringIO(game_1997))
        best_games = game_1997[game_1997.Global_Sales == game_1997.Global_Sales.max()]
        best_games = f'The {best_games.Name.values[0]} is the bestseller in 1997'
        return best_games
    
    @task()
    def get_best_genre(game_1997):
        # third task
        # Which games ware the bestseller in the 1997 year in EU?
        game_1997 = pd.read_csv(StringIO(game_1997))
        best_genre_EU = (game_1997.groupby('Genre', as_index = False)['EU_Sales'].sum())
        best_genre_EU = best_genre_EU[best_genre_EU.EU_Sales < best_genre_EU.EU_Sales.max()]
        best_genre_EU = best_genre_EU.Genre.values
        return best_genre_EU
        
        #print(*best_genre_EU, sep = ', ')
        
    @task()
    def get_best_platform(game_1997):
    
        # forth task
        # Which platforms sold more than 1 mln copies of games in North America in 1997 year?
        game_1997 = pd.read_csv(StringIO(game_1997))
        best_platform_NA = game_1997.query('NA_Sales >= 1').groupby('Platform', as_index = False)['Name'].nunique()
        best_platform_NA = best_platform_NA[best_platform_NA.Name == best_platform_NA.Name.max()]
        best_platform_NA = best_platform_NA.Platform.values
        return best_platform_NA
        
        #print(*best_platform_NA, sep = ', ')
    
    @task()
    def get_best_publisher(game_1997):
    
        # fifth task
        # Which publishers had the highest average sales in Japan?
        game_1997 = pd.read_csv(StringIO(game_1997))
        best_publisher_Japan = game_1997.groupby('Publisher', as_index = False)['JP_Sales'].mean().round(4)
        best_publisher_Japan = best_publisher_Japan[best_publisher_Japan.JP_Sales == best_publisher_Japan.JP_Sales.max()]
        best_publisher_Japan = best_publisher_Japan.Publisher.values
        return best_publisher_Japan
        
        #print(*best_publisher_Japan, sep = ', ')
        
    @task()
    def get_amount_better_game(game_1997):
    
        # sixth task
        # How many games were sold better in the EU than in Japan?
        game_1997 = pd.read_csv(StringIO(game_1997))
        better_game_EUvsJP = game_1997.query('EU_Sales > JP_Sales').Name.nunique()
        return better_game_EUvsJP
    
    @task()
    def get_result(best_games, best_genre_EU, best_platform_NA, best_publisher_Japan, better_game_EUvsJP):
        
        context = get_current_context()
        date = context['ds']
        
        
        print(f'Current date: {date}')
        print(best_games)
        print()
        print(f'The best Genre in EU is/are: ', *best_genre_EU, sep = '\n')
        print()
        print(f'The best platform in NA is/are: ', *best_platform_NA, sep = '\n')
        print()
        print(f'The best publisher in JP is/are: ', *best_publisher_Japan, sep = '\n')
        print()
        print(f'Games were sold better in the EU than in Japan : {better_game_EUvsJP}')
        
    game_1997 = read_file()
    best_games = get_best_game(game_1997)
    best_genre_EU = get_best_genre(game_1997)
    best_platform_NA = get_best_platform(game_1997)
    best_publisher_Japan = get_best_publisher(game_1997)
    better_game_EUvsJP = get_amount_better_game(game_1997)
    get_result(best_games, best_genre_EU, best_platform_NA, best_publisher_Japan, better_game_EUvsJP)
    
airflow_task2_rossihinapirozhkova = airflow_task2_rossihinapirozhkova()  