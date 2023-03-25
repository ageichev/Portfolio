import pandas as pd
from datetime import timedelta, datetime
from io import StringIO
from airflow.decorators import dag, task

DATA = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'j.ustits'
YEAR = 1994 + hash(f'{login}') % 23
print(YEAR)

default_args = {
    'owner': 'j.ustits',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 14),
}
schedule_interval = '00 12 * * *'


@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def games_airflow_2():
    @task()
    def get_data():
        games = pd.read_csv(DATA)
        games = games[games.Year == YEAR]
        return games.to_csv(index=False)

    @task()
    def get_top_game(games):
        games = pd.read_csv(StringIO(games))
        game = games \
            .groupby('Name', as_index=False) \
            .agg({'Global_Sales': 'sum'}) \
            .rename(columns={'Global_Sales': 'Sales'})
        top_game = list(game[game.Sales == game.Sales.max()].Name)
        return top_game

    @task()
    def get_top_genre_eu(games):
        games = pd.read_csv(StringIO(games))
        genre_eu = games \
            .groupby('Genre', as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .rename(columns={'EU_Sales': 'Sales'})
        top_genre_eu = list(genre_eu[genre_eu.Sales == genre_eu.Sales.max()].Genre)
        return top_genre_eu

    @task()
    def get_top_platform_na(games):
        games = pd.read_csv(StringIO(games))
        America_games = games \
            .groupby('Name') \
            .agg({'NA_Sales': 'sum'}) \
            .query('NA_Sales > 1')
        platform_na = games[games.Name.isin(America_games.index)] \
            .groupby('Platform', as_index=False) \
            .agg({'NA_Sales': 'count'}) \
            .rename(columns={'NA_Sales': 'Sales'})
        top_platform_na = list(platform_na[platform_na.Sales == platform_na.Sales.max()].Platform)
        return top_platform_na

    @task()
    def get_top_publisher_jp(games):
        games = pd.read_csv(StringIO(games))
        publisher_jp = games \
            .groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .rename(columns={'JP_Sales': 'Sales'})
        top_publisher_jp = list(publisher_jp[publisher_jp.Sales == publisher_jp.Sales.max()].Publisher)
        return top_publisher_jp

    @task()
    def get_eu_more_jp(games):
        games = pd.read_csv(StringIO(games))
        eu_more_jp = games \
            .groupby('Name', as_index=False) \
            .agg({'JP_Sales': 'sum',
                  'EU_Sales': 'sum'}) \
            .query('EU_Sales > JP_Sales') \
            .shape[0]
        return eu_more_jp

    @task()
    def print_data(top_game, top_genre_eu, top_platform_na, top_publisher_jp, eu_more_jp):
        print(f'Sales data for {YEAR}:')
        print(f'  The best-selling game in the world: {", ".join(top_game)}')
        print(f'  The best-selling genre in Europe: {", ".join(top_genre_eu)}')

        if len(top_platform_na) > 0:
            print(
                f'  The best-selling platform in America (for games with a million copies): {", ".join(top_platform_na)}')
        else:
            print('  There were no games in America with sales of more than 1 million')

        print(f'  The publisher with the highest average sales in Japan: {", ".join(top_publisher_jp)}')
        print(f'  {eu_more_jp} games sold better in Europe than in Japan')

    top_data = get_data()
    top_game = get_top_game(top_data)
    top_genre_eu = get_top_genre_eu(top_data)
    top_platform_na = get_top_platform_na(top_data)
    top_publisher_jp = get_top_publisher_jp(top_data)
    eu_more_jp = get_eu_more_jp(top_data)

    print_data(top_game, top_genre_eu, top_platform_na, top_publisher_jp, eu_more_jp)


j_ustits_lesson3 = games_airflow_2()
