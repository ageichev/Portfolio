import pandas as pd
import numpy as np
from io import StringIO
from datetime import timedelta
from datetime import datetime

login = 'd-badrtdinova'
year_for_survey = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'd-badrtdinova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 25),
    'schedule_interval': '0 3 * * *'
}


@dag(default_args=default_args, catchup=False)
def d_badrt_games_survey():

    # task 1 reading data
    @task(retries=3)
    def get_data(year_for_survey):
        df = pd.read_csv('vgsales.csv')
        df = df.dropna()
        df = df[df.Year == year_for_survey]
        games_stat = df.to_csv(index=False)
        return  games_stat

    # task 2_1 Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=3)
    def get_global_bestseller(games_stat):
        df_games_stat = pd.read_csv(StringIO(games_stat))
        max_global_index = df_games_stat.Global_Sales.idxmax()
        bestseller = df_games_stat['Name'].loc[max_global_index]
        return bestseller

    # task 2_2 Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task(retries=3)
    def get_best_eu_genre(games_stat):
        df_games_stat = pd.read_csv(StringIO(games_stat))
        eu_genres = df_games_stat.groupby(['Genre'], as_index = False).agg({'EU_Sales' : 'sum'})
        max_EU_Sales = eu_genres.EU_Sales.max()
        best_genres_eu_list = eu_genres[eu_genres.EU_Sales == max_EU_Sales].Genre.to_list()
        return best_genres_eu_list

    # task 2_3 На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
               # Перечислить все, если их несколько
    @task(retries=3)
    def get_best_na_platform(games_stat):
        df_games_stat = pd.read_csv(StringIO(games_stat))
        best_platforms_na = df_games_stat[df_games_stat.NA_Sales >=1]
        best_platforms_na = best_platforms_na.groupby('Platform', as_index = False).agg({'Name' : 'count'})
        best_platforms_na = best_platforms_na.rename(columns = {'Name' : 'amount'})
        max_amount = best_platforms_na.amount.max()
        max_popular_platform_na = best_platforms_na[best_platforms_na.amount == max_amount].Platform.to_list()
        return max_popular_platform_na

    # task 2_4 У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task(retries=3)
    def get_best_publishers_in_jp(games_stat):
        df_games_stat = pd.read_csv(StringIO(games_stat))
        jp_games = df_games_stat[df_games_stat.JP_Sales > 0].groupby(['Publisher'], as_index = False).agg({'JP_Sales' : 'mean'})
        max_jp_avg_sales = jp_games.JP_Sales.max()
        best_publishers_in_jp = jp_games[jp_games.JP_Sales == max_jp_avg_sales].Publisher.to_list()
        return best_publishers_in_jp

    # task 2_5 Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=3)
    def get_eu_vs_jp(games_stat):
        df_games_stat = pd.read_csv(StringIO(games_stat))
        comparing_sales_in_both_regions = df_games_stat.query('EU_Sales > JP_Sales and JP_Sales > 0').shape[0]
        comparing_sales_only_eu_sales = df_games_stat.query('EU_Sales > JP_Sales and JP_Sales == 0').shape[0]
        if comparing_sales_in_both_regions > 0:
            eu_sales_better_jp = comparing_sales_in_both_regions
        else:
            eu_sales_better_jp = 'no'
        if comparing_sales_only_eu_sales > 0:
            eu_sales_and_no_jp = comparing_sales_only_eu_sales
        else:
            eu_sales_and_no_jp = 'no'
        return {'eu_sales_better_jp': eu_sales_better_jp, 'eu_sales_and_no_jp': eu_sales_and_no_jp}

    # task 3 print data
    @task(retries=3)
    def print_data(year_for_survey, bestseller, best_genres_eu_list, max_popular_platform_na, best_publishers_in_jp, eu_vs_jp):
        eu_sales_better_jp = eu_vs_jp['eu_sales_better_jp']
        eu_sales_and_no_jp = eu_vs_jp['eu_sales_and_no_jp']
        print(f'Survey on games sales for year {year_for_survey}')
        print(f' Global bestseller is: ')
        print( bestseller)
        print(f' Top genre(s):')
        print(*best_genres_eu_list, sep ='\n')
        print(f' Top platform(s) for games with 1M copies:')
        print(*max_popular_platform_na, sep ='\n') 
        print(f' Top publisher(s) by average sales in Japan:')
        print(*best_publishers_in_jp, sep ='\n') 
        print(f' There is {eu_sales_better_jp} games with better sales in EU than in Japan')
        print(f'and {eu_sales_and_no_jp} games with sales in EU that have no sales in Japan ')

    games_stat = get_data(year_for_survey)

    bestseller = get_global_bestseller(games_stat)

    best_genres_eu_list = get_best_eu_genre(games_stat)

    max_popular_platform_na = get_best_na_platform(games_stat)

    best_publishers_in_jp = get_best_publishers_in_jp(games_stat)

    eu_vs_jp = get_eu_sales_better_jp(games_stat)

    print_data(year_for_survey, bestseller, best_genres_eu_list, max_popular_platform_na, best_publishers_in_jp, eu_vs_jp)
    
d_badrt_games_survey = d_badrt_games_survey()

