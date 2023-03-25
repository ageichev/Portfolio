import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
'owner': 'a-dobrov-30',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2023, 2, 27)
}

schedule_interval = '0 12 * * *'

@dag(default_args=default_args, schedule_interval = schedule_interval, catchup=False)
def a_dobrov_30_lesson_3():
    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        my_Year = 1994 + hash('a-dobrov-30') % 23
        df_my_Year = df[df.Year == my_Year]
        return df_my_Year.to_csv(index=False)

    @task()
    def top_1_game_global(df_my_Year):
        top_1_game_global = pd.read_csv(StringIO(df_my_Year)).sort_values('Global_Sales', ascending = False).head(1).Name.values[0]
        return top_1_game_global

    @task()
    def top_1_genre_eu(df_my_Year):
        top_1_genre_eu = pd.read_csv(StringIO(df_my_Year)).groupby('Genre', as_index = False).agg(EU_Sales = ('EU_Sales', 'sum')).sort_values('EU_Sales', ascending = False).head(1).Genre.values[0]
        return top_1_genre_eu

    @task()
    def top_platforms_na(df_my_Year):
        top_platforms_na = pd.read_csv(StringIO(df_my_Year))
        top_platforms_na['NA_Sales > 1'] = 0
        top_platforms_na.loc[top_platforms_na['NA_Sales'] > 1, 'NA_Sales > 1'] = 1
        top_platforms_na = top_platforms_na.groupby('Platform', as_index = False).agg(cnt = ('NA_Sales > 1', 'sum')).sort_values('cnt', ascending = False)
        top_platforms_na = ', '.join(top_platforms_na[top_platforms_na.cnt == top_platforms_na.cnt.max()].Platform.tolist())
        return top_platforms_na
    
    @task()
    def top_publishers_jp(df_my_Year):
        top_publishers_jp = pd.read_csv(StringIO(df_my_Year))
        top_publishers_jp = top_publishers_jp.groupby('Publisher', as_index = False).agg(AVG_JP_Sales = ('JP_Sales', 'mean')).sort_values('AVG_JP_Sales', ascending = False)
        top_publishers_jp = ', '.join(top_publishers_jp[top_publishers_jp.AVG_JP_Sales == top_publishers_jp.AVG_JP_Sales.max()].Publisher.tolist())
        return top_publishers_jp
    
    @task()
    def games_eu_better_jp(df_my_Year):
        games_eu_better_jp = pd.read_csv(StringIO(df_my_Year))
        games_eu_better_jp['EU_Sales > JP_Sales'] = 0
        games_eu_better_jp.loc[games_eu_better_jp['EU_Sales'] >= games_eu_better_jp['JP_Sales'], 'EU_Sales > JP_Sales'] = 1
        games_eu_better_jp = games_eu_better_jp[games_eu_better_jp['EU_Sales > JP_Sales'] == 1]
        games_eu_better_jp = games_eu_better_jp['Name'].count()
        return games_eu_better_jp

    @task()
    def print_data(top_1_game_global,
                   top_1_genre_eu,
                   top_platforms_na,
                   top_publishers_jp,
                   games_eu_better_jp):

        context = get_current_context()
        date = context['ds']
        my_Year = 1994 + hash('a-dobrov-30') % 23

        print(f'''Дата выполнения {date}
                  Отчет за {my_Year} год
                  Самая продаваемая игра во всем мире: {top_1_game_global}
                  Самый продаваемый жанр в Европе: {top_1_genre_eu}
                  Платформы, на которых было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке: {top_platforms_na}
                  Издатели, у которых были самые высокие средние продажи в Японии: {top_publishers_jp}
                  Количество игр, которые продались лучше в Европе, чем в Японии: {games_eu_better_jp}''')


    df_my_Year = get_data()
    
    top_1_game_global = top_1_game_global(df_my_Year)
    top_1_genre_eu = top_1_genre_eu(df_my_Year)
    top_platforms_na = top_platforms_na(df_my_Year)
    top_publishers_jp = top_publishers_jp(df_my_Year)
    games_eu_better_jp = games_eu_better_jp(df_my_Year)

    print_data(top_1_game_global,
               top_1_genre_eu,
               top_platforms_na,
               top_publishers_jp,
               games_eu_better_jp)

a_dobrov_30_lesson_3 = a_dobrov_30_lesson_3()