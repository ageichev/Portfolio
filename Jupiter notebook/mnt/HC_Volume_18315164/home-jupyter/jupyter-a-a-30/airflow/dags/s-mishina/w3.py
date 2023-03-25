from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task

VGSALES_FILE = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
LOGIN = 's-mishina'
YEAR = 1994 + hash(f'{LOGIN}') % 23

default_args = {
    'owner': 's.mishina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 2),
    'schedule_interval': '0 17 * * *'
}

def format_question(question):
    return question + f' в {YEAR} году:'


@dag(default_args=default_args, catchup=False)
def s_mishina_w3():
    @task(retries=3)
    def get_data():
        df = pd.read_csv(VGSALES_FILE)
        df = df[df['Year'] == YEAR]
        return df

    @task()
    def get_top_global_game(df):
        q = format_question('Самая продаваемая игра во всем мире')
        v = df['Global_Sales'].groupby(df['Name']).sum().idxmax()
        return {'question': q, 'result': v}

    @task()
    def get_top_genres(df):
        q = format_question('Жанры наиболее продаваемых игр в Европе')
        game_eu_sales = df['EU_Sales'].groupby(df['Name']).sum()
        max_sales = game_eu_sales.max()
        games_top_sales = game_eu_sales[game_eu_sales.eq(max_sales)].index
        v = ', '.join(set(df['Genre'][df['Name'].isin(games_top_sales)]))
        return {'question': q, 'result': v}

    @task()
    def get_top_platforms(df):
        q = format_question(
            'Платформы на которой было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке'
        )
        platform_game_gt_1m = df['Platform'].where(
            df['Name'].where(
                df['NA_Sales'] > 1.0
            ).notna()
        )
        platform_count = platform_game_gt_1m.value_counts()
        max_platform_count = platform_count.max()
        v = ', '.join(platform_count[platform_count.eq(max_platform_count)].index)
        return {'question': q, 'result': v}

    @task()
    def get_top_publishers(df):
        q = format_question('Издатели с самыми высокими средними продажами в Японии')
        publisher_avg_sales = df['JP_Sales'].groupby(df['Publisher']).mean()
        max_sales = publisher_avg_sales.max()
        v = ', '.join(set(publisher_avg_sales[publisher_avg_sales.eq(max_sales)].index))
        return {'question': q, 'result': v}

    @task()
    def get_games_jp_gt_eu(df):
        q = format_question('Количество игр, которые продались лучше в Европе, чем в Японии')
        v = sum(df['EU_Sales'].groupby(df['Name']).sum() > df['JP_Sales'].groupby(df['Name']).sum())
        return {'question': q, 'result': v}

    @task()
    def print_data(*args):
        
        for data in args:
            print(data['question'], data['result'], end='\n\n')

    data = get_data()
    top_global_game = get_top_global_game(data)
    top_genres = get_top_genres(data)
    top_platforms = get_top_platforms(data)
    top_publishers = get_top_publishers(data)
    games_jp_gt_eu = get_games_jp_gt_eu(data)

    print_data(top_global_game, top_genres, top_platforms, top_publishers, games_jp_gt_eu)

s_mishina_w3 = s_mishina_w3()
