import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

default_args = {
    'owner': 's-novitskij-21',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 10),
    'schedule_interval': '15 20 * * *'
}

data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f's-novitskij-21') % 23


@dag(default_args=default_args, catchup=False)
def data_vgsales2000():
    @task(retries=3)
    def get_data():
        vgsales = pd.read_csv(data)
        vgsales = vgsales.query("Year == @year")
        return vgsales

#Какая игра была самой продаваемой в этом году во всем мире
    @task(retries=3)
    def best_selling_videogame(vgsales):
        best_selling_game = vgsales.sort_values('Global_Sales', ascending=False) \
            .head(1)
        best_selling_game = [str(i) for i in best_selling_game['Name']]
        return best_selling_game

#Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task(retries=3)
    def topsales_game_eu(vgsales):
        top_sales_eu = vgsales.groupby("Genre", as_index=False) \
            .agg({'EU_Sales': 'sum'}) \
            .sort_values('EU_Sales', ascending=False)
        numer_top_sales_eu = top_sales_eu.head(1)['EU_Sales'].iloc[0]
        top_game_sales_eu = top_sales_eu.query("EU_Sales == @numer_top_sales_eu")
        top_game_sales_eu = [str(i) for i in top_game_sales_eu['Genre']]
        return top_game_sales_eu

#На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
#Перечислить все, если их несколько
    @task(retries=3)
    def top_platform(vgsales):
        na_sales = vgsales.query("NA_Sales > 1") \
            .groupby('Platform', as_index=False) \
            .agg({'NA_Sales': 'sum'}) \
            .sort_values('NA_Sales', ascending=False)
        na_sales_sum = na_sales['NA_Sales'].iloc[0]
        top_platforms_NA = na_sales.query("NA_Sales == @na_sales_sum")
        top_platforms_NA = [str(i) for i in top_platforms_NA['Platform']]
        return top_platforms_NA

#У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task(retries=3)
    def top_sales_publisher_JP(vgsales):
        jp_sales_median = vgsales.groupby('Publisher', as_index=False) \
            .agg({'JP_Sales': 'mean'}) \
            .sort_values('JP_Sales', ascending=False)
        jp_median_num = jp_sales_median['JP_Sales'].iloc[0]
        top_med_sales_publisher_JP = jp_sales_median.query("JP_Sales == @jp_median_num")
        top_med_sales_publisher_JP = [str(i) for i in top_med_sales_publisher_JP['Publisher']]
        return top_med_sales_publisher_JP

#Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=3)
    def num_sales_game_eu_more_jp(vgsales):
        num_game_eu_more_jp = vgsales.query("EU_Sales > JP_Sales").Name.count()
        return num_game_eu_more_jp
    
#собираем ответы
    @task(retries=3)
    def print_data(best_selling_game, top_game_sales_eu, top_platforms_NA, top_med_sales_publisher_JP, num_game_eu_more_jp):
        best_sales = best_selling_game
        best_eu = top_game_sales_eu
        na_platform = top_platforms_NA
        jp_publisher = top_med_sales_publisher_JP
        num_game = num_game_eu_more_jp
        print(f"In {year} year:")
        print("Самая продаваемая игра в этом году: ", *best_sales)
        print("Жанр самой продаваемой игры в Европе: ", * best_eu)
        print("Платформа, на которой игры продались более чем миллионным тиражом в Северной Америке: ", *na_platform)
        print("Издатель с самыми высокими средними продажами в Японии: ", *jp_publisher)
        print(f"{num_game} игр продались лучше в Европе, чем в Японии в {year} году ")
        

    vgsales = get_data()
    best_selling_game = best_selling_videogame(vgsales)
    top_game_sales_eu = topsales_game_eu(vgsales)
    top_platforms_NA = top_platform(vgsales)
    top_med_sales_publisher_JP = top_sales_publisher_JP(vgsales)
    num_game_eu_more_jp = num_sales_game_eu_more_jp(vgsales)
    print_data(best_selling_game, top_game_sales_eu, top_platforms_NA, top_med_sales_publisher_JP, num_game_eu_more_jp)

data_vgsales2000 = data_vgsales2000()
