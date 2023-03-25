import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task


INSERT_FILE = 'vgsales.csv'

default_args = {
    'owner': 'a-salomennikov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 8, 2),
}

schedule_interval = '0 12 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag3_salomennikov():
    @task()
    def read_data():
        df = pd.read_csv(INSERT_FILE, sep=',')
        df = df[df['Year'] == 1999]
        return df

    @task()
    def top_sales_game(dataframe):
        dataframe = dataframe.groupby('Name', as_index=False).agg({'Global_Sales': 'sum'}).sort_values(by='Global_Sales', ascending=False)
        top_sales_game = []
        for row in range(len(dataframe.index)-1):
            if row == 0:
                top_sales_game.append(dataframe.iloc[row]['Name'])
            if dataframe.iloc[row]['Global_Sales'] != dataframe.iloc[row+1]['Global_Sales']:
                break
            else:
                top_sales_game.append(dataframe.iloc[row+1]['Name'])
        return top_sales_game

    @task()
    def top_genre_EU(dataframe):
        dataframe = dataframe.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}).sort_values(by='EU_Sales', ascending=False)
        top_genre_EU = []
        for row in range(len(dataframe.index)-1):
            if row == 0:
                top_genre_EU.append(dataframe.iloc[row]['Genre'])
            if dataframe.iloc[row]['EU_Sales'] != dataframe.iloc[row+1]['EU_Sales']:
                break
            else:
                top_genre_EU.append(dataframe.iloc[row+1]['Genre'])
        return top_genre_EU

    @task()
    def best_NA_platform(dataframe):
        dataframe = dataframe[dataframe['NA_Sales'] > 1]
        dataframe = dataframe.groupby('Platform', as_index=False).agg({'NA_Sales': 'count'}).sort_values(by='NA_Sales',
                                                                                                         ascending=False)
        best_platform_list = []
        for row in range(len(dataframe.index)-1):
            if row == 0:
                best_platform_list.append(dataframe.iloc[row]['Platform'])
            if dataframe.iloc[row]['NA_Sales'] != dataframe.iloc[row+1]['NA_Sales']:
                break
            else:
                best_platform_list.append(dataframe.iloc[row+1]['Platform'])
        return best_platform_list

    @task()
    def best_JP_publisher(dataframe):
        dataframe = dataframe.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).sort_values(by='JP_Sales',
                                                                                                    ascending=False)
        best_JP_publisher = []
        for row in range(len(dataframe.index) - 1):
            if row == 0:
                best_JP_publisher.append(dataframe.iloc[row]['Genre'])
            if dataframe.iloc[row]['JP_Sales'] != dataframe.iloc[row + 1]['JP_Sales']:
                break
            else:
                best_JP_publisher.append(dataframe.iloc[row+1]['Genre'])
        return best_JP_publisher

    @task()
    def better_EU_than_JP(dataframe):
        better_count = 0
        for row in range(len(dataframe.index)):
            if dataframe.iloc[row]['JP_Sales'] < dataframe.iloc[row]['EU_Sales']:
                better_count += 1
        return better_count

    @task()
    def log_create(top_sales, top_genre, best_platform, best_publisher, EU_better):
        with open('data_log_03.txt', 'w') as f:
            for i in top_sales:
                f.write("%s\n" % i)
            f.write('\n')
            for i in top_genre:
                f.write("%s\n" % i)
            f.write('\n')
            for i in best_platform:
                f.write("%s\n" % i)
            f.write('\n')
            for i in best_publisher:
                f.write("%s\n" % i)
            f.write('\n')
            f.write(str(EU_better))


    datafile = read_data()

    top_sales_game = top_sales_game(datafile)
    top_genre_EU = top_genre_EU(datafile)
    best_NA_platform = best_NA_platform(datafile)
    best_JP_publisher = best_JP_publisher(datafile)
    better_EU_than_JP = better_EU_than_JP(datafile)

    log_create(top_sales_game, top_genre_EU, best_NA_platform, best_JP_publisher, better_EU_than_JP)

dag3_salomennikov = dag3_salomennikov()
