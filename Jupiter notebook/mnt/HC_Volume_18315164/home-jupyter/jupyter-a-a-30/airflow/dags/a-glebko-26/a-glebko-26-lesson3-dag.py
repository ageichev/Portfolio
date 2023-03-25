#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import BytesIO
from io import StringIO
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task

login = 'a-glebko-26'
year_ag = 1994 + hash(f'{login}') % 23


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 24),
    'schedule_interval': '0 11 * * *'
}


@dag(default_args=default_args)
def a_glebko_26_games():
    @task()
    def load_data():
        table = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        login = 'a-glebko-26'
        year_ag = 1994 + hash(f'{login}') % 23
        table_year = table.query('Year == @year_ag')
        return table_year

    @task()
    def max_sales_1(table_year):
        table = table_year
        max_sales = table.loc[table.Global_Sales == table.Global_Sales.max()]
        max_sales.Name.values[0]
        return(max_sales)

    @task()
    def eu_popular_2(table_year):
        table = table_year
        max_sales_eu = table.loc[table.EU_Sales == table.EU_Sales.max()]
        popular_eu = ', '.join(max_sales_eu.Genre.values)
        return(popular_eu)

    @task()
    def na_platform_3(table_year):
        table = table_year
        platform = table.query('NA_Sales > 1').groupby('Platform', as_index=False).NA_Sales.count()
        platform_max = platform.loc[platform.NA_Sales == platform.NA_Sales.max()].Platform.values
        na_platform_max = ', '.join(platform_max)
        return(na_platform_max)

    @task()
    def publisher_jp_4(table_year):
        table = table_year
        publisher_jp = table.groupby('Publisher', as_index=False).agg({'JP_Sales':'mean'}).sort_values('JP_Sales', ascending = False)
        publisher_max_jp = publisher_jp.loc[publisher_jp.JP_Sales == publisher_jp.JP_Sales.max()].Publisher.values
        publisher_max_jp = ', '.join(publisher_max_jp)
        return(publisher_max_jp)

    @task()
    def EU_gather_JP_5(table_year):
        table = table_year
        more_eu_than_jp = len(table.loc[table.EU_Sales > table.JP_Sales])
        return(more_eu_than_jp)

    @task()
    def print_data(max_sales_game, most_popular_eu, platform_max, publisher_most_jp, eu_than_jp):
        context = get_current_context()
        date = context['ds']
        
        login = 'a-glebko-26'
        year_ag = 1994 + hash(f'{login}') % 23

        print(f'''Info about'{year_ag}
                The most saled game: {max_sales_game}
                The most pop EU genre: {most_popular_eu}
                The most NA popular platforms with >1m games: {platform_max}
                JP publisher the most mean sales: {publisher_most_jp}
                Number of games with beter EU thn JP sales: {eu_than_jp}''')


    table_year = load_data()
    max_sales = max_sales_1(table_year)
    popular_eu = eu_popular_2(table_year)
    na_platform_max = na_platform_3(table_year)
    publisher_max_jp = publisher_jp_4(table_year)
    more_eu_than_jp = EU_gather_JP_5(table_year)
    print_data(max_sales, popular_eu, na_platform_max, publisher_max_jp, more_eu_than_jp)

    
a_glebko_26_lesson3 = a_glebko_26_games()