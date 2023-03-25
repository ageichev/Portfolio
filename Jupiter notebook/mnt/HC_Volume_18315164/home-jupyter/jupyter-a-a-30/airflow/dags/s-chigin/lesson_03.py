# import pandas as pd
# from datetime import timedelta
# from datetime import datetime
# from io import StringIO
# from airflow.decorators import dag, task
# from airflow.operators.python import get_current_context

# path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
# login = 's-chigin'

# default_args = {
#     'owner': 's-chigin',
#     'depends_on_past': False,
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
#     'start_date': datetime(2023, 2, 28)
# }

# @dag(default_args = default_args, schedule_interval = '0 16 * * *', catchup = False)
# def s_chigin_lesson_3():
    
#     @task()
#     def get_data():
#         year = 1994 + hash(f'{login}') % 23
#         df = pd.read_csv(path) \
#             .query(f'Year == {year}') \
#             .reset_index(drop = True)
#         return {'data': df, 'year': year}

#     @task()
#     def best_selling_game(df):
# #         df = pd.read_csv(StringIO(df), sep = ',')
#         best_sell_game = df \
#                             .groupby('Name', as_index = False) \
#                             .agg(sales_sum = ('Global_Sales', 'sum')) \
#                             .sort_values(by = 'sales_sum', ascending = False) \
#                             .reset_index(drop = True)['Name'][0]
#         return {'best_sell_game': best_sell_game}

#     @task()
#     def best_selling_genre_in_EU(df):
# #         df = pd.read_csv(StringIO(df), sep = ',')
#         best_sell_genre_eu = df \
#                                 .groupby('Genre', as_index = False) \
#                                 .agg(eu_sales_sum = ('EU_Sales', 'sum')) \
#                                 .sort_values(by = 'eu_sales_sum', ascending = False) \
#                                 .reset_index(drop = True)['Genre'][0]
#         return {'best_sell_genre_eu': best_sell_genre_eu}

#     @task()
#     def best_platform_in_NA(df):
# #         df = pd.read_csv(StringIO(df), sep = ',')
#         best_patform_na = df \
#                                 .query("NA_Sales > 1") \
#                                 .groupby('Platform', as_index = False) \
#                                 .agg(cnt_games = ('Name', 'count')) \
#                                 .sort_values(by = 'cnt_games', ascending = False) \
#                                 .reset_index(drop = True)['Platform'][0]
#         return {'best_patform_na': best_patform_na}

#     @task()
#     def best_publisher_in_JP(df):
# #         df = pd.read_csv(StringIO(df), sep = ',')
#         best_publisher_jp = df \
#                                 .groupby('Publisher', as_index = False) \
#                                 .agg(sales_mean = ('JP_Sales', 'mean')) \
#                                 .sort_values(by = 'sales_mean', ascending = False) \
#                                 .reset_index(drop = True)['Publisher'][0]
#         return {'best_publisher_jp': best_publisher_jp}

#     @task()
#     def n_games(df):
# #         df = pd.read_csv(StringIO(df), sep = ',')
#         n = df \
#                 .groupby('Name', as_index = False) \
#                 .agg(jp_sales = ('JP_Sales', 'sum'), eu_sales = ('EU_Sales', 'sum')) \
#                 .query('eu_sales > jp_sales') \
#                 .shape[0]
#         return {'n': n}

#     @task()
#     def print_results(res1, res2, res3, res4, res5, year):
#         context = get_current_context()
#         date = context['ds']
#         print(f'All task has been run on {date}')
#         print(f'Results for year = {year}')
#         print(f"1. Best selling game: {res1['best_sell_game']}")
#         print(f"2. Best selling genre in EU: {res2['best_sell_genre_eu']}")
#         print(f"3. Best selling platform in NA (games with sales > 1 mln): {res3['best_patform_na']}")
#         print(f"4. Best publisher in Japan: {res4['best_publisher_jp']}")
#         print(f"5. Number of games, which were sold better in EU than in JP: {res5['n']}")
#         pass
        

#     output = get_data()
#     res1 = best_selling_game(output['data'])
#     res2 = best_selling_genre_in_EU(output['data'])
#     res3 = best_platform_in_NA(output['data'])
#     res4 = best_publisher_in_JP(output['data'])
#     res5 = n_games(output['data'])

#     print_results(res1, res2, res3, res4, res5, output['year'])

# s_chigin_lesson_3 = s_chigin_lesson_3()