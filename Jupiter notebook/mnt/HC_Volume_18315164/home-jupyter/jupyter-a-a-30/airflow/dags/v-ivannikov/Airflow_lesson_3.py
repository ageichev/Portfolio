import pandas as pd
from datetime import timedelta, datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


PATH = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv' 
YEAR = 1994 + hash(f'v-ivannikov') % 23

default_args = {
    'owner': 'aivannikova1',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 27),
    'schedule_interval': '0 12 * * *'
}

CHAT_ID = -684035813
try:
    BOT_TOKEN = '5499903160:AAF5zhcO3ycA1lYPE4CIHcIJWK1m_1_I7LA'
except:
    BOT_TOKEN = ''

def send_message_tg(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Даг {dag_id} выполнился успешно за {date}.'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass


@dag(default_args=default_args, catchup=False)
def v_ivannikov_lesson_3():
    @task
    def get_data():
        df = pd.read_csv(PATH)
        df = df.query('Year == @YEAR')
        return df

    @task
    def get_Global_sales_top(df):
        most_sales = df.sort_values('Global_Sales', ascending=False).head(1).Name
        return most_sales.iloc[0]

    @task
    def get_EU_genre_top(df):
        EU_top_df = df.groupby('Genre', as_index=False). \
                                agg({'EU_Sales': 'sum'}). \
                                sort_values('EU_Sales', ascending=False). \
                                round(0)
        return EU_top_df[EU_top_df['EU_Sales'] == EU_top_df['EU_Sales'].max()].Genre.to_list()

    @task
    def get_NA_platforms_top(df):
        NA_top_platforms_df = df. \
                            query('NA_Sales > 1'). \
                            groupby('Platform', as_index=False). \
                            agg({'NA_Sales': 'count'}). \
                            sort_values('NA_Sales', ascending=False)
        return NA_top_platforms_df[NA_top_platforms_df['NA_Sales'] == NA_top_platforms_df['NA_Sales'].max()].Platform.to_list()
    
    @task
    def get_JP_top_publishers(df):
        JP_top_publishers_df = df. \
                            groupby('Publisher', as_index=False). \
                            agg({'JP_Sales': 'mean'}). \
                            sort_values('JP_Sales', ascending=False). \
                            round(2)
        return JP_top_publishers_df[JP_top_publishers_df['JP_Sales'] == JP_top_publishers_df['JP_Sales'].max()].Publisher.to_list()
    
    @task
    def get_EU_more_JP(df):
        return df.query('EU_Sales > JP_Sales').Name.count()

    @task(on_success_callback=send_message_tg)
    def print_data(top_game, top_eu_genres, top_na_platforms, top_jp_publishers, eu_jp_comparison):

        contex = get_current_context()
        date = contex['ds']

        top_eu_genres_0 = 'Самым популярным жанром игр в Европе является'
        top_na_platforms_0 = 'Самая популярная платформа в Северной Америке (продажи >1млн):'
        top_eu_genres_1 = 'Самыми популярными жанрами игр в Европе являются'
        top_na_platforms_1 = 'Самые популярные платформы в Северной Америке (продажи >1млн):'
    
    
        output_variation_dict = {
            True: [top_eu_genres_0, top_na_platforms_0],
            False: [top_eu_genres_1, top_na_platforms_1]
        }
    
        print(f'Дата выполнения: {date}')
        print(f'Год, за который анализируем данные: {YEAR}')
        print(f'1) Самая продаваемая игра во всем мире: {top_game}')
    
        print(f'2) {output_variation_dict[len(top_eu_genres) == 1][0]}', end=' ')
        print(*top_eu_genres, sep=', ')
        print(f'3) {output_variation_dict[len(top_na_platforms) == 1][1]}', end=' ')
        print(*top_na_platforms, sep=', ')
        print('4) Самые высокие средние продажи в Японии у', end=' ')
        print(*top_jp_publishers, sep=', ')
    
        print(f'5) {eu_jp_comparison} - столько игр продались лучше в Европе, чем я Японии') 



    data = get_data()
    Global_sales_top = get_Global_sales_top(data)
    EU_genre_top = get_EU_genre_top(data)
    NA_platforms_top = get_NA_platforms_top(data)
    JP_top_publishers = get_JP_top_publishers(data)
    EU_more_JP = get_EU_more_JP(data)

    print_data(Global_sales_top, EU_genre_top, NA_platforms_top, JP_top_publishers, EU_more_JP)

v_ivannikov_lesson_3 = v_ivannikov_lesson_3()