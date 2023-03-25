import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import timedelta, datetime
from io import StringIO
import telegram

default_args = {
    'owner': 'k.konoplev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 23)
}

schedule_interval = '0 15 * * *'

chat_id = 279775042
bot_token = '5473693098:AAFtdYrRPOTBHDlnOyX9xx9PmTfB-36xV6E'
    
def tg_message(context):
    dag_id = context['dag'].dag_id
    date = context['ds']
    message = f'Dag {dag_id} completed on {date}'
    bot = telegram.Bot(token=bot_token)
    bot.send_message(chat_id=chat_id, text=message)


@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def k_konoplev_26_lesson3():

    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        login = 'k-konoplev-26'
        year = 1994 + hash(f'{login}') % 23
        df = df.query("Year == @year").to_csv(index=False)
        return df, year

    @task()
    def task1(data):
        bestseller = pd.read_csv(StringIO(data[0])).groupby('Name', as_index=False). \
                    Global_Sales.sum(). \
                    sort_values('Global_Sales', ascending=False). \
                    head(1). \
                    Name.values[0]
        return bestseller

    @task()
    def task2(data):
        genre = pd.read_csv(StringIO(data[0])).groupby('Genre', as_index=False).EU_Sales.sum()
        top_genre = genre[genre.EU_Sales == genre.EU_Sales.max()].Genre.values[0]
        return top_genre

    @task()
    def task3(data):
        platform = pd.read_csv(StringIO(data[0])).query("NA_Sales > 1").groupby('Platform', as_index=False).Name.count()
        top_platform = platform[platform.Name == platform.Name.max()].Platform.values[0]
        return top_platform

    @task()
    def task4(data):
        publisher = pd.read_csv(StringIO(data[0])).groupby('Publisher', as_index=False).JP_Sales.mean()
        top_publisher = publisher[publisher.JP_Sales == publisher.JP_Sales.max()].Publisher.values[0]
        return top_publisher

    @task()
    def task5(data):
        games = pd.read_csv(StringIO(data[0])).groupby('Name', as_index=False).agg({'JP_Sales': sum, 'EU_Sales': sum})
        task5_result = games[games.JP_Sales < games.EU_Sales].shape[0]
        return task5_result

    @task(on_success_callback=tg_message)
    def print_data(bestseller, top_genre, top_platform, top_publisher, task5_result, data):
        year = data[1]
        print(f'Data for the year {year}: \n')
        print(f"The world's bestseller was: {bestseller}")
        print(f'The top genre in Europe was(were): {top_genre}')
        print(f'The top platform was(were): {top_platform}')
        print(f'The top publisher in Japan was(were): {top_publisher}')
        print(f'The number of games that were sold better in Europe than in Japan was: {task5_result}')
    
    
    data = get_data()
    bestseller = task1(data)
    top_genre = task2(data)
    top_platform = task3(data)
    top_publisher = task4(data)
    task5_result = task5(data)
    printdata = print_data(bestseller, top_genre, top_platform, top_publisher, task5_result, data)

    
result = k_konoplev_26_lesson3() 
    