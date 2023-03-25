from datetime import datetime, timedelta, date
import pandas as pd
from io import StringIO
import requests
import pandas as pd
import pandahouse as ph
import numpy as np

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'a.markovicheva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 10),
}

# Интервал запуска DAG
schedule_interval = '0 21 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_lesson_6_task():
    # получаем датафрейм с числом просмотров и лайков контента
    @task()
    def get_feed_actions():
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database': 'simulator_20221220',
                      'user': 'student',
                      'password': 'dpo_python_2020'
                      }
        q = """
            SELECT
              t.event_date AS event_date,
              t.user_id AS user_id,
              t.os AS os,
              t.gender AS gender,
              t.age AS age,
              t2.likes AS likes,
              t3.views AS views
            FROM
              (
                SELECT
                  DISTINCT user_id,
                  toDate(time) as event_date,
                  os,
                  gender,
                  age
                FROM
                  simulator_20221220.feed_actions
                ORDER BY
                  event_date,
                  user_id
              ) AS t
              LEFT JOIN (
                SELECT
                  user_id,
                  COUNT(action) AS likes,
                  toDate(time) as event_date
                FROM
                  simulator_20221220.feed_actions
                WHERE
                  action = 'like'
                GROUP BY
                  user_id,
                  event_date
                ORDER BY
                  event_date,
                  user_id
              ) AS t2 ON t.user_id = t2.user_id
              AND t.event_date = t2.event_date
              LEFT JOIN (
                SELECT
                  user_id,
                  COUNT(action) AS views,
                  toDate(time) as event_date
                FROM
                  simulator_20221220.feed_actions
                WHERE
                  action = 'view'
                GROUP BY
                  user_id,
                  event_date
                ORDER BY
                  event_date,
                  user_id
              ) AS t3 ON t.user_id = t3.user_id
              AND t.event_date = t3.event_date
            ORDER BY
              event_date,
              user_id
            """
        df_feed_actions = ph.read_clickhouse(q, connection=connection)
        return df_feed_actions

    # получаем датафрейм с сообщениями
    @task()
    def get_message_actions():
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database': 'simulator_20221220',
                      'user': 'student',
                      'password': 'dpo_python_2020'
                      }
        q = """
            SELECT
              t.event_date AS event_date,
              t.user_id AS user_id,
              t.os AS os,
              t.gender AS gender,
              t.age AS age,
              t2.messages_received AS messages_received,
              t3.messages_sent AS messages_sent,
              t4.users_sent AS users_sent,
              t5.users_received AS users_received
            FROM
              (
                SELECT
                  DISTINCT user_id,
                  toDate(time) as event_date,
                  os,
                  gender,
                  age
                FROM
                  simulator_20221220.feed_actions
                ORDER BY
                  event_date,
                  user_id
              ) AS t
              LEFT JOIN (
                SELECT
                  reciever_id,
                  COUNT(*) AS messages_received,
                  toDate(time) as event_date
                FROM
                  simulator_20221220.message_actions
                GROUP BY
                  reciever_id,
                  event_date
                ORDER BY
                  event_date,
                  reciever_id
              ) AS t2 ON t.user_id = t2.reciever_id
              AND t.event_date = t2.event_date
              LEFT JOIN (
                SELECT
                  user_id,
                  toDate(time) as event_date,
                  COUNT(*) AS messages_sent
                FROM
                  simulator_20221220.message_actions
                GROUP BY
                  user_id,
                  event_date
                ORDER BY
                  event_date,
                  user_id
              ) AS t3 ON t.user_id = t3.user_id
              AND t.event_date = t3.event_date
              LEFT JOIN (
                SELECT
                  user_id,
                  toDate(time) as event_date,
                  COUNT(DISTINCT reciever_id) AS users_sent
                FROM
                  simulator_20221220.message_actions
                GROUP BY
                  user_id,
                  event_date
                ORDER BY
                  event_date,
                  user_id
              ) AS t4 ON t.user_id = t4.user_id
              AND t.event_date = t4.event_date
              LEFT JOIN (
                SELECT
                  reciever_id,
                  toDate(time) as event_date,
                  COUNT(DISTINCT user_id) AS users_received
                FROM
                  simulator_20221220.message_actions
                GROUP BY
                  reciever_id,
                  event_date
                ORDER BY
                  event_date,
                  reciever_id
              ) AS t5 ON t.user_id = t5.reciever_id
              AND t.event_date = t5.event_date
            """
        df_message_actions = ph.read_clickhouse(q, connection=connection)
        return df_message_actions

    # объединяем две таблицы в одну
    @task
    def get_united_df(df_feed_actions, df_message_actions):
        united_df = df1.merge(df_feed_actions, df_message_actions, how='outer')
        return united_df

    # срез по полу
    @task
    def get_gender_cube(united_df):
        gender_cube = united_df.groupby(['event_date', 'gender']) \
            .agg({'likes': 'sum', 'views': 'sum', 'messages_received': 'sum', 'messages_sent': 'sum',
                  'users_received': 'sum', 'users_sent': 'sum'}) \
            .reset_index()
        gender_cube.rename(columns={'gender': 'dimension_value'}, inplace=True)
        gender_cube.insert(1, 'dimension', 'gender')
        return gender_cube

    # срез по возрасту
    @task
    def get_age_cube(united_df):
        age_cube = united_df.groupby(['event_date', 'age']) \
            .agg({'likes': 'sum', 'views': 'sum', 'messages_received': 'sum', 'messages_sent': 'sum',
                  'users_received': 'sum', 'users_sent': 'sum'}) \
            .reset_index()
        age_cube.rename(columns={'age': 'dimension_value'}, inplace=True)
        age_cube.insert(1, 'dimension', 'age')
        return age_cube

    # срез по платформе
    @task
    def get_os_cube(united_df):
        os_cube = united_df.groupby(['event_date', 'os']) \
            .agg({'likes': 'sum', 'views': 'sum', 'messages_received': 'sum', 'messages_sent': 'sum',
                  'users_received': 'sum', 'users_sent': 'sum'}) \
            .reset_index()
        os_cube.rename(columns={'os': 'dimension_value'}, inplace=True)
        os_cube.insert(1, 'dimension', 'os')
        return os_cube

    # объединяем три датафрейма в один
    @task
    def create_res_df(gender_cube, age_cube, os_cube):
        res_df = pd.concat([gender_cube, age_cube, os_cube])
        return res_df

    # создаём таблицу в ClickHouse, если она не существует, выгружаем данные
    @task
    def create_and_load(res_df):
        cn = {'host': 'https://clickhouse.lab.karpov.courses',
              'database': 'test',
              'user': 'student-rw',
              'password': '656e2b0c9c'
              }
        q3 = """
            CREATE TABLE IF NOT EXISTS test.anna_marko_lesson_6 ( 
                event_date Date,
                dimension String,
                dimension_value String,
                likes UInt64,
                views UInt64,
                messages_received UInt64,
                messages_sent UInt64,
                users_received UInt64,
                users_sent UInt64
                ) 

            ENGINE = MergeTree
            ORDER BY (country_id)
            """

        ph.execute(connection=cn, query=q3)
        ph.to_clickhouse(res_df, table='anna_marko_lesson_6', connection=cn, index=False)

    # удаляем данные за сегодня - они будут в любом случае подгружены отдельным таском
    @task
    def delete_today_info():
        cn = {'host': 'https://clickhouse.lab.karpov.courses',
              'database': 'test',
              'user': 'student-rw',
              'password': '656e2b0c9c'
              }
        q = """
            ALTER TABLE test.anna_marko_lesson_6
            DELETE WHERE toDate(event_date) = today()
            """
        ph.execute(connection=cn, query=q)

    # загружаем данные за сегодня
    @task
    def os_cube(united_df):
        cn = {'host': 'https://clickhouse.lab.karpov.courses',
              'database': 'test',
              'user': 'student-rw',
              'password': '656e2b0c9c'
              }

        today_df = united_df[united_df.event_date.dt.date == date.today()]
        ph.to_clickhouse(today_df, table='anna_marko_lesson_6', connection=cn, index=False)

    df_feed_actions = get_feed_actions()
    df_message_actions = get_message_actions()
    united_df = get_united_df(df_feed_actions, df_message_actions)
    gender_cube = get_gender_cube(united_df)
    age_cube = get_age_cube(united_df)
    os_cube = get_os_cube(united_df)
    res_df = create_res_df(gender_cube, age_cube, os_cube)
    create_and_load(res_df)
    delete_today_info()
    os_cube(united_df)


dag_lesson_6_task = dag_lesson_6_task()