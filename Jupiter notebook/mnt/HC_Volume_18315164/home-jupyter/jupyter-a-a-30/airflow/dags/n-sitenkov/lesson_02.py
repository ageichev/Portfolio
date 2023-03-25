import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = "http://s3.amazonaws.com/alexa-static/top-1m.csv.zip"
TOP_1M_DOMAINS_FILE = "top-1m.csv"



def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, "w") as f:
        f.write(top_data)


def get_top_10_domain_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=["rank", "domain"])
    
    df["domain_zones"] = df["domain"].apply(lambda x: x.split(".")[-1])
    
    res_top_10_domain = df.groupby("domain_zones", as_index=False) \
                          .agg({"domain": "count"}) \
                          .sort_values("domain", ascending=False) \
                          .head(10)

    with open("res_top_10.csv", "w") as f:
        f.write(res_top_10_domain.to_csv(index=False, header=False))


def get_the_longest_domain_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=["rank", "domain"])
    
    df["domain_lengths"] = df["domain"].str.len()
    
    domain_length = df.sort_values("domain_lengths", ascending=False).head(1)[["domain"]]

    with open("res_longest_name.csv", "w") as f:
        f.write(domain_length.to_csv(index=False, header=False))


def get_airflow_com_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=["rank", "domain"])
    
    res_airflow_com_rank = df[df["domain"].str.contains("airflow.com")][["rank"]]

    with open("res_airflow_com_rank.csv", "w") as f:
        f.write(res_airflow_com_rank.to_csv(index=False, header=False))

def print_data():
    with open("res_top_10.csv", "r") as f:
        res_top_10 = f.read()

    with open("res_longest_name.csv", "r") as f:
        res_longest_name = f.read()

    with open("res_airflow_com_rank.csv", "r") as f:
        res_airflow_com_rank = f.read()

    print(f"Топ-10 доменных зон по численности доменов: {res_top_10}")
    print(f"Домен с самым длинным именем: {res_longest_name}")
    print(f"Позиция Airflow.com в общей ранке: {res_airflow_com_rank}")



default_args = {
    "owner": "n-sitenkov-26",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2022, 11, 28),
}

schedule_interval = "30 5 * * *"

dag_lesson_2 = DAG("lesson_2_n_sitenkov",
                    default_args=default_args,
                    schedule_interval=schedule_interval)

t1 = PythonOperator(task_id="get_data",
                    python_callable=get_data,
                    dag=dag_lesson_2)

t2 = PythonOperator(task_id="get_top_10_domain_zones",
                    python_callable=get_top_10_domain_zones,
                    dag=dag_lesson_2)

t3 = PythonOperator(task_id="get_the_longest_domain_name",
                    python_callable=get_the_longest_domain_name,
                    dag=dag_lesson_2)

t4 = PythonOperator(task_id="get_airflow_com_rank",
                    python_callable=get_airflow_com_rank,
                    dag=dag_lesson_2)

t5 = PythonOperator(task_id="print_data",
                    python_callable=print_data,
                    dag=dag_lesson_2)

t1 >> [t2, t3, t4] >> t5
