import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

import seaborn as sns
from scipy import stats
import scipy
import matplotlib.pyplot as plt



from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


import requests
from urllib.parse import urlencode
# group_path = '/mnt/HC_Volume_18315164/home-jupyter/jupyter-v-syropjatov-25/Finals/Проект_2_groups.csv'
# group_after2day_path ='/mnt/HC_Volume_18315164/home-jupyter/jupyter-v-syropjatov-25/Finals/Проект_2_group_add.csv'
# users_in_experiment_path = '/mnt/HC_Volume_18315164/home-jupyter/jupyter-v-syropjatov-25/Finals/Проект_2_active_studs.csv' 
# checks_path ='/mnt/HC_Volume_18315164/home-jupyter/jupyter-v-syropjatov-25/Finals/Проект_2_checks.csv'
base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
group = 'https://disk.yandex.ru/d/58Us0DWOzuWAjg'
group_after2day = 'https://disk.yandex.ru/d/3aARY-P9pfaksg' 
users_in_experiment = 'https://disk.yandex.ru/d/prbgU-rZpiXVYg' # Сюда вписываете вашу ссылку
checks = 'https://disk.yandex.ru/d/84hTmELphW2sqQ'

# Получаем загрузочную ссылку
final_url1 = base_url + urlencode(dict(public_key=group))
response1 = requests.get(final_url1)
download_url_group = response1.json()['href']
download_url_group

final_url2 = base_url + urlencode(dict(public_key=group_after2day))
response2 = requests.get(final_url2)
download_url_group_after2day = response2.json()['href']
download_url_group_after2day

final_url3 = base_url + urlencode(dict(public_key=users_in_experiment))
response3 = requests.get(final_url3)
download_url_users_in_experiment = response3.json()['href']
download_url_users_in_experiment

final_url4 = base_url + urlencode(dict(public_key=checks))
response4 = requests.get(final_url4)
download_url_checks = response4.json()['href']
download_url_checks

group_path  =  download_url_group
group_after2day_path = download_url_group_after2day
users_in_experiment_path = download_url_users_in_experiment 
checks_path = download_url_checks

# group_path  =  '/var/lib/airflow/airflow.git/dags/v.syropjatov/Проект_2_groups.csv'
# group_after2day_path ='/var/lib/airflow/airflow.git/dags/v.syropjatov/Проект_2_group_add.csv'
# users_in_experiment_path = '/var/lib/airflow/airflow.git/dags/v.syropjatov/Проект_2_active_studs.csv' 
# checks_path ='/var/lib/airflow/airflow.git/dags/v.syropjatov/Проект_2_checks.csv'

group_file =  'Проект_2_groups.csv'
group_after2day_file ='Проект_2_group_add.csv'
users_in_experiment_file = 'Проект_2_active_studs.csv' 
checks_file ='Проект_2_checks.csv'





default_args = {
    'owner': 'v.syropjatov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 14),
    'schedule_interval': '0 14 * * *'
}



@dag(default_args=default_args, catchup=False)
def v_syropyatov_air_final():
    @task(retries=3)
    def get_data(group_path,group_after2day_path,users_in_experiment_path,checks_path):
        group = pd.read_csv(group_path)
        group_after2day =pd.read_csv(group_after2day_path)
        users_in_experiment = pd.read_csv(users_in_experiment_path) 
        checks =pd.read_csv(checks_path)
        return group,group_after2day,users_in_experiment,checks
       

    @task(retries=4, retry_delay=timedelta(10))
    def get_ab_results(group_path,group_after2day_path,users_in_experiment_path,checks_path):
        group = pd.read_csv(group_path, sep=';')
        group_after2day =pd.read_csv(group_after2day_path)
        users_in_experiment = pd.read_csv(users_in_experiment_path) 
        checks =pd.read_csv(checks_path,sep=';') 
        group_after2day = group_after2day.rename(columns={ group_after2day.columns[0]: "grp",group_after2day.columns[1]: "id" })
        group_all= pd.concat([group, group_after2day])
        Grouped_all1 = pd.merge(users_in_experiment, checks,  how='left', left_on=['student_id'], right_on = ['student_id'])
        Grouped_all_fin = pd.merge(Grouped_all1, group_all,  how='outer', left_on=['student_id'], right_on = ['id'])
        Grouped_all_fin_excl_nan = Grouped_all_fin[Grouped_all_fin.student_id.notnull()]
        Grouped_for_analysis = Grouped_all_fin_excl_nan.drop(['student_id', 'id'], axis=1)
        sns.boxplot(data=Grouped_for_analysis, x="grp", y="rev",orient='v')
        sns.distplot(Grouped_for_analysis.rev)
        sns.distplot(np.log(Grouped_for_analysis.rev))
        Grouped_for_analysis_no_null = Grouped_for_analysis.dropna()
        A = Grouped_for_analysis_no_null[Grouped_for_analysis_no_null.grp=='A'].rev
        B = Grouped_for_analysis_no_null[Grouped_for_analysis_no_null.grp=='B'].rev
        mean_check_A = A.mean()
        mean_check_B = B.mean()
        median_check_A = A.median()
        median_check_B = B.median()
        return {'mean_check_A':mean_check_A,'mean_check_B': mean_check_B,'median_check_A': median_check_A,'median_check_B': median_check_B}

    @task()
    def print_data(x_stat):

        context = get_current_context()
        date = context['ds']

        mean_check_A, mean_check_B, median_check_A, median_check_B = x_stat['mean_check_A'],x_stat['mean_check_B'],x_stat['median_check_A'], x_stat['median_check_B']


        print(f'''For the date: {date} results of the experiment are: 
        Group A mean reciept is {mean_check_A} and median reciept is {median_check_A}
        Group B mean reciept is {mean_check_B} and median reciept is {median_check_B}
        For the date {date} expirement is going on well. ''')
      
    x_stat = get_ab_results(group_path,group_after2day_path,users_in_experiment_path,checks_path)
    
    
    print_data(x_stat)
    
v_syropyatov_air_final = v_syropyatov_air_final()
