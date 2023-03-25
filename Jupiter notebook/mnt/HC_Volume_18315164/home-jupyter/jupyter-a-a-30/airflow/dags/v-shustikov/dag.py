from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# import logging
# logging.basicConfig(level=logging.INFO)
# logging.info('123')


dag_params = {
    'dag_id': "shustikov_dag_v3",  # Dag id
    'description': 'Процесс подгружает дополнительный файл, пересчитывает метрики, сохраняет графики',
    'start_date': datetime(2023, 1, 1),
    'schedule_interval': '@once',  # or cron format for example: '0 0 * * *' - see crontab.guru
    'catchup': False
}

additional_filepath = 'https://disk.yandex.ru/d/eyM-cfHKAjVl2A'
updated_metrics_filepath = 'metric_file.csv'
metrics_image_filepath = "metric.png"
URL = 'https://cloud-api.yandex.net/v1/disk/resources'
TOKEN = 'y0_AgAAAABRCl6NAADLWwAAAADbzoB--DJNsxOPSp6OBWy2M0-4A5gROec'
headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'OAuth {TOKEN}'}
date_path = datetime.today().strftime('%Y-%m-%d')
path_save_file = f'First_Project/{date_path}' 


#import os
#print(os.getcwd())
#print(os.listdir())

def get_df_yadisk(public_key, sep): # функция считывания данных
    import requests
    import pandas as pd
    from urllib.parse import urlencode 
    from datetime import timedelta
    import json
    # используем api 
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'  
    # получаем url 
    final_url = base_url + urlencode(dict(public_key=public_key)) 
    response = requests.get(final_url) 
    download_url = response.json()['href'] 
    # загружаем файл в df 
    download_response = requests.get(download_url)
    if '404' in download_response:
        return download_response
    else:
        return pd.read_csv(download_url, sep)

def _save_updated_metrics_csv(additional_filepath, updated_metrics_filepath, sep=','):
    import pandas as pd
    import warnings
    warnings.filterwarnings("ignore")
    import matplotlib.pyplot as plt
    import seaborn as sns


    groups_part_1 = get_df_yadisk('https://disk.yandex.ru/d/_J4A_QxEOmUEoA', ';')
    groups_part_1 = groups_part_1.rename(columns={groups_part_1.columns[0]: "student_id", groups_part_1.columns[1]: "grp"})
    groups_part_2 = get_df_yadisk(additional_filepath, sep)
    groups_part_2 = groups_part_2.rename(columns={groups_part_2.columns[0]: "student_id", groups_part_2.columns[1]: "grp"})
    active_user = get_df_yadisk('https://disk.yandex.ru/d/_AvZi6SoGzWAyw', ',')
    checks = get_df_yadisk('https://disk.yandex.ru/d/BKqy6J89P6NmoA', ';') 
    groups_part = pd.concat([groups_part_1, groups_part_2])
    groups_part.drop_duplicates()
    active_user['activ'] = 1
    checks['check'] = 1
    A_B_testing_full = groups_part.merge(checks, how='left', on='student_id').merge(active_user, how='left',
                                                                                    on='student_id')
    A_B_testing_full = A_B_testing_full.fillna(0)
    CR_A = round(A_B_testing_full[A_B_testing_full.grp == 'A'].check.mean(), 5)
    CR_B = round(A_B_testing_full[A_B_testing_full.grp == 'B'].check.mean(), 5)
    ARPU_A = round(A_B_testing_full.rev[A_B_testing_full.grp == 'A'].sum() / A_B_testing_full[
        A_B_testing_full.grp == 'A'].rev.count(), 3)
    ARPU_B = round(A_B_testing_full[A_B_testing_full.grp == 'B'].rev.sum() / A_B_testing_full[
        A_B_testing_full.grp == 'B'].rev.count(), 3)
    ARPPU_A = round(
        A_B_testing_full[(A_B_testing_full.rev > 0) & (A_B_testing_full.grp == 'A')].rev.sum() / A_B_testing_full[
            (A_B_testing_full.rev > 0) & (A_B_testing_full.grp == 'A')].rev.count(), 3)
    ARPPU_B = round(
        A_B_testing_full[(A_B_testing_full.rev > 0) & (A_B_testing_full.grp == 'B')].rev.sum() / A_B_testing_full[
            (A_B_testing_full.rev > 0) & (A_B_testing_full.grp == 'B')].rev.count(), 3)
    new = pd.DataFrame(data={'CR': [CR_A, CR_B], 'ARPU': [ARPU_A, ARPU_B], 'ARPPU': [ARPPU_A, ARPPU_B]},
                       index=['A', 'B'])
    new = new.reset_index()
    new.to_csv(updated_metrics_filepath)
    print(f'{updated_metrics_filepath} saved')
    return True


def _resave_images(updated_metrics_filepath, metrics_image_filepath):
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns

    df = pd.read_csv(updated_metrics_filepath)
    fig, axs = plt.subplots(1, 3, sharex=True, figsize=(20, 5))
    fig.suptitle('Соотношение метрик в разных группа')
    sns.barplot(data=df, x="index", y='CR', ax=axs[0])
    sns.barplot(data=df, x="index", y='ARPU', ax=axs[1])
    sns.barplot(data=df, x="index", y='ARPPU', ax=axs[2])
    fig.savefig(metrics_image_filepath)
    print('Графики сохранены')
    return True


def _remove_updated_metrics_from_local(updated_metrics_filepath):
    import os
    os.remove(updated_metrics_filepath)
    return True

def create_folder(path_save_file):
    import requests
    """Создание папки. \n path: Путь к создаваемой папке."""
    requests.put(f'{URL}?path={path_save_file}', headers=headers)

def upload_file(loadfile, savefile, replace=True):
    import requests
    """Загрузка файла.
    savefile: Путь к файлу на Диске
    loadfile: Путь к загружаемому файлу
    replace: true or false Замена файла на Диске"""
    res = requests.get(f'{URL}/upload?path={savefile}&overwrite={replace}', headers=headers).json()
    with open(loadfile, 'rb') as f:
        try:
            requests.put(res['href'], files={'file':f})
        except KeyError:
            print(res)

def _save_file_in_yandex_disk(path_save_file,metrics_image_filepath):
    create_folder(path_save_file)
    upload_file(f'{metrics_image_filepath}',f'{path_save_file}/{metrics_image_filepath}')
    return True

def _remove_metrics_image_from_local(metrics_image_filepath):
    import os
    os.remove(metrics_image_filepath)
    return True 

with DAG(**dag_params) as dag:
    save_updated_metrics_csv = PythonOperator(
        task_id='save_updated_metrics_csv',
        python_callable=_save_updated_metrics_csv,
        op_kwargs={
            "additional_filepath": additional_filepath,
            'updated_metrics_filepath': updated_metrics_filepath
        }
    )

    resave_images = PythonOperator(
        task_id='resave_images',
        python_callable=_resave_images,
        op_kwargs={
            "updated_metrics_filepath": updated_metrics_filepath,
            'metrics_image_filepath': metrics_image_filepath
        }
    )

    remove_updated_metrics_from_local = PythonOperator(
        task_id='remove_updated_metrics_from_local',
        python_callable=_remove_updated_metrics_from_local,
        op_kwargs={
            "updated_metrics_filepath": updated_metrics_filepath,
        }
    )

    save_file_in_yandex_disk = PythonOperator(
        task_id='save_file_in_yandex_disk',
        python_callable=_save_file_in_yandex_disk,
        op_kwargs={
            "path_save_file": path_save_file,
            'metrics_image_filepath': metrics_image_filepath
        }
    )
    
    remove_metrics_image_from_local = PythonOperator(
        task_id='remove_metrics_image_from_local',
        python_callable=_remove_metrics_image_from_local,
        op_kwargs={
            "metrics_image_filepath": metrics_image_filepath,
        }
    )
    

    save_updated_metrics_csv >> resave_images >> remove_updated_metrics_from_local >> save_file_in_yandex_disk >> remove_metrics_image_from_local
