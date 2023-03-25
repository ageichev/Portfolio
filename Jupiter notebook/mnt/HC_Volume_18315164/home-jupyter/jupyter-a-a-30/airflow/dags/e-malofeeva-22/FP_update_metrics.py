#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import BytesIO
import requests

from airflow import DAG
from airflow.decorators import dag, task

default_args = {
    'owner': 'e.malofeeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 31),
    'schedule_interval' : '0 24 * * *'
}


@dag(default_args = default_args, catchup = False)
def update_metrics():
    # таска считывания groups
    @task()
    def get_data_groups():
        req1 = requests.get('https://s856sas.storage.yandex.net/rdisk/68e81c82e181de5daf7388627f052597b13b536aa0a80fef5ee9ed42c29fd847/62ffbd8d/fKqInKw3d7bLFOeFnMGnhLWIS9AmXMMXWGR17A-BkO3-qg5Wp8tUAp7lxoJJvJyG8pjltSuuxShICmGl2FDiJhbsM4PC04KdB-NoZUjfeOOr8npumZHI4midPdWhecNq?uid=0&filename=%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82_2_groups.csv&disposition=attachment&hash=FjLs%2B6AJdyaRoyFMFeL4raj9RoBP3hYIuXfguWvuEGcjdjYEIrmN7ZN/eZ32Z77Cq/J6bpmRyOJonT3VoXnDag%3D%3D&limit=0&content_type=text%2Fplain&owner_uid=125175473&fsize=724984&hid=4b9098e1d545e562c779de19f9f64e2f&media_type=spreadsheet&tknv=v2&rtoken=hnecBu3cpsUk&force_default=no&ycrid=na-2b0f5c1337be7f69292be597866703f9-downloader21h&ts=5e69aca10bd40&s=0494cd844e1de7e7cd86ef2596ed29eb129ca6f9be7725452a0c695808e7636a&pb=U2FsdGVkX196IfjqXhBcL2stXy2LnkQ7NVKd0kJLP_0-SOAlIGqhpZytKXChCTr_taw1cyV4-HCoJZq2x7i-IM6Cn1-mjfxFb9VT1BKqfptmlay7nPbkfLszesK5TypY')
        data1 = req1.content
        df_groups = pd.read_csv(BytesIO(data1), header = None, skiprows = 1, names = ['student_id', 'grp'], sep = ';')
        return df_groups.to_csv('df_groups.csv')
    
        # таска считывания active_studs
    @task()
    def get_data_active_studs():
        req2 = requests.get('https://s432vla.storage.yandex.net/rdisk/3a40f377f696432a2588a986b2af49044dd68afb5b18d69010f8282c1f40f146/62ffbec8/n3N0SQYM2SoDHEJIQgKt2ujEDw5jYN17sSz14UE4iwGyYlrHRmnPoV9WI8mqa1dmF6Qt_88ffG90wBmmEpDhFQ==?uid=0&filename=%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82_2_active_studs.csv&disposition=attachment&hash=zWS0uLxK3MRXd/4/IErmDFQBGvbpGD1qm/aJx/k6EUUtE6DaDI0uGkDT4cHh/WJbq/J6bpmRyOJonT3VoXnDag%3D%3D&limit=0&content_type=text%2Fplain&owner_uid=125175473&fsize=65131&hid=3ed23002f82477fd8c7d4a21c17d8034&media_type=spreadsheet&tknv=v2&rtoken=VLQnNMBdZHav&force_default=no&ycrid=na-96300cdc146eb8e9f9841f7d1c03ae4f-downloader11f&ts=5e69adcd74200&s=298f3f9a6a0b4dcfcd30b5e2531375074e1817b4485c4e2acc2cdda94c064471&pb=U2FsdGVkX19nAru0O76sUeX5I7sjU7s3OfVwfBSgj3bVELERwhe44uxz5-Qbg-huJf5HKtETRz_t9FsHXfXWWfoU1L7utUUpsCWTRvpOBWo')
        data2 = req2.content
        df_active_studs = pd.read_csv(BytesIO(data2), header = None, skiprows = 1, names = ['student_id'])
        df_active_studs['activity'] = 'yes'
        return df_active_studs.to_csv('df_active_studs.csv')
    
        # таска считывания checks
    @task()
    def get_data_checks():
        req3 = requests.get('https://s251vla.storage.yandex.net/rdisk/2ed0cef7d3eeb388a1cfef16c898d547abd95ff3590fc483208d4d0329f39e40/62ffbf91/n3N0SQYM2SoDHEJIQgKt2psRrG10OKyAasqgpda4iX7NDTSjoP6nmHZtzOqJXgQH436kYOx6cWVM8zgvGq_zHg==?uid=0&filename=%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82_2_checks.csv&disposition=attachment&hash=TeGedpkC9yRvIkaaNDf1zC3bFkr2Kvo9MvW7kYjfHc5svktv83UZ47kn6%2BaauC1pq/J6bpmRyOJonT3VoXnDag%3D%3D&limit=0&content_type=text%2Fplain&owner_uid=125175473&fsize=7764&hid=caf94adff3374e1c1a09fba3aae11e50&media_type=spreadsheet&tknv=v2&rtoken=lCY7FQdHOPMD&force_default=no&ycrid=na-5f259d0cfa6acec9fbf8a1703b68f794-downloader2e&ts=5e69ae8d24640&s=5e3efcf73091f37afe750f8ddb7beb1773a95e621febe7d1acb402b6068f975a&pb=U2FsdGVkX19V-bWbgCAsy4qge7CA2oQF6lBXPRtQz-MNZMZkdtcatn1jMBpFi1o_th5_ltM41rhn_qRsTsB_Kcy6rHTqxIQjj_CLNuwylEM')
        data3 = req3.content
        df_checks = pd.read_csv(BytesIO(data3), header = None, skiprows = 1, names = ['student_id', 'rev'], sep = ';')
        return df_checks.to_csv('df_checks.csv')
    
    # таска считывания add
    @task()
    def get_data_groups_add():
        req4 = requests.get('https://s750sas.storage.yandex.net/rdisk/f06bf3fb0ce3d7ed783c715aa18a0389af6e9d4efa00d64ca5f53aac6182abf1/62ffc01e/fKqInKw3d7bLFOeFnMGnhGAdJ2MSdW82i4_ghmwgsZlWhJkpRytn07Ls4kX484X-lezQy4loqSz9AFrIeHDnv8QEHwKSTiYbb_hzq1q6XQir8npumZHI4midPdWhecNq?uid=0&filename=%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82_2_group_add.csv&disposition=attachment&hash=0ertv33lnyTdzTu9NbdN9MtgpuTSXleSHZ%2BvhK85v05jGwJs7DHSrj4ununKsRcfq/J6bpmRyOJonT3VoXnDag%3D%3D&limit=0&content_type=text%2Fplain&owner_uid=125175473&fsize=949&hid=e8dcec35987c367a56f82404a22463b7&media_type=spreadsheet&tknv=v2&rtoken=x7sQvDBvEcVd&force_default=no&ycrid=na-6ae8cf4f9feb8da91b76dd02295986e5-downloader2e&ts=5e69af139c380&s=28a8f791e2b55bce136323bbd03a635c3fae77f3b10cbcaff7b7a06d71e43fd6&pb=U2FsdGVkX18Wzaw8soN5pjC05gU8iWY8NCgpwE3Bgchzktb4Mdsjxki6tJpzFvDFmTxaIG_MODh8nBJa5LDlTyyWkkVUpW84A0_EDWJ33f5Fytt1LWGLsoVjXXtP2sIv')
        data4 = req4.content
        df_groups_add = pd.read_csv(BytesIO(data4), header = None, skiprows = 1, names = ['student_id', 'grp'])
        return df_groups_add.to_csv('df_groups_add.csv')

    # таска преобразования фреймов
    @task()
    def union_df(df_groups, df_groups_add, df_checks, df_active_studs):
        df_groups = pd.read_csv('df_groups.csv')
        df_groups_add = pd.read_csv('df_groups_add.csv')
        df_checks = pd.read_csv('df_checks.csv')
        df_active_studs = pd.read_csv('df_active_studs.csv')
        df_groups = pd.concat([df_groups, df_groups_add])
        df = df_groups.merge(df_active_studs, how = 'left', on = 'student_id').fillna('no')
        df = df.merge(df_checks, how = 'left', on = 'student_id').fillna(0)
        return df.to_csv('df.csv')

    # таска расчета метрик
    @task()
    def metrics(df):
        df = pd.read_csv('df.csv')
        users_total = df.groupby('grp', as_index = False)         .agg({'student_id' : 'nunique'})         .rename(columns = {'student_id' : 'users_total'})

        users_rev = df.query('rev > 0')         .groupby('grp', as_index = False)         .agg({'student_id' : 'nunique'})         .rename(columns = {'student_id' : 'users_with_revenue'})

        rev_total = df.groupby('grp', as_index = False)         .agg({'rev' : 'sum'})         .rename(columns = {'rev' : 'total_rev'})

        metrics = users_total.merge(rev_total, how = 'inner', on = 'grp')         .merge(users_rev, how = 'inner', on = 'grp')

        metrics['CR'] = metrics.users_with_revenue / metrics.users_total
        metrics['ARPAU'] = metrics.total_rev / metrics.users_total
        metrics['ARPPU'] = metrics.total_rev / metrics.users_with_revenue

        return metrics.to_csv('metrics.csv')
        
    df_groups = get_data_groups()
    df_active_studs = get_data_active_studs()
    df_checks = get_data_checks()
    df_groups_add = get_data_groups_add()
    df = union_df(df_groups, df_groups_add, df_checks, df_active_studs)
    metrics = metrics(df)
    
update_metrics = update_metrics()

