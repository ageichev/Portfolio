{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import requests\n",
    "import pandas as pd\n",
    "from datetime import timedelta\n",
    "from datetime import datetime"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "from airflow import DAG\n",
    "from airflow.operators.python import PythonOperator"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'\n",
    "TOP_1M_DOMAINS_FILE = 'top-1m.csv'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_data():\n",
    "    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке\n",
    "    top_doms = pd.read_csv(TOP_1M_DOMAINS)\n",
    "    top_data = top_doms.to_csv(index=False)\n",
    "\n",
    "    with open(TOP_1M_DOMAINS_FILE, 'w') as f:\n",
    "        f.write(top_data)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_stat_zone():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['adres', 'zone'], sep='.')\n",
    "    top_data_zone_10 = top_data_df.groupby('zone', as_index=False)\\\n",
    "        .agg({'adres':'count'})\\\n",
    "        .rename(columns={'adres':'count'})\\\n",
    "        .sort_values('count', ascending=False)\\\n",
    "        .head(10)\n",
    "    with open('top_data_zone_10.csv', 'w') as f:\n",
    "        f.write(top_data_zone_10.to_csv(index=False, header=False))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_stat_len():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    z =[]\n",
    "    for i in range(len(top_data_df)):\n",
    "        z.append(len(top_data_df['domain'][i]))\n",
    "    top_data_df['lenght']=z\n",
    "    top_data_len_10 = top_data_df.sort_values('lenght', ascending=False).head(10)\n",
    "    with open('top_data_len_10.csv', 'w') as f:\n",
    "        f.write(top_data_len_10.to_csv(index=False, header=False))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 39,
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_stat_airflow():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    if top_data_df[top_data_df['domain']=='airflow.com'].empty == True:\n",
    "        airfloy = {'There is no airflow.com domain in data'}\n",
    "        top_data_airfloy = pd.DataFrame(data=airfloy)\n",
    "    else:\n",
    "        top_data_airfloy = top_data_df[top_data_df['domain']=='airflow.com']\n",
    "    with open('top_data_airfloy.csv', 'w') as f:\n",
    "        f.write(top_data_airfloy.to_csv(index=False, header=False))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "metadata": {},
   "outputs": [],
   "source": [
    "def print_data():\n",
    "    with open('top_data_zone_10.csv', 'r') as f:\n",
    "        all_data_zone = f.read()\n",
    "    with open('top_data_len_10.csv', 'r') as f:\n",
    "        all_data_len = f.read() \n",
    "    with open('top_data_airfloy.csv', 'r') as f:\n",
    "        all_data_airflow = f.read() \n",
    "    print('Top zone of domains ')\n",
    "    print(all_data_zone)\n",
    "    \n",
    "    print('Top domains lenght')\n",
    "    print(all_data_len)\n",
    "    \n",
    "    print(all_data_airflow)    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "default_args = {\n",
    "    'owner': 'e-jankovenko',\n",
    "    'depends_on_past': False,\n",
    "    'retries': 2,\n",
    "    'retry_delay': timedelta(minutes=5),\n",
    "    'start_date': datetime(2022, 11, 16),\n",
    "}\n",
    "schedule_interval = '30 09 * * *'\n",
    "\n",
    "dag = DAG('e-jankovenko-top-10', default_args=default_args, schedule_interval=schedule_interval)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "t1 = PythonOperator(task_id='get_data',\n",
    "                    python_callable=get_data,\n",
    "                    dag=dag)\n",
    "\n",
    "t2 = PythonOperator(task_id='get_stat_zone',\n",
    "                    python_callable=get_stat,\n",
    "                    dag=dag)\n",
    "\n",
    "t2_len = PythonOperator(task_id='get_stat_len',\n",
    "                        python_callable=get_stat_com,\n",
    "                        dag=dag)\n",
    "\n",
    "\n",
    "t2_air = PythonOperator(task_id='get_stat_airflow',\n",
    "                        python_callable=get_stat_com,\n",
    "                        dag=dag)\n",
    "\n",
    "t3 = PythonOperator(task_id='print_data',\n",
    "                    python_callable=print_data,\n",
    "                    dag=dag)\n",
    "\n",
    "t1 >> [t2, t2_len, t2_air] >> t3\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
