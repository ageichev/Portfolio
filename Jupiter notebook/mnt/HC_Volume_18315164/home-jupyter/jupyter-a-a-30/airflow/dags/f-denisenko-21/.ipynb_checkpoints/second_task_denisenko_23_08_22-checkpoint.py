{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<Task(PythonOperator): print_data>"
      ]
     },
     "execution_count": 1,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "import requests\n",
    "from zipfile import ZipFile\n",
    "from io import BytesIO\n",
    "import pandas as pd\n",
    "from datetime import timedelta\n",
    "from datetime import datetime\n",
    "\n",
    "from airflow import DAG\n",
    "from airflow.operators.python import PythonOperator\n",
    "\n",
    "#Подгружаем данные\n",
    "\n",
    "TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'\n",
    "TOP_1M_DOMAINS_FILE = 'top-1m.csv'\n",
    "\n",
    "#Таски\n",
    "#считываем \n",
    "def get_data():\n",
    "    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)\n",
    "    zipfile = ZipFile(BytesIO(top_doms.content))\n",
    "    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')\n",
    "\n",
    "    with open(TOP_1M_DOMAINS_FILE, 'w') as f:\n",
    "        f.write(top_data)\n",
    "\n",
    "\n",
    "def get_stat():\n",
    "    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    airflow_place = top_data_df.query(\"domain == 'airflow.com'\")\n",
    "    with open('airflow_place.csv', 'w') as f:\n",
    "        f.write(airflow_place.to_csv(index=False, header=False))\n",
    "\n",
    "\n",
    "\n",
    "\n",
    "def print_data(ds): # передаем глобальную переменную airflow\n",
    "    with open('airflow_place.csv', 'r') as f:\n",
    "        all_data = f.read()\n",
    "    date = ds\n",
    "\n",
    "    print(f'Domain airflow.com place for date {date}')\n",
    "    print(all_data)\n",
    "\n",
    "# В Airflow есть свои глобальные переменные, список которых можно посмотреть  в документации\n",
    "\n",
    "#Инициализируем DAG\n",
    "\n",
    "default_args = {\n",
    "    'owner': 'f-denisenko-21',\n",
    "    'depends_on_past': False,\n",
    "    'retries': 2,\n",
    "    'retry_delay': timedelta(minutes=5),\n",
    "    'start_date': datetime(2022, 8, 23),\n",
    "    'schedule_interval': '0 17 * * *'\n",
    "}\n",
    "dag = DAG('denisenko_2_task', default_args=default_args)\n",
    "\n",
    "#Инициализируем таски\n",
    "\n",
    "t1 = PythonOperator(task_id='get_data',\n",
    "                    python_callable=get_data,\n",
    "                    dag=dag)\n",
    "\n",
    "t2 = PythonOperator(task_id='get_stat',\n",
    "                    python_callable=get_stat,\n",
    "                    dag=dag)\n",
    "\n",
    "t3 = PythonOperator(task_id='print_data',\n",
    "                    python_callable=print_data,\n",
    "                    dag=dag)\n",
    "\n",
    "#Задаем порядок выполнения\n",
    "\n",
    "t1 >> t2 >> t3\n"
   ]
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
