{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import requests\n",
    "from zipfile import ZipFile\n",
    "from io import BytesIO\n",
    "import pandas as pd\n",
    "from datetime import timedelta\n",
    "from datetime import datetime\n",
    "\n",
    "from airflow import DAG\n",
    "from airflow.operators.python import PythonOperator"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'\n",
    "TOP_1M_DOMAINS_FILE = 'top-1m.csv'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 91,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<Task(PythonOperator): print_data>"
      ]
     },
     "execution_count": 91,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "def get_data():\n",
    "    top_doms = pd.read_csv(TOP_1M_DOMAINS)\n",
    "    top_data = top_doms.to_csv(index=False)\n",
    "    with open(TOP_1M_DOMAINS_FILE, 'w') as f:\n",
    "        f.write(top_data)\n",
    "        \n",
    "def get_top_10lvl_domain():\n",
    "    top_10lvl_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    top_10lvl_domain['top_lvl_domain'] = top_10lvl_domain.domain.str.split('.').str[1]   \n",
    "    top_10lvl_domain = top_10lvl_domain.groupby(\"top_lvl_domain\", as_index=False) \\\n",
    "                       .agg({\"rank\": \"count\"}) \\\n",
    "                       .sort_values(\"rank\", ascending=False) \\\n",
    "                       .reset_index()[['top_lvl_domain']].head(10)       \n",
    "    with open('top_10lvl_domain.csv', 'w') as f:\n",
    "        f.write(top_10lvl_domain.to_csv(index=False, header=False))\n",
    "\n",
    "def get_longest_domain():\n",
    "    longest_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    longest_domain['name'] = longest_domain.domain.str.split('.').str[0]\n",
    "    longest_domain['length_name'] = longest_domain.name.apply(lambda x: len(x))\n",
    "    longest_domain = longest_domain.sort_values('length_name', ascending=False).head(1).name\n",
    "    with open('longest_domain.csv', 'w') as f:\n",
    "        f.write(longest_domain.to_csv(index=False, header=False))\n",
    "        \n",
    "def get_airflow_position():\n",
    "    airflow_position = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])\n",
    "    airflow_position = airflow_position.query('domain == \"airflow.com\"')\n",
    "    if airflow_position.empty == False:\n",
    "        airflow_position = airflow_position['rank']\n",
    "    else:\n",
    "        airflow_position = 'No domain.'    \n",
    "        with open('airflow_position.csv', 'w') as f:\n",
    "            f.write(airflow_position)\n",
    "    \n",
    "def print_data(ds):\n",
    "    with open('top_10lvl_domain.csv', 'r') as f:\n",
    "        top_10lvl_domain = f.read()\n",
    "    with open('longest_domain.csv', 'r') as f:\n",
    "        longest_domain = f.read()\n",
    "    with open('airflow_position.csv', 'r') as f:\n",
    "        airflow_position = f.read()    \n",
    "    date = ds\n",
    "\n",
    "    print(f'Top 10 domain zones {date}')\n",
    "    print(top_10lvl_domain)\n",
    "\n",
    "    print(f'Longest domain name {date}')\n",
    "    print(longest_domain)\n",
    "   \n",
    "    print(f'Rank airflow.com {date}')\n",
    "    print(airflow_position)\n",
    "\n",
    "default_args = {\n",
    "    'owner': 'i-ishakov-22',\n",
    "    'depends_on_past': False,\n",
    "    'retries': 2,\n",
    "    'retry_delay': timedelta(minutes=5),\n",
    "    'start_date': datetime(2022, 7, 23),\n",
    "}\n",
    "schedule_interval = '* 12 * * *'\n",
    "\n",
    "dag = DAG('iskhakov_lesson_2', default_args=default_args, schedule_interval=schedule_interval)\n",
    "\n",
    "t1 = PythonOperator(task_id='get_data',\n",
    "                    python_callable=get_data,\n",
    "                    dag=dag)\n",
    "\n",
    "t2 = PythonOperator(task_id='get_top_10lvl_domain',\n",
    "                    python_callable=get_stat,\n",
    "                    dag=dag)    \n",
    "\n",
    "t3 = PythonOperator(task_id='get_longest_domain',\n",
    "                        python_callable=get_stat_com,\n",
    "                        dag=dag)\n",
    "\n",
    "t4 = PythonOperator(task_id='get_airflow_position',\n",
    "                    python_callable=print_data,\n",
    "                    dag=dag)\n",
    "\n",
    "t5 = PythonOperator(task_id='print_data',\n",
    "                    python_callable=print_data,\n",
    "                    dag=dag)\n",
    "\n",
    "t1 >> [t2, t3, t4] >> t5"
   ]
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
