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
    "import numpy as np\n",
    "from datetime import timedelta\n",
    "from datetime import datetime\n",
    "from io import StringIO\n",
    "#import telegram\n",
    "\n",
    "from airflow.decorators import dag, task\n",
    "from airflow.operators.python import get_current_context\n",
    "from airflow.models import Variable"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "metadata": {},
   "outputs": [],
   "source": [
    "TOP_GAMES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'\n",
    "TOP_GAMES_FILE = 'vgsales.csv'\n",
    "\n",
    "\n",
    "default_args = {\n",
    "    'owner': 'e-jankovenko',\n",
    "    'depends_on_past': False,\n",
    "    'retries': 2,\n",
    "    'retry_delay': timedelta(minutes=5),\n",
    "    'start_date': datetime(2022, 11, 17),\n",
    "    'schedule_interval': '30 08 * * *'\n",
    "}\n",
    "\n",
    "#CHAT_ID = -620798068\n",
    "#try:\n",
    "#    BOT_TOKEN = Variable.get('telegram_secret')\n",
    "#except:\n",
    "#    BOT_TOKEN = ''\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "@dag(default_args=default_args, catchup=False)\n",
    "def top_games_2009():\n",
    "    @task()\n",
    "    def get_data_2009():\n",
    "        #top_games = requests.get(TOP_GAMES, stream=True)\n",
    "        top_game = pd.read_csv(TOP_GAMES_FILE)\n",
    "        games_data = top_game[top_game['Year']==2009]\n",
    "        #games_data = top_games.read(TOP_GAMES_FILE).decode('utf-8')\n",
    "        return games_data.to_csv(index=False)\n",
    "\n",
    "    @task()\n",
    "    def get_top_game(games_data):\n",
    "        games_data = pd.read_csv(games_data)\n",
    "        top_game = games_data \\\n",
    "            .groupby('Name', as_index=False)\\\n",
    "            .agg({'Global_Sales':'sum'})\\\n",
    "            .sort_values('Global_Sales', ascending=False)\\\n",
    "            .head(1)\n",
    "        return top_game.to_csv(index=False)\n",
    "\n",
    "    @task()\n",
    "    def get_top_genre(games_data):\n",
    "        games_data = pd.read_csv(games_data)\n",
    "        top_genre = games_data \\\n",
    "            .groupby('Genre', as_index=False)\\\n",
    "            .agg({'EU_Sales':'sum'})\\\n",
    "            .sort_values('EU_Sales', ascending=False)\n",
    "        return top_genre.to_csv(index=False)\n",
    "\n",
    "    @task()\n",
    "    def get_top_publisher(games_data):\n",
    "        games_data = pd.read_csv(games_data)\n",
    "        top_publisher = games_data[games_data['JP_Sales']>0] \\\n",
    "            .groupby('Publisher', as_index=False)\\\n",
    "            .agg({'JP_Sales':'mean'})\\\n",
    "            .sort_values('JP_Sales', ascending=False)\n",
    "        return top_publisher.to_csv(index=False)    \n",
    "    \n",
    "    @task()\n",
    "    def get_NA_sales(games_data):\n",
    "        games_data = pd.read_csv(games_data)\n",
    "        NA_sales = games_data \\\n",
    "            .groupby(['Platform','Name'], as_index=False)\\\n",
    "            .agg({'NA_Sales':'sum'}) \\\n",
    "            .sort_values('NA_Sales', ascending=False)\n",
    "        top_platform = NA_sales[NA_sales['NA_Sales']>1] \\\n",
    "            .groupby('Platform')\\\n",
    "            .agg({'Name':'count'})\\\n",
    "            .sort_values('Name', ascending=False)\n",
    "        return top_platform.to_csv(index=False)\n",
    "\n",
    "    @task()\n",
    "    def get_EU_sales(games_data):\n",
    "        games_data = pd.read_csv(games_data)\n",
    "        sales = games_data \\\n",
    "            .groupby('Name')\\\n",
    "            .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})\n",
    "        top_EU_sales = sales[sales['EU_Sales']>sales['JP_Sales']]\n",
    "        return top_EU_sales.to_csv(index=False)\n",
    "    \n",
    "    @task(on_success_callback=send_message)\n",
    "    def print_data(top_game, top_genre, top_publisher, get_NA_sales, get_EU_sales):\n",
    "\n",
    "        context = get_current_context()\n",
    "        date = context['ds']\n",
    "\n",
    "        print(f'Top game in 2009 year:')\n",
    "        print(top_game)\n",
    "\n",
    "        print(f'Top genre in EU in 2009 year:')\n",
    "        print(top_genre)\n",
    "        \n",
    "        print(f'Top platform in NA in 2009 year:')\n",
    "        print(get_NA_sales)\n",
    "        \n",
    "        print(f'Top publisher in JP in 2009 year:')\n",
    "        print(top_publisher)\n",
    "        \n",
    "        print(f'Top games sold in EU than in JP in 2009 year:')\n",
    "        print(get_EU_sales)\n",
    "        "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "games_data = get_data_2009()\n",
    "top_game = get_top_game(games_data)\n",
    "top_genre = get_top_genre(games_data)\n",
    "top_publisher = get_top_publisher(games_data)\n",
    "top_platform = get_NA_sales(games_data)\n",
    "top_EU_sales = get_EU_sales(games_data)\n",
    "print_data(top_game, top_genre, top_publisher, get_NA_sales, get_EU_sales)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "top_games_2009 = top_games_2009()"
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
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "    def get_data_2009():\n",
    "        #top_games = requests.get(TOP_GAMES, stream=True)\n",
    "        top_game = pd.read_csv(TOP_GAMES_FILE)\n",
    "        games_data = top_game[top_game['Year']==2009]\n",
    "        #games_data = top_games.read(TOP_GAMES_FILE).decode('utf-8')\n",
    "        return games_data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [],
   "source": [
    "    def get_top_game(games_data):\n",
    "        top_game = games_data \\\n",
    "            .groupby('Name', as_index=False)\\\n",
    "            .agg({'Global_Sales':'sum'})\\\n",
    "            .sort_values('Global_Sales', ascending=False)\\\n",
    "            .head(1)\n",
    "        return top_game.to_csv(index=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [],
   "source": [
    "    def get_top_genre(games_data):\n",
    "        top_genre = games_data \\\n",
    "            .groupby('Genre', as_index=False)\\\n",
    "            .agg({'EU_Sales':'sum'})\\\n",
    "            .sort_values('EU_Sales', ascending=False)\n",
    "        return top_genre.to_csv(index=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "    def get_top_publisher(games_data):\n",
    "        top_publisher = games_data[games_data['JP_Sales']>0] \\\n",
    "            .groupby('Publisher', as_index=False)\\\n",
    "            .agg({'JP_Sales':'mean'})\\\n",
    "            .sort_values('JP_Sales', ascending=False)\n",
    "        return top_publisher.to_csv(index=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [],
   "source": [
    "    def get_NA_sales(games_data):\n",
    "        NA_sales = games_data \\\n",
    "            .groupby(['Platform','Name'], as_index=False)\\\n",
    "            .agg({'NA_Sales':'sum'}) \\\n",
    "            .sort_values('NA_Sales', ascending=False)\n",
    "        top_platform = NA_sales[NA_sales['NA_Sales']>1] \\\n",
    "            .groupby('Platform')\\\n",
    "            .agg({'Name':'count'})\\\n",
    "            .sort_values('Name', ascending=False)\n",
    "        return top_platform.to_csv(index=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [],
   "source": [
    "    def get_EU_sales(games_data):\n",
    "        sales = games_data \\\n",
    "            .groupby('Name')\\\n",
    "            .agg({'EU_Sales':'sum', 'JP_Sales':'sum'})\n",
    "        top_EU_sales = sales[sales['EU_Sales']>sales['JP_Sales']]\n",
    "        return top_EU_sales.to_csv(index=False)"
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
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "metadata": {},
   "outputs": [],
   "source": [
    "TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'\n",
    "TOP_1M_DOMAINS_FILE = 'top-1m.csv'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_data():\n",
    "        top_doms = requests.get(TOP_1M_DOMAINS, stream=True)\n",
    "        zipfile = ZipFile(BytesIO(top_doms.content))\n",
    "        top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')\n",
    "        return top_data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "\n"
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
   "source": [
    "def send_message(context):\n",
    "    date = context['ds']\n",
    "    dag_id = context['dag'].dag_id\n",
    "    message = f'Huge success! Dag {dag_id} completed on {date}'\n",
    "    if BOT_TOKEN != '':\n",
    "        bot = telegram.Bot(token=BOT_TOKEN)\n",
    "        bot.send_message(chat_id=CHAT_ID, text=message)\n",
    "    else:\n",
    "        pass\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "@dag(default_args=default_args, catchup=False)\n",
    "def top_10_airflow_2():\n",
    "    @task(retries=3)\n",
    "    def get_data():\n",
    "        top_doms = requests.get(TOP_1M_DOMAINS, stream=True)\n",
    "        zipfile = ZipFile(BytesIO(top_doms.content))\n",
    "        top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')\n",
    "        return top_data\n",
    "\n",
    "    @task(retries=4, retry_delay=timedelta(10))\n",
    "    def get_table_ru(top_data):\n",
    "        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])\n",
    "        top_data_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]\n",
    "        return top_data_ru.to_csv(index=False)\n",
    "\n",
    "    @task()\n",
    "    def get_stat_ru(top_data_ru):\n",
    "        ru_df = pd.read_csv(StringIO(top_data_ru))\n",
    "        ru_avg = int(ru_df['rank'].aggregate(np.mean))\n",
    "        ru_median = int(ru_df['rank'].aggregate(np.median))\n",
    "        return {'ru_avg': ru_avg, 'ru_median': ru_median}\n",
    "\n",
    "    @task()\n",
    "    def get_table_com(top_data):\n",
    "        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])\n",
    "        top_data_com = top_data_df[top_data_df['domain'].str.endswith('.com')]\n",
    "        return top_data_com.to_csv(index=False)\n",
    "\n",
    "    @task()\n",
    "    def get_stat_com(top_data_com):\n",
    "        com_df = pd.read_csv(StringIO(top_data_com))\n",
    "        com_avg = int(com_df['rank'].aggregate(np.mean))\n",
    "        com_median = int(com_df['rank'].aggregate(np.median))\n",
    "        return {'com_avg': com_avg, 'com_median': com_median}\n",
    "\n",
    "    @task(on_success_callback=send_message)\n",
    "    def print_data(ru_stat, com_stat):\n",
    "\n",
    "        context = get_current_context()\n",
    "        date = context['ds']\n",
    "\n",
    "        ru_avg, ru_median = ru_stat['ru_avg'], ru_stat['ru_median']\n",
    "        com_avg, com_median = com_stat['com_avg'], com_stat['com_median']\n",
    "\n",
    "        print(f'''Data from .RU for {date}\n",
    "                  Avg rank: {ru_avg}\n",
    "                  Median rank: {ru_median}''')\n",
    "\n",
    "        print(f'''Data from .COM for {date}\n",
    "                          Avg rank: {com_avg}\n",
    "                          Median rank: {com_median}''')\n",
    "\n",
    "    top_data = get_data()\n",
    "    top_data_ru = get_table_ru(top_data)\n",
    "    ru_data = get_stat_ru(top_data_ru)\n",
    "\n",
    "    top_data_com = get_table_com(top_data)\n",
    "    com_data = get_stat_com(top_data_com)\n",
    "\n",
    "    print_data(ru_data, com_data)\n",
    "\n",
    "top_10_airflow_2 = top_10_airflow_2()"
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
