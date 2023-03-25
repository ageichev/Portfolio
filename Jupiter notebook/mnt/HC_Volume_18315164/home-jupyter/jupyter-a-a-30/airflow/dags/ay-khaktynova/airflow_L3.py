import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task


# data_source = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
data_source = 'https://git.lab.karpov.courses/lab/airflow/-/blob/master/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'ay-khaktynova') % 23
data_subset_filename = "vgsales_subset_{}.csv".format(year)


default_args = {
    'owner': 'ay-khaktynova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 10),
    'start_date': datetime(2022, 10, 25),
    'schedule_interval': '30 9 * * *'
}

@dag(default_args = default_args, catchup = False)
def ay_khaktynova_L3():
    @task(retries = 3)
    def get_data():
        df = pd.read_csv(DATA_SOURCE)
        df_subset = df[df['Year'] == year]
        return df_subset
    
    @task()
    def get_table(df, data_file):
        return df.to_csv(data_file, index = False)
    
    @task()
    def get_game_with_max_sales(data_file):
        df_subset = pd.read_csv(data_file)
        game_with_max_sales = df_subset.loc[df_subset['Global_Sales'].idxmax()]['Name']
        return game_with_max_sales

    @task()
    def get_sales_by_genres_EU(data_file):
        df_subset = pd.read_csv(data_file)
        sales_by_genres_EU = df_subset.groupby('Genre') \
                                      .agg({'EU_Sales': 'sum'}) \
                                      .sort_values(by = 'EU_Sales', ascending = False)
        max_value = sales_by_genres_EU['EU_Sales'].max()
        max_sales_by_genres_EU = sales_by_genres_EU.query("EU_Sales == @max_value")
        return sales_by_genres_EU.to_csv()

    @task()
    def get_titles_by_platform_NA(data_file):
        df_subset = pd.read_csv(data_file)
        titles_by_platform_NA = df_subset[df_subset['NA_Sales'] > 1] \
                                .groupby('Platform') \
                                .agg({'NA_Sales': 'count'}) \
                                .rename(columns = {'NA_Sales' : 'titles'}) \
                                .sort_values(by = 'titles', ascending = False)
        max_value = titles_by_platform_NA['titles'].max()
        top_titles_NA = titles_by_platform_NA[titles_by_platform_NA['titles'] == max_value]       
        return top_titles_NA.to_csv()
   
    @task()
    def get_mean_sales_by_publisher_JP(data_file):
        df_subset = pd.read_csv(data_file)
        mean_sales_by_publisher_JP = df_subset.groupby('Publisher') \
                                              .agg({'JP_Sales': 'mean'}) \
                                              .rename(columns = {'JP_Sales' : 'JP_Sales_mean'}) \
                                              .sort_values(by = 'JP_Sales_mean', ascending = False)
        max_value = mean_sales_by_publisher_JP['JP_Sales_mean'].max()
        top_mean_sales_by_publisher_JP = mean_sales_by_publisher_JP.query("JP_Sales_mean == @max_value")
        return top_mean_sales_by_publisher_JP.to_csv()
    
    @task()
    def get_titles_with_sales_EU_more_JP(data_file):
        df_subset = pd.read_csv(data_file)
        sales_EU_more_JP = df_subset.query("EU_Sales > JP_Sales")
        titles_with_sales_EU_more_JP = sales_EU_more_JP.shape[0]
        return titles_with_sales_EU_more_JP
        
    @task()
    def print_data(game_with_max_sales,
                   sales_by_genres_EU,
                   titles_by_platform_NA,
                   mean_sales_by_publisher_JP,
                   titles_with_sales_EU_more_JP) :

        print(f'Game with max sales in {year} is \'{game_with_max_sales}\'.\n')
        
        print(f'Sales by genres in the EU in {year}:')
        print(sales_by_genres_EU)
        
        print(f'Platform(s) with highest sales in {year} in NA:')
        print(titles_by_platform_NA)
        
        print(f'Mean sales value(s) for publishers in {year} in JP:')
        print(mean_sales_by_publisher_JP)
        
        print(f'{titles_with_sales_EU_more_JP} games in {year} have higher sales in EU, than in JP.')


    data_subset = get_data()
    data_file = get_table(data_subset, data_subset_filename)
    
    game_with_max_sales = get_game_with_max_sales(data_file)
    sales_by_genres_EU = get_sales_by_genres_EU(data_file)
    titles_by_platform_NA = get_titles_by_platform_NA(data_file)
    mean_sales_by_publisher_JP = get_mean_sales_by_publisher_JP(data_file)
    titles_with_sales_EU_more_JP = get_titles_with_sales_EU_more_JP(data_file)

    print_data(game_with_max_sales,
               sales_by_genres_EU,
               titles_by_platform_NA,
               mean_sales_by_publisher_JP,
               titles_with_sales_EU_more_JP)

ay_khaktynova_L3 = ay_khaktynova_L3()
