import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta

data_path =  '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
user_login = 'p-gordejchuk'

default_args = {'owner': user_login,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 2, 16)}


@dag(default_args=default_args, catchup=False, schedule_interval='* * */1 * *')
def games_analytics_pg():
    @task  # Task  1 - Retrieve data with a path and filter it by a user login
    def get_data(data_path: str, user_login: str) -> pd.DataFrame:
        """Returns a pandas data frame filtered with user's login name."""

        current_year = int(1994 + hash(f'{user_login}') % 23)
        df = (pd.read_csv(data_path)
              .query('Year == @current_year')
              .reset_index(drop=True))
        return df

    @task  # Task 2.1 - Get best-selling titles in all the world
    def get_bestseller_name(df: pd.DataFrame, region: str = 'Global_Sales') -> dict:
        """Returns a dictionary with a name of the best-selling game and it's sales in the selected region."""

        bestseller_name: str = df.set_index('Name')[region].idxmax()
        bestseller_sales: float = df[region].max()

        return {'name': bestseller_name,
                'sales': bestseller_sales,
                'region': region}

    @task  # Task 2.2 - Get best-selling genres in EU
    def get_bestseller_genres(df: pd.DataFrame, region: str = 'EU_Sales') -> dict:
        """Returns a dictionary with a list of the best-selling genres and theirs sales in the selected region."""

        sales_by_genre: pd.DaraFrame = (df.groupby('Genre', as_index=False)
                                        .agg({region: 'sum'})
                                        .sort_values(by=region, ascending=False))
        bestseller_sum: float = sales_by_genre[region].max()
        bestseller_genres: list = (sales_by_genre
                                   [sales_by_genre[region] == bestseller_sum]
                                   .Genre.to_list())

        return {'genres': bestseller_genres,
                'sales': round(bestseller_sum, 2),
                'region': region}

    @task  # Task 2.3 - Get platforms names with the most game titles sold more than $1 mln in NA
    def get_top_platforms(df: pd.DataFrame, region: str = 'NA_Sales', sales_limit: float = 1.00) -> dict:
        """
        Returns a dictionary with a list of platform names with the most game titles which sales are more
        than a certain monetary limit and a number of these titles.
        """
        # Find a platform with the most game titles
        games_by_platform: pd.DaraFrame = (df[df[region] >= sales_limit]
                                           .groupby('Platform', as_index=False)
                                           .agg({'Name': 'count'}).rename(columns={'Name': 'Number'})
                                           .sort_values(by='Number', ascending=False))

        # Prepare values for the dict
        top_games_number: int = (games_by_platform
                                 .head(1)
                                 .Number.values[0])
        platform_names: list = (games_by_platform
                                [games_by_platform.Number == top_games_number]
                                .Platform
                                .to_list())

        return {'platforms': platform_names,
                'games_number': top_games_number,
                'region': region,
                'sales_limit': sales_limit}

    @task  # Task 2.4 - Get publisher names with the most mean sales in Japan
    def get_top_publishers(df: pd.DataFrame, region: str = 'JP_Sales') -> dict:
        """Returns a dictionary with a list of publishers with the most mean sales."""

        # Calculate mean sales by publisher
        mean_sales_by_publisher: pd.DaraFrame = (df.groupby('Publisher', as_index=False)
                                                 .agg({region: 'mean'})
                                                 .sort_values(by=region, ascending=False))

        # Prepare values for the dict
        top_mean_sales: float = (mean_sales_by_publisher.head(1)
                                 [region].values[0])
        top_publishers: list = (mean_sales_by_publisher
                                [mean_sales_by_publisher[region] == top_mean_sales]
                                .Publisher.to_list())

        return {'publishers': top_publishers,
                'mean_sales': round(top_mean_sales, 2),
                'region': region}

    @task  # Task 2.5 - Calculate number of games better sold in EU than in Japan
    def compare_regions(df: pd.DataFrame, main_region: str = 'EU_Sales', sub_region='JP_Sales') -> dict:
        """Returns a dictionary of games which were sold better in the main region than in subregion."""

        # Create an additional column with True/False if sales in the main region better than in the subregion
        comps_df = df.assign(Comps=(df[main_region] > df[sub_region]))

        # Filter out False values and return a list
        better_sales: list = (comps_df.query('Comps == True')
                              .Name.to_list())

        return {'titles': better_sales,
                'main_region': main_region,
                'sub_region': sub_region}

    @task  # Task 3 - Print all results from all "2" tasks
    def print_results(df: pd.DataFrame,
                      best_title: dict,
                      best_genres: dict,
                      best_platforms: dict,
                      best_publishers: dict,
                      sales_comparison: dict) -> print:
        """Prints the results of games analysis."""
        current_date = get_current_context()['ds']  # get the current date from DAG's context
        selected_year = int(df.Year[0])

        # Function that turns any list to a string
        def turn_list_to_string(some_list):
            string = "".join('"' + str(value) + '", ' for value in some_list)
            string = string[:-2]  # exlude the last comma and space
            return string

        # Preparing results by task
        # 1 - Titles
        task_1_result = ("1. The best-selling game in " + best_title['region'].split('_')[0] +
                         " is \"" + best_title['name'] + "\" with $"
                         + str(best_title['sales']) + "M of sales.")

        # 2 - Genres
        if len(best_genres['genres']) == 1:
            task_2_result = ("2. The best-selling genre in " + best_genres['region'].split('_')[0] +
                             " is \"" + best_genres['genres'][0] + "\" with $"
                             + str(best_genres['sales']) + "M of sales.")
        else:
            task_2_result = ("2. The best-selling genres in " + best_genres['region'].split('_')[0] +
                             " are " + turn_list_to_string(best_genres['genres']) + " with $"
                             + str(best_genres['sales']) + "M of sales.")

        # 3 - Platforms
        if len(best_platforms['platforms']) == 1:
            task_3_result = ("3. Platform with the most game titles which revenue is more than $" +
                             str(best_platforms['sales_limit']) + "M in " + best_platforms['region'].split('_')[0] +
                             " is \"" + best_platforms['platforms'][0] + "\" with " +
                             str(best_platforms['games_number']) + " titles.")
        else:
            task_3_result = ("3. Platforms with the most game titles which revenue is more than $" +
                             str(best_platforms['sales_limit']) + "M in " + best_platforms['region'].split('_')[0] +
                             " are " + turn_list_to_string(best_platforms['platforms']) + " with " +
                             str(best_platforms['games_number']) + " titles.")

        # 4 - Publishers
        if len(best_publishers['publishers']) == 1:
            task_4_result = ("4. Publisher with the highest mean sales in " + best_publishers['region'].split('_')[0] +
                             " is \"" + best_publishers['publishers'][0] + "\" with $"
                             + str(best_publishers['mean_sales']) + "M of mean sales per game.")
        else:
            task_4_result = ("4. Publisher with the highest mean sales in " + best_publishers['region'].split('_')[0] +
                             " are " + turn_list_to_string(best_publishers['genres']) + " with $"
                             + str(best_publishers['mean_sales']) + "M of mean sales per game.")

        # 5 - Sales comparison
        task_5_result = ("5. Number of games which revenue is better in " +
                         sales_comparison['main_region'].split('_')[0] +
                         " than in " + sales_comparison['sub_region'].split('_')[0] + " is " +
                         str(len(sales_comparison['titles'])) + ".")

        message = f"""
        RESULTS OF CALCULATIONS---------------------------------------------------------------------------------------------
        Selected year - {selected_year}, current date - {current_date}
        {task_1_result}
        {task_2_result}
        {task_3_result}
        {task_4_result}
        {task_5_result}
        --------------------------------------------------------------------------------------------------------------------
        """
        print(message)

    # DAG ALGORITHM-----------------------------------------------------------------------------------------------------
    # INPUT
    df = get_data(data_path, user_login)

    # PROCESSING
    best_title = get_bestseller_name(df)
    best_genres = get_bestseller_genres(df)
    best_platforms = get_top_platforms(df)
    best_publishers = get_top_publishers(df)
    sales_comparison = compare_regions(df)

    # OUTPUT
    print_results(df, best_title, best_genres, best_platforms, best_publishers, sales_comparison)

# Start the DAG
games_analytics_pg = games_analytics_pg()
