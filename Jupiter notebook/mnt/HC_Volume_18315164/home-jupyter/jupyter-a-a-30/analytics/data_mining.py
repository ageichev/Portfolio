import pandas as pd

def get_dataframe_shape(file_path):
    """
    Эта функция считывает файл и выводит размеры датафрейма
    """
    df = pd.read_csv(file_path, encoding='utf-8')
    return df.shape

def get_dataframe_dtypes(file_path):
    """
    Our function to get dataframe data types  
    """
    df = pd.read_csv(file_path, encoding='windows-1251')
    return df.dtypes


def get_dataframe_columns(file_path):
    """
    Our function to get dataframe data types  
    """
    df = pd.read_csv(file_path)
    return df.columns
