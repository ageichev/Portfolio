import sys
import os
repo_dir = os.path.expanduser('~/analytics')

sys.path.append(repo_dir)

from data_mining import get_dataframe_shape

file_path = '~/shared/order_leads.csv'
data_shape = get_dataframe_shape(file_path)

print(data_shape)
