import pandas as pd

df = pd.DataFrame({'day': [1, 2, 3], 'users': [10, 20, 30]})

daily_users = df.groupby('day') \
    .agg({'users': 'max'})

print(daily_users)
