import pandas as pd
import matplotlib.pyplot as plt
# pd.set_option('display.mpl_style', 'default')  # 使图表漂亮一些
# figsize(15, 5)
# pd.set_option('display.mpl_style', 'default')
# pd.set_option('display.line_width', 5000)
pd.set_option('display.max_columns', 60)
# broken_df = pd.read_csv('C:\\codebase\\yanzi2020\\py_code\\data1.csv')
fixed_df = pd.read_csv('C:\\codebase\\yanzi2020\\py_code\\data1.csv',
                       encoding='latin1',
                       parse_dates=['Time'],
                       dayfirst=True,
                       index_col='Time')
# fixed_df['weekday'] = fixed_df.index.weekday
fixed_df['Time'] = fixed_df['Time'].astype(datetimes)
weekday_sum = fixed_df.groupby('weekday').aggregate(sum)
weekday_sum.index = [
    'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday',
    'Sunday'
]
# fixed_df[:3]
print(weekday_sum[:])
# fixed_df['Pct'].plot()
# print(fixed_df.index.weekday[:])
# fixed_df.plot(figsize=(15, 10))
weekday_sum.plot(kind='bar')
plt.show()
