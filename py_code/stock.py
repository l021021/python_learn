import numpy as np
from datetime import datetime


# 星期一 0
# 星期二 1
# 星期三 2
# 星期四 3
# 星期五 4
# 星期六 5
# 星期日 6
def datestr2num(s):
    # print(str(s, encoding="utf-8"))
    return datetime.strptime(str(s, encoding="utf-8"), "%Y/%m/%d").date()


# dates = np.loadtxt('C:\\codebase\\yanzi2020\\py_code\\stock.csv',
#                    delimiter=',',
#                    skiprows=1,
#                    usecols=(0),
#                    dtype='datetime64',
#                    unpack=True)

dates, pre, open, high, low, close, volume = np.loadtxt(
    'C:\\codebase\\yanzi2020\\py_code\\stock.csv',
    delimiter=',',
    skiprows=1,
    usecols=(0, 1, 3, 5, 7, 9, 15),
    converters={0: datestr2num},
    unpack=True)

print("Dates =", dates)
averages = np.zeros(5)
averagesv = np.zeros(5)
for i in range(5):
    indices = np.where(dates == i)
    prices = np.take(close, indices)
    volumes = np.take(volume, indices)
    avg = np.mean(prices)
    avgv = np.mean(volume)
    # print("Day", i, "prices", prices, "Average", avg)
    averages[i] = avg
    averagesv[i] = avgv
top = np.max(averages)
bottom = np.min(averages)
bottomv = np.min(averagesv)
topv = np.max(averagesv)
print("Highest average", top)
print("Top day of the week", np.argmax(averages))
# bottom = np.min(averages)
print("Lowest average", bottom)
print("Bottom day of the week", np.argmin(averages))
print("Highest average vol", topv)
print("Top day of the week", np.argmax(averagesv))
# bottom = np.min(averagesv)
print("Lowest average", bottomv)
print("Bottom day of the week", np.argmin(averagesv))