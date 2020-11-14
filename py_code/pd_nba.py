import pandas as pd
with pd.ExcelFile('C:\\codebase\\yanzi2020\\py_code\\TEAM_NAME.xlsx') as xls:
    df1 = pd.read_excel(xls, 'sheet1')
data = df1['TeamAB']
print(data)
data = df1.loc[0]
print(data)
data = df1.loc['0':'1', ['TeamAB', 'TeamAB1']]
print(data)
data = df1.iloc[3]
print(data)
data = df1.iloc[3]
print(data)
data = df1.iloc[3:5, 0:2]
print(data)
data = df1[df1.TeamAB > 'H']
print(data)