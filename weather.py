import pandas as pd
bbox = pd.read_excel('bbox_list.xlsx', index_col=None,  sheet_name='bbox_list')
print(bbox)