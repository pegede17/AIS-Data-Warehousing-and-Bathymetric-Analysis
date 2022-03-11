import pandas as pd

df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
                              'Parrot', 'Parrot'],
                   'Max Speed': [380., 370., 24., 26.]})

dfGroup = df.groupby(by = ['Animal'])
dflist = list(dfGroup)[1]

print(dflist)

# for animal in dfGroup:
#     for a, element in animal:
#         print(element)
    
