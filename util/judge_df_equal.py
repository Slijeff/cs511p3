# judge if two pandas table are equal, when encountering float, if their difference is within a threshold,
# then regard them as equal
import numpy as np
import pandas


def judge_df_equal(df1: pandas.DataFrame, df2: pandas.DataFrame):
    if df1.shape != df2.shape:
        return False

    # Check if the column names are the same
    if list(df1.columns) != list(df2.columns):
        return False

    # Compare each element of the DataFrames, considering float tolerance
    for index, row in df1.iterrows():
        row2 = df2.loc[index]
        for index1 in range(len(row)):
            if not (row.iloc[index1] == row2.iloc[index1] or abs(row.iloc[index1] - row2.iloc[index1] < 0.001)):
                return False

    return True
