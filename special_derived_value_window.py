import pandas as pd

def size(dfm):
    """
    Returns the number of rows in the dataframe of the DataframeManager dfm
    :param dfm: the DataframeManager we want to know the numbers of rows of its dataframe
    :return: the number of rows
    """
    return dfm.dataframe.shape[0]


def weighted_avg(df, field, weight):
    sum = 0
    sum_weight = 0
    for index, field, weight in df[[field, weight]].itertuples():
        sum = sum + field*weight
        sum_weight = sum_weight + weight
    return sum / sum_weight