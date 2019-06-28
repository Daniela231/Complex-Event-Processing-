import pandas as pd

def size(dfm):
    """
    Returns the number of rows in the dataframe of the DataframeManager dfm
    :param dfm: the DataframeManager we want to know the numbers of rows of its dataframe
    :return: the number of rows
    """
    return dfm.dataframe.shape[0]


def weighted_avg(df, field, weight):
    """
    This function returns the weighted average for the values from the column 'field' with weights from the 'weight'
    column.
    :param df: dataframe to be considered.
    :param field: name of the values column
    :param weight: name of the weights column
    :return: the weighted average
    """
    sum = 0
    sum_weight = 0
    for index, field, weight in df[[field, weight]].itertuples():
        sum = sum + field*weight
        sum_weight = sum_weight + weight
    return sum / sum_weight if sum_weight != 0 else 0
