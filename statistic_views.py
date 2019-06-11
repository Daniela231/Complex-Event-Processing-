from DataframeManager import *
import pandas as pd

def linest():


def correl(method,axis, *columns):
    """
    Calculates the pairwise correlation of columns or rows with the given correlation method
    :param method: correlation method (pearson, kendall or spearman)
    :param axis: define if use columns(0) or rows(1)
    :param columns: all columns or rows we want the correlation of
    :return: correlation values
    """
    return all_dfs["StockTick"].dataframe.corrwith(columns,axis=axis, method=method)