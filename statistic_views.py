from DataframeManager import *
import pandas as pd


#def linest():


def prepare_for_correl(dataframe, *params):
    """
    Prepares dataframes for correlation function by enabling us to filter a given dataframe by the params
    :param dataframe: dataframe we want to prepare
    :param params: List of columns we want to keep
    :return: returns the filtered dataframe
    """
    ReturnDataframe=pd.DataFrame
    for param in params:
        ReturnDataframe = dataframe.loc[:, param]

    return ReturnDataframe



def simple_correl(method, dataframe, decimals):
    """
    calculates the correlation inside of a dataframe by the given method for the cólumns
    :param method: correlation method (pearson, kendall or spearman)
    :param dataframe: dataframe for the correlation
    :param decimals: number of decimals
    :return: correlation values
    """
    return dataframe.corr(method=method, min_periods=decimals)


def correlwith(df1, df2, method, axis):
    """
    Calculates the correlation of 2 dataframes by a given method and axis
    :param df1: fist dataframe
    :param df2: second dataframe
    :param method: correlation method ('pearson', 'kendall', 'spearman', 'callable')
    :param axis: correlation by columns(0) or rows(1)
    :return: correlation of the 2 dataframes
    """
    return df1.corrwith(df2, method=method, axis=axis)
