import pandas as pd
from length_window import *
from special_derived_value_window import *


def linest(dataframe, function, value_expr1, value_expr2):
    """
    calculation of regression and intermediate results for 2 columns
    :param dataframe: dataframe we want the calculation of
    :param function: what we want to calculate
    :param value_expr1: first set of data
    :param value_expr2: second set of data
    :return: returns the result of the chosen function
    """
    new_dataframe= pd.DataFrame()
    if function == 'slope':
        return dataframe[value_expr1].diff()/dataframe[value_expr2].diff()
    elif function == 'YIntercept':
        variable =dataframe[value_expr2.tail(1)]-((dataframe[value_expr1].diff()/dataframe[value_expr2].diff())
                                                  * dataframe[value_expr1.tail(1)])
        return variable
    elif function == 'XAverage':
        return dataframe.sum(value_expr1)/dataframe.shape[0]
    elif function == 'XStandardDeviationPop':
        return dataframe.std(value_expr1)
    elif function == 'XSum':
        return dataframe.sum(value_expr1)
    elif function == 'XVariance':
        new_dataframe['X'] = dataframe(value_expr1)
        return new_dataframe.var()
    elif function == 'YAverage':
        return dataframe.sum(value_expr2)/dataframe.shape[0]
    elif function == 'YSum':
        return dataframe.sum(value_expr2)
    elif function == 'YVariance':
        new_dataframe['Y'] = dataframe(value_expr2)
        return new_dataframe.var()
    elif function == 'datapoints':
        return size(dataframe)
    elif function == 'n':
        return size(dataframe)
    elif function == 'SumX':
        return dataframe.sum(value_expr1)
    elif function == 'SumXsq':
        new_dataframe['X'] = dataframe[value_expr1]**2
        return new_dataframe.sum('X')
    elif function == 'SumXY':
        new_dataframe['XY'] = dataframe[value_expr1]+dataframe[value_expr2]
        return new_dataframe.sum('XY')
    elif function == 'SumY':
        return dataframe.sum(value_expr1)
    elif function == 'SumYsq':
        new_dataframe['Y'] = dataframe[value_expr2] ** 2
        return new_dataframe.sum('Y')

def univariate(dataframe, property, value_expr):
    """
    This function calculates univariate statistics on a numeric expression
    :param dataframe: dataframe of which we want the statistics
    :param property: univariate statistics derived properties
    :param value_expr: set of data
    :return: result of the chosen property
    """
    dataframe = pd.DataFrame()
    if property == 'datapoints':
        return dataframe.shape[0]
    elif property == 'total':
        return dataframe[value_expr].sum(axis=0)
    elif property == 'average':
        return dataframe[value_expr].mean(axis=0)
    elif property == 'variance':
        return dataframe[value_expr].var(axis=0)
    elif property == 'stddev':
        return dataframe.std(value_expr)


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
