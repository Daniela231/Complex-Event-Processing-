"""
This file provides some functions that are generalizations of some functions from the other files. Functions from this
file can also be applied to dataframes where it is possible to delete rows (not only to 'StockTick'). Moreover, if time
values should be compared in any of the functions in this file, the parameter 'col' defines which time column should be
considered (it does not have to be the 'INSERTION_TIMESTAMP' column). Therefore, these functions are less efficient but
more general. All functions in this file need a pandas dataframe as a parameter and their names begin with 'df_'.
"""

from datetime import datetime, timedelta
import pandas as pd


def df_last_length(df, col, len):
    """
    Returns a pandas dataframe that contains the last 'len' rows from the pandas dataframe 'df' after sorting it by the
    values of the column 'col'.
    :param df: pandas dataframe
    :param col: name of the column that should be considered
    :param len: length of dataframe that should be returned
    :return: pandas dataframe
    """
    return df.sort_values(by=col).tail(len)


def df_first_length(df, col, len):
    """
    Returns a pandas dataframe that contains the first 'len' rows from the pandas dataframe 'df' after sorting it by the
    values of the column 'col'.
    :param df: pandas dataframe
    :param col: name of the column that should be considered
    :param len: length of dataframe that should be returned
    :return: pandas dataframe
    """
    return df.sort_values(by=col).head(len)


def df_first_time(df, col, start_point=datetime.now(), seconds=0, milliseconds=0, microseconds=0, minutes=0, hours=0,
                  days=0, weeks=0):
    """
    Returns a pandas dataframe that contains all rows from given dataframe 'df' where the value in the time column 'col'
    is within the given time span after start_point.
    :param df: pandas dataframe
    :param col: name of the time column
    :param start_point: start point of time (it is set by default to the current system time)
    :param seconds: number of seconds
    :param milliseconds: number of milliseconds
    :param microseconds: number of microseconds
    :param minutes: number of minutes
    :param hours: number of hours
    :param days: number of days
    :param weeks: number of weeks
    :return: pandas dataframe
    """
    time = start_point + timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                             milliseconds=milliseconds, microseconds=microseconds)
    res = pd.DataFrame()

    for index, row in df.iterrows():
        val = row[col]
        if val < time and val >= start_point:
            res = res.append(row)

    return res


def df_last_time(df, col, start_point=datetime.now(), seconds=0, milliseconds=0, microseconds=0, minutes=0, hours=0,
                 days=0, weeks=0):
    """
    Returns a pandas dataframe that contains all rows from given dataframe 'df' where the value in the time column 'col'
    is within the given time span looking back into the past from the start_point.
    :param df: pandas dataframe
    :param col: name of the time column
    :param start_point: start point of time (it is set by default to the current system time)
    :param seconds: number of seconds
    :param milliseconds: number of milliseconds
    :param microseconds: number of microseconds
    :param minutes: number of minutes
    :param hours: number of hours
    :param days: number of days
    :param weeks: number of weeks
    :return: pandas dataframe
    """
    time = start_point - timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                                       milliseconds=milliseconds, microseconds=microseconds)
    res = pd.DataFrame()

    for index, row in df.iterrows():
        val = row[col]
        if val <= start_point and val > time:
            res = res.append(row)

    return res


def df_last_unique(df, *columns):
    """
    Returns a pandas dataframe that includes only the most recent among events having the same values for the columns
    given as parameters (considering the events from the given pandas dataframe 'df').
    :param df: pandas dataframe
    :param columns: names of the columns to be considered
    :return: pandas dataframe
    """
    return df.drop_duplicates(subset=set(columns), keep='last')


def df_first_unique(df, *columns):
    """
    Returns a pandas dataframe that includes only the first among events having the same values for the columns given as
    parameters (considering the events from the given pandas dataframe 'df').
    :param df: pandas dataframe
    :param columns: names of the columns to be considered
    :return: pandas dataframe
    """
    return df.drop_duplicates(subset=set(columns), keep='first')


def df_order_first_time(df, col, start_point=datetime.now(), seconds=0, milliseconds=0, microseconds=0, minutes=0,
                        hours=0, days=0, weeks=0):
    """
    Returns a pandas dataframe that contains all rows from given dataframe 'df' where the value in the time column 'col'
    is within the given time span after start_point. The rows of the returned dataframe are ordered by the values of the
    time column 'col'.
    :param df: pandas dataframe
    :param col: name of the time column
    :param start_point: start point of time (it is set by default to the current system time)
    :param seconds: number of seconds
    :param milliseconds: number of milliseconds
    :param microseconds: number of microseconds
    :param minutes: number of minutes
    :param hours: number of hours
    :param days: number of days
    :param weeks: number of weeks
    :return: pandas dataframe
    """
    time = start_point + timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                             milliseconds=milliseconds, microseconds=microseconds)
    res = pd.DataFrame()
    df = df.sort_values(by=col, ascending=False)

    for index, row in df.iterrows():
        val = row[col]
        if val >= time:
            continue
        elif val >= start_point:
            res = res.append(row)
        else:
            break

    return res.iloc[::-1]


def df_order_last_time(df, col, start_point=datetime.now(), seconds=0, milliseconds=0, microseconds=0, minutes=0,
                 hours=0, days=0, weeks=0):
    """
    Returns a pandas dataframe that contains all rows from given dataframe 'df' where the value in the time column 'col'
    is within the given time span looking back into the past from the start_point. The rows of the returned dataframe
    are ordered by the values of the time column 'col'.
    :param df: pandas dataframe
    :param col: name of the time column
    :param start_point: start point of time (it is set by default to the current system time)
    :param seconds: number of seconds
    :param milliseconds: number of milliseconds
    :param microseconds: number of microseconds
    :param minutes: number of minutes
    :param hours: number of hours
    :param days: number of days
    :param weeks: number of weeks
    :return: pandas dataframe
    """
    time = start_point - timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                                       milliseconds=milliseconds, microseconds=microseconds)
    res = pd.DataFrame()
    df = df.sort_values(by=col, ascending=False)

    for index, row in df.iterrows():
        val = row[col]
        if val > start_point:
            continue
        elif val > time:
            res = res.append(row)
        else:
            break

    return res.iloc[::-1]


def df_time_to_live(df, col, time=datetime.now()):
    """
    Returns a pandas dataframe that contains all rows from the dataframe 'df' where the value in the time column 'col' is
    greater than 'time'.
    :param df: pandas dataframe
    :param col: name of the time column
    :param time: start point of time (it is set by default to the current system time)
    :return: pandas dataframe
    """
    return df[df[col] > time]


def df_last_n_events(df, n=1):
    '''
    Returns a pandas dataframe that contains the last 'n' rows from the dataframe 'df' (n is set to 1 by default).
    :param df: pandas dataframe
    :param n: number of rows
    :return: pandas dataframe
    '''
    return df.tail(n)


def df_first_n_events(df, n=1):
    '''
    Returns a pandas dataframe that contains the first 'n' rows from the dataframe 'df' (n is set to 1 by default).
    :param df: pandas dataframe
    :param n: number of rows
    :return: pandas dataframe
    '''
    return df.head(n)


def df_sort(df, criteria, size=None):
    """
    Returns a pandas dataframe that contains the first 'size' elements of the dataframe 'df' after sorting it by the
    given criteria.
    :param df: pandas dataframe
    :param criteria: list of tupels (column_name, boolean). For example, (price, True) means: sort the 'price' column
    ascending. (price False) means: sort the 'price' column descending.
    :param size: size of the sort dataframe (number of rows). If it is set to None (by default), the function returns
    all rows.
    :return: pandas dataframe dataframe
    """
    df = df.sort_values( by=[elm[0] for elm in criteria], ascending=[elm[1] for elm in criteria])
    if size is None:
        return df
    else:
        return df.head(size)