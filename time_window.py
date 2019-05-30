import logging
from DataframeManager import *
from length_window import last_event


abbreviations = {
        'years':'Y',
        'months':'M',
        'weeks':'W',
        'days':'D',
        'hours':'h',
        'minutes':'m',
        'seconds':'s',
        'milliseconds':'ms',
        'microseconds':'us',
        'nanoseconds':'ns',
}


def last_time_observer(key, time):
    """
    observer for the last_time dataframe
    :param key: key of the last_time dataframe
    :param time: timespan on the dataframe from system time to system time - time
    :return: True as we want to add the last event of the stream to the dataframe
    """
    all_dfs[key].dataframe = all_dfs[key].dataframe[all_dfs[key].dataframe['INSERTION_TIMESTAMP'] > np.datetime64('now') - time]
    return True


def last_time(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
    """
    Returns the dataframe of all events arriving within a given time after statement start
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :param nanoseconds: number of nanoseconds into the past
    :return: last_time dataframe
    """
    now = np.datetime64('now')
    del dict['now']
    dict = locals()
    key = 'last_time'
    time = 0

    for i in dict.keys():
        if dict[i]!= 0:
            time = time + np.timedelta64(int(dict[i]), abbreviations[i])
            key = key + '_' + str(dict[i]) + abbreviations[i]

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
    else:
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[all_dfs["StockTick"].dataframe['INSERTION_TIMESTAMP'] > now - time]
        all_dfs[key].observers.update({last_time_observer : [key, time]})
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe


def first_time_observer(time):
    """
    observer for the first_time dataframe
    :param time: timespan on the dataframe
    :return: True if we want to add last event, else False
    """
    return np.datetime64('now') < time


def first_time(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
    """
    Returns the dataframe looking back a given time into the past from the current system time
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :param nanoseconds: number of nanoseconds into the past
    :return: first_time dataframe
    """
    dict = locals()
    key = 'first_time'
    time = list(all_dfs['StockTick'].dataframe['INSERTION_TIMESTAMP'])[0]

    for i in dict.keys():
        if dict[i]!= 0:
            time = time + np.timedelta64(int(dict[i]), abbreviations[i])
            key = key + '_' + str(dict[i]) + abbreviations[i]

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[all_dfs["StockTick"].dataframe['INSERTION_TIMESTAMP'] < time]
        all_dfs[key].observers.update({first_time_observer : [time]})
    else:
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe