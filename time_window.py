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

    :param key:
    :param time:
    :return:
    """
    all_dfs[key].dataframe = all_dfs[key].dataframe[all_dfs[key].dataframe['INSERTION_TIMESTAMP'] > np.datetime64('now') - time]
    return True


def last_time(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
    """

    :param weeks:
    :param days:
    :param hours:
    :param minutes:
    :param seconds:
    :param milliseconds:
    :param microseconds:
    :param nanoseconds:
    :return:
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

    :param time:
    :return:
    """
    return np.datetime64('now') < time


def first_time(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
    """

    :param weeks:
    :param days:
    :param hours:
    :param minutes:
    :param seconds:
    :param milliseconds:
    :param microseconds:
    :param nanoseconds:
    :return:
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