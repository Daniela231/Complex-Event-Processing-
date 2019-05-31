from DataframeManager import *
from length_window import last_event

lastEventTime = np.datetime64('now')
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
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


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
    dict = locals()
    del dict['now']
    key = ('last_time',)
    time = 0

    for i in dict.keys():
        if dict[i]!= 0:
            time = time + np.timedelta64(int(dict[i]), abbreviations[i])
            key = key + (dict[i], abbreviations[i])

    try:
        all_dfs[key].update_df()
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[all_dfs["StockTick"].dataframe['INSERTION_TIMESTAMP'] > now - time]
        all_dfs[key].observers.update({last_time_observer : [key, time]})

    return all_dfs[key].dataframe


def first_time_observer(time):
    """
    observer for the first_time dataframe
    :param time: timespan on the dataframe
    :return: True if we want to add last event, else False
    """
    return np.datetime64('now') < time
def first_time_observer(key, time):
    if np.datetime64('now') < time:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


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
    key = ('first_time',)
    time = list(all_dfs['StockTick'].dataframe['INSERTION_TIMESTAMP'])[0]

    for i in dict.keys():
        if dict[i]!= 0:
            time = time + np.timedelta64(int(dict[i]), abbreviations[i])
            key = key + (dict[i], abbreviations[i])

    try:
        all_dfs[key].update_df()
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[all_dfs["StockTick"].dataframe['INSERTION_TIMESTAMP'] < time]
        all_dfs[key].observers.update({first_time_observer : [key, time]})

    return all_dfs[key].dataframe


import threading
result = None
result_available = threading.Event()


def time_batch_observer(key, time):
def time_batch_observer(time):
    """
    observer for the time_batch_observer dataframe
    :param time: timespan on the dataframe
    :return: True if we want to add last event, else False
    """
    if(np.datetime64('now') < time):
        result = np.datetime64('now') < time
        result_available.set()
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


def time_batch(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
    """
    Returns the dataframe buffering events up to a defined time period
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :param nanoseconds: number of nanoseconds into the past
    :return: time_batch dataframe
    """
     while True:
        dict = locals()
        time = list(all_dfs['StockTick'].dataframe['INSERTION_TIMESTAMP'])[0]

        for i in dict.keys():
            if dict[i] != 0:
                time = time + np.timedelta64(int(dict[i]), abbreviations[i])

        first_time(weeks,days,hours,minutes,seconds,milliseconds,microseconds,nanoseconds)

        thread = threading.Thread(target=time_batch_observer)
        thread.start()
        result_available.wait()
     return True


def ext_time_batch_observer(lasttime,time):
    """
    observer for the ext_time_batch_observer dataframe
    :param lasttime:
    :param time: timespan on the dataframe
    :return: True if we want to add last event, else False
    """
    return lasttime < time

def ext_time_batch_observer(key, lasttime, time):
    if lasttime < time:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())

def ext_time_batch(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
    """
    Returns the dataframe buffering events up to a defined time period but time based on timestamp expression
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :param nanoseconds: number of nanoseconds into the past
    :return: wxt_time_batch dataframe
    """

    while True:
        dict = locals()
        key = 'ext_time'
        time = list(all_dfs['StockTick'].dataframe['INSERTION_TIMESTAMP'])[0]
        for i in dict.keys():
            if dict[i] != 0:
                time = time + np.timedelta64(int(dict[i]), abbreviations[i])
                key = key + '_' + str(dict[i]) + abbreviations[i]

        if key not in all_dfs.keys():
            all_dfs[key] = DataframeManager()
            all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[
                all_dfs["StockTick"].dataframe['INSERTION_TIMESTAMP'] < time]
            all_dfs[key].observers.update({ext_time_batch_observer: [key, all_dfs['StockTick'].dataframe['INSERTION_TIMESTAMP'],time]})
        else:
            all_dfs[key].update_df()


    return all_dfs[key].dataframe


def time_accum_observer(timedelta, time):
    """
    observer for the time_accum_observer dataframe
    :param timedelta: delta of our given times
    :param time: timespan on the dataframe
    :return: True if we want to add last event, else False
    """
    return timedelta < time
    return True
def time_accum_observer(key, timedelta, time):
    if timedelta < time:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())
    #return timedelta < time
    #return True


def time_accum(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
    """
    Returns the dataframe when no more events arrive in a given time
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :param nanoseconds: number of nanoseconds into the past
    :return: time_accum dataframe
    """
    dict = locals()
    key = 'time_accum'
    time = 0

    for i in dict.keys():
        if dict[i]!= 0:
            time = time + np.timedelta64(int(dict[i]), abbreviations[i])
            key = key + '_' + str(dict[i]) + abbreviations[i]

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[all_dfs["StockTick"].dataframe['INSERTION_TIMESTAMP']]
        all_dfs[key].observers.update({time_accum_observer : [key, np.timedelta64('now')-lastEventTime, time]})
    else:
        all_dfs[key].update_df()
        lastEventTime = np.datetime64('now')

    return all_dfs[key].dataframe