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
    all_dfs[key].dataframe = all_dfs[key].dataframe[all_dfs[key].dataframe['INSERTION_TIMESTAMP'] > np.datetime64('now') - time]
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


def last_time(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
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


def first_time_observer(key, time):
    if np.datetime64('now') < time:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


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
    if(np.datetime64('now') < time):
        result = np.datetime64('now') < time
        result_available.set()
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


def time_batch(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
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


def ext_time_batch_observer(key, lasttime, time):
    if lasttime < time:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())

def ext_time_batch(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
    continueBatch = True
    while continueBatch:
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

        x = input("Continue? (y/n)")
        if x == "y":
            continueBatch = True
        else:
            continueBatch = False

    return all_dfs[key].dataframe

def time_accum_observer(key, timedelta, time):
    if timedelta < time:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())
    #return timedelta < time
    #return True


def time_accum(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):

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