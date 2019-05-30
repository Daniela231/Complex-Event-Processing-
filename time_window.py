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
    all_dfs[key].dataframe = all_dfs[key].dataframe[all_dfs[key].dataframe['INSERTION_TIMESTAMP'] > np.datetime64('now') - time]
    return True


def last_time(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, nanoseconds=0):
    now = np.datetime64('now')
    dict = locals()
    del dict['now']
    key = 'last_time'
    time = 0

    for i in dict.keys():
        if dict[i]!= 0:
            time = time + np.timedelta64(int(dict[i]), abbreviations[i])
            key = key + '_' + str(dict[i]) + abbreviations[i]

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[all_dfs["StockTick"].dataframe['INSERTION_TIMESTAMP'] > now - time]
        all_dfs[key].observers.update({last_time_observer : [key, time]})
    else:
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe


def first_time_observer(time):
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


result = None
result_available = threading.Event()


def time_batch_observer(time):
    if(np.datetime64('now') < time):
        result = np.datetime64('now') < time
        result_available.set()
    return np.datetime64('now') < time


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


def ext_time_batch_observer(lasttime,time):
    return lasttime < time


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
            all_dfs[key].observers.update({ext_time_batch_observer: [all_dfs['StockTick'].dataframe['INSERTION_TIMESTAMP'],time]})
        else:
            all_dfs[key].add_df(last_event())

        x = input("Continue? (y/n)")
        if x == "y":
            continueBatch = True
        else:
            continueBatch = False

    return all_dfs[key].dataframe
