from DataframeManager import *
from length_window import last_event, sort
from datetime import datetime, timedelta
import logging

l = logging.getLogger("cepgenerator")


def last_time(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0):
    """
    Returns the dataframe looking back a given time into the past from the current system time considering the insertion
    timestamp of the events.
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :return: last_time dataframe
    """
    return externally_last_time(col='INSERTION_TIMESTAMP', weeks=weeks, days=days, hours=hours, minutes=minutes,
                                seconds=seconds, milliseconds=milliseconds, microseconds=microseconds)


def externally_last_time_observer(key, col, time):
    """
    Observer for the externally_last_time dataframe
    :param col:
    :param key: key of the externally_last_time dataframe
    :param time: time for the externally_last_time dataframe
    :return: None
    """
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())
    all_dfs[key].dataframe = all_dfs[key].dataframe[all_dfs[key].dataframe[col] > time]


def externally_last_time(col, weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0):
    """
    This function returns the dataframe looking back a given time into the past from the current system time based on
    the time values from the time column 'col'. The key difference between externally_last_time and last_time is that
    the externally_last_time dataframe is based on the time values from 'col' instead of the insertion timestamp.
    :param col: name of the considered time column
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :return: externally_last_time dataframe
    """
    now = datetime.now()
    dict = locals()
    del dict['now']
    del dict['col']
    key = ('externally_last_time', col)
    for i, val in dict.items():
        if val != 0:
            key = key + (i, val)
    time = now - timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds, milliseconds=milliseconds,
                     microseconds=microseconds)

    try:
        all_dfs[key].update_df(key, col, time)
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[all_dfs["StockTick"].dataframe[col] > time]
        all_dfs[key].observers.append(externally_last_time_observer)

    return all_dfs[key].dataframe


def first_time_observer(list):
    """
    observer for the first_time dataframe
    :param list:
    :return: None
    """
    key = list[0]
    last = last_event()

    if last['INSERTION_TIMESTAMP'].iloc[0] - all_dfs[key].variables['statement_start'] < all_dfs[key].variables['time'] \
            and last['INSERTION_TIMESTAMP'].iloc[0] >= all_dfs[key].variables['statement_start']:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


def first_time(statement_start=None, time=None, seconds=0, milliseconds=0, microseconds=0, minutes=0, hours=0, days=0,
               weeks=0):
    """
    Returns the dataframe of all events arriving within a given time after statement_start.
    :param statement_start: start point of time. If statement_start is set to None (by default), the first time the
    function is called with the same parameters will be considered as the statement_start.
    :param time: time value. If it is set to None, time will be calculated from the next parameters.
    :param seconds: number of seconds
    :param milliseconds: number of milliseconds
    :param microseconds: number of microseconds
    :param minutes: number of minutes
    :param hours: number of hours
    :param days: number of days
    :param weeks: number of weeks
    :return: first_time dataframe
    """
    time = time or timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                         milliseconds=milliseconds, microseconds=microseconds)
    key = ('first_time', statement_start, time)

    try:
        df = all_dfs[key].dataframe
    except:
        statement_start = statement_start or datetime.now()
        all_dfs[key] = DataframeManager(columns_list=all_dfs['StockTick'].dataframe.columns)

        for index in reversed(range(len(all_dfs['StockTick'].dataframe.index))):
            row = all_dfs['StockTick'].dataframe.iloc[[index]]
            t = row['INSERTION_TIMESTAMP'].iloc[0]
            if t - statement_start < time and t < statement_start:
                all_dfs[key].dataframe = all_dfs[key].dataframe.append(row)

        all_dfs['StockTick'].observers_with_param.append([first_time_observer, key])
        all_dfs[key].variables['statement_start'] = statement_start
        all_dfs[key].variables['time'] = time
        l.critical('statement_start for ' + str(key) + ' : ' + str(statement_start))
        df = all_dfs[key].dataframe

    return df


import threading
result = None
result_available = threading.Event()


def time_batch_observer(key, time):
    """
    Observer for the time_batch dataframe
    :param key: key of the dataframe
    :param time: time for the dataframe
    :return: None
    """
    if(datetime.now() < time):
        result = datetime.now() < time
        result_available.set()
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


def time_batch(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0, reference_point=None):
    """
    Returns the dataframe buffering events up to a defined time period
    :param reference_point:
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :return: time_batch dataframe
    """
    if reference_point is None:
        reference_point = datetime.now()
    time = reference_point + timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                                       milliseconds=milliseconds, microseconds=microseconds)
    while True:
        dict = locals()
        first_time(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                           milliseconds=milliseconds, microseconds=microseconds)
        thread = threading.Thread(target=time_batch_observer)
        thread.start()
        result_available.wait()
    return True


def time_length_batch_observer(key, len, time):
    """
    Observer for the time_length_batch dataframe
    :param key: key of the dataframe
    :param len: maximal length of the batch
    :param time: maximal timespan for the batch
    :return: None
    """
    count = all_dfs[key].variables['count']
    if count == len or all_dfs[key].variables['last_update_time'] < time:
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.tail(count)
        all_dfs[key].variables['count'] = 0
        all_dfs[key].variables['last_update_time'] = datetime.now()


def time_length_batch(len, weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0):
    """
    Returns the dataframe that batches events and releases them when it has collected 'len' events or the given time
    interval has passed.
    :param len: maximal length of the batch
    :param weeks: number of weeks
    :param days: number of days
    :param hours: number of hours
    :param minutes: number of minutes
    :param seconds: number of seconds
    :param milliseconds: number of milliseconds
    :param microseconds: number of microseconds
    :return: time_length_batch dataframe
    """
    now = datetime.now()
    dict = locals()
    del dict['len']
    del dict['now']
    key = ('time_length_batch', len)
    for i, val in dict.items():
        if val != 0:
            key = key + (i, val)

    time = now - timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                           milliseconds=milliseconds, microseconds=microseconds)

    try:
        all_dfs[key].variables['count'] = all_dfs[key].variables['count'] + 1
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = pd.DataFrame()
        all_dfs[key].observers.append(time_length_batch_observer)
        all_dfs[key].variables['count'] = 1
        all_dfs[key].variables['last_update_time'] = now

    all_dfs[key].update_df(key, len, time)
    return all_dfs[key].dataframe


def ext_time_batch_observer(key, lasttime, time):
    """
    observer for the ext_time_batch_observer dataframe
    :param key: key of the dataframe
    :param lasttime:
    :param time: timespan on the dataframe
    :return: None
    """
    if lasttime < time:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


def ext_time_batch(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0):
    """
    Returns the dataframe buffering events up to a defined time period but time based on timestamp expression
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :return: ext_time_batch dataframe
    """

    while True:
        dict = locals()
        key = ('ext_time',)
        time = list(all_dfs['StockTick'].dataframe['INSERTION_TIMESTAMP'])[0] \
               + timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                           milliseconds=milliseconds, microseconds=microseconds)
        for i, val in dict.items():
            if val != 0:
                key = key + (i, val)

        if key not in all_dfs.keys():
            all_dfs[key] = DataframeManager()
            all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[
                all_dfs["StockTick"].dataframe['INSERTION_TIMESTAMP'] < time]
            all_dfs[key].observers.append(ext_time_batch_observer)
        else:
            all_dfs[key].update_df(key, all_dfs['StockTick'].dataframe['INSERTION_TIMESTAMP'],time)

    return all_dfs[key].dataframe


def time_accum_observer(key, timedelta, time):
    """
    observer for the time_accum_observer dataframe
    :param key: key of the dataframe
    :param timedelta: delta of our given times
    :param time: timespan on the dataframe
    :return: None
    """
    if timedelta < time:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


def time_accum(weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0):
    """
    Returns the dataframe when no more events arrive in a given time
    :param weeks: number of weeks into the past
    :param days: number of days into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :return: time_accum dataframe
    """
    dict = locals()
    key = 'time_accum'
    time = 0

    time = list(all_dfs['StockTick'].dataframe['INSERTION_TIMESTAMP'])[0] \
           + timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
                       milliseconds=milliseconds, microseconds=microseconds)
    for i, val in dict.items():
        if val != 0:
            key = key + (i, val)

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe[all_dfs["StockTick"].dataframe['INSERTION_TIMESTAMP']]
        all_dfs[key].observers.append(time_accum_observer)
    else:
        all_dfs[key].update_df(key, (datetime.now() - last_event()['INSERTION_TIMESTAMP'].iloc[0]), time)
        lastEventTime = datetime.now()

    return all_dfs[key].dataframe


def time_order(timestamp_expression, weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, microseconds=0):
    """
    This function returns a dataframe that orders events that arrive out-of-order using timestamp values from the
    column 'timestamp_expression' and by comparing that timestamp values to engine system time. The other parameters
    define the time interval that an arriving event should maximally be held in this dataframe. The only difference
    between this function and externally_last_time is that time_order orders the event by 'timestamp_expression'.
    :param timestamp_expression: name of the time column
    :param weeks: number of weeks into the past
    :param days: number of dass into the past
    :param hours: number of hours into the past
    :param minutes: number of minutes into the past
    :param seconds: number of seconds into the past
    :param milliseconds: number of milliseconds into the past
    :param microseconds: number of microseconds into the past
    :return: externally_last_time dataframe ordered by 'timestamp_expression'
    """
    df = externally_last_time(col=timestamp_expression, weeks=weeks, days=days, hours=hours, minutes=minutes,
                                seconds=seconds, milliseconds=milliseconds, microseconds=microseconds)
    return df.sort_values(by=timestamp_expression, kind='mergesort')


def time_to_live(col):
    """
    This function returns the dataframe that retains each event until engine time reaches the time value stored in the
    column 'col'
    :param col: name of the time column
    :return: time_to_live dataframe
    """
    return time_order(timestamp_expression=col)