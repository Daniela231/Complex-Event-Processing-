from DataframeManager import *
from length_window import *
from datetime import timedelta, datetime
import numpy as np

retain = {}


def keep_all():
    retain.dataframe = all_dfs.dataframe
    return True


def expiry_exp_observer(key, exp):
    """
    Observer for the expiry_exp dataframe.
    :param key: key of the expiry_exp dataframe
    :param exp: expiry expression for the expiry_exp dataframe
    :return: None
    """
    def current_count():
        return all_dfs[key].variables['current_count']

    def expired_count():
        return all_dfs[key].variables['expired_count']

    def newest_event():
        return all_dfs[key].variables['newest_event']

    def newest_timestamp():
        return all_dfs[key].variables['newest_timestamp']

    def oldest_event():
        return all_dfs[key].variables['oldest_event']

    def oldest_timestamp():
        return all_dfs[key].variables['oldest_timestamp']

    while not eval(exp):
        all_dfs[key].dataframe = all_dfs[key].dataframe.iloc[1:]
        all_dfs[key].variables['current_count'] = all_dfs[key].variables['current_count'] - 1
        all_dfs[key].variables['expired_count'] = all_dfs[key].variables['expired_count'] + 1
        first = all_dfs[key].dataframe.head(1)
        all_dfs[key].variables['oldest_event'] = first
        all_dfs[key].variables['oldest_timestamp'] = first['INSERTION_TIMESTAMP'].iloc[0]


def expiry_exp(exp):
    """
    This function returns the expiry_exp dataframe that applies the given expiry expression and removes events from the
    dataframe when the expression returns false.
    :param exp: string to be evaluated to boolean. Built-in properties of the expiry_exp dataframe:
        'current_count': number of events in the dataframe including the currently-arriving event.
        'expired_count': number of events expired during this evaluation.
        'newest_event':	last-arriving event.
        'newest_timestamp':	insertion timestamp of the last-arriving event.
        'oldest_event':	currently-evaluated event itself.
        'oldest_timestamp':	insertion timestamp of the currently-evaluated event.
    :return: expiry_exp dataframe
    """
    key = ('expiry_exp', exp)
    last = last_event()

    try:
        all_dfs[key].variables['current_count'] = all_dfs[key].variables['current_count'] + 1
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = pd.DataFrame()
        all_dfs[key].observers.append(expiry_exp_observer)
        all_dfs[key].variables['current_count'] = 1
        all_dfs[key].variables['oldest_event'] = last
        all_dfs[key].variables['oldest_timestamp'] = last['INSERTION_TIMESTAMP'].iloc[0]

    all_dfs[key].variables['newest_event'] = last
    all_dfs[key].variables['newest_timestamp'] = last['INSERTION_TIMESTAMP'].iloc[0]
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last)
    all_dfs[key].variables['expired_count'] = 0
    all_dfs[key].update_df(key, exp)

    return all_dfs[key].dataframe


def expiry_exp_batch_observer(key, exp):
    """
    Observer for the expiry_exp_batch dataframe.
    :param key: key of the expiry_exp_batch dataframe
    :param exp: expiry expression for the expiry_exp_batch dataframe
    :return: None
    """
    def batch_counter():
        return all_dfs[key].variables['batch_counter']

    def newest_event():
        return all_dfs[key].variables['newest_event']

    def newest_timestamp():
        return all_dfs[key].variables['newest_timestamp']

    def triggering_event():
        return all_dfs[key].variables['triggering_event']

    def triggering_event_timestamp():
        return all_dfs[key].variables['triggering_event_timestamp']

    if eval(exp):
        batch_counter = batch_counter()
        all_dfs[key].dataframe = all_dfs['StockTick'].dataframe.tail(batch_counter)
        newest_event = newest_event()
        if not key[2]:
            all_dfs[key].dataframe = pd.concat([triggering_event(), all_dfs[key].dataframe[:-1]])
        all_dfs[key].variables['triggering_event'] = newest_event
        all_dfs[key].variables['triggering_event_timestamp'] = newest_event['INSERTION_TIMESTAMP'].iloc[0]
        all_dfs[key].variables['batch_counter'] = 0


def expiry_exp_batch(exp, include_triggering_event=True):
    """
    This function returns the expiry_exp_batch dataframe that buffers events and releases them when a given expiry
    expression returns true.
    :param exp: string to be evaluated to boolean. Built-in properties of the expiry_exp_batch dataframe:
        'batch_counter': number of events waiting to be released in the next update including the
        currently-arriving event.
        'newest_event':	last-arriving event.
        'newest_timestamp':	insertion timestamp of the last-arriving event.
        'triggering_event':	event that has triggered the batch in the last update.
        'triggering_event_timestamp': insertion timestamp of the triggering event.
    :param include_triggering_event: boolean that defines whether to include the event that triggers the batch in the
    current batch (true, the default) or in the next batch (false).
    :return: expiry_exp_batch dataframe
    """
    key = ('expiry_exp_batch', exp, include_triggering_event)
    last = last_event()

    try:
        all_dfs[key].variables['batch_counter'] = all_dfs[key].variables['batch_counter'] + 1
    except:
        columns = all_dfs['StockTick'].dataframe.columns
        all_dfs[key] = DataframeManager(columns_list=columns)
        all_dfs[key].observers.append(expiry_exp_batch_observer)
        all_dfs[key].variables['batch_counter'] = 1
        all_dfs[key].variables['triggering_event'] = pd.DataFrame({col: [np.nan] for col in columns})

    all_dfs[key].variables['newest_event'] = last
    all_dfs[key].variables['newest_timestamp'] = last['INSERTION_TIMESTAMP'].iloc[0]
    all_dfs[key].update_df(key, exp)
    return all_dfs[key].dataframe

