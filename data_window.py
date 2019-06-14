from DataframeManager import *
from length_window import *
from datetime import timedelta, datetime
import numpy as np

retain = {}

def keep_all():
    retain.dataframe = all_dfs.dataframe
    return True

def expiry_exp_observer(key, exp):
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

    #def view_reference():
    #    return all_dfs[key].variables['view_reference']

    while not eval(exp):
        all_dfs[key].dataframe = all_dfs[key].dataframe.iloc[1:]
        all_dfs[key].variables['current_count'] = all_dfs[key].variables['current_count'] - 1
        all_dfs[key].variables['expired_count'] = all_dfs[key].variables['expired_count'] + 1
        first = all_dfs[key].dataframe.head(1)
        all_dfs[key].variables['oldest_event'] = first
        all_dfs[key].variables['oldest_timestamp'] = first['INSERTION_TIMESTAMP'].iloc[0]

def expiry_exp(exp):

    key = ('expiry_exp', exp)

    last = last_event()

    try:
        all_dfs[key].variables['current_count'] =  all_dfs[key].variables['current_count'] + 1
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = pd.DataFrame()
        all_dfs[key].observers.append(expiry_exp_observer)
        all_dfs[key].variables['current_count'] = 1
        all_dfs[key].variables['oldest_event'] = last
        all_dfs[key].variables['oldest_timestamp'] = last['INSERTION_TIMESTAMP'].iloc[0]
        # all_dfs[key].variables['view_reference'] = ?????????????????ß

    all_dfs[key].variables['newest_event'] = last
    all_dfs[key].variables['newest_timestamp'] = last['INSERTION_TIMESTAMP'].iloc[0]
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last)
    all_dfs[key].variables['expired_count'] = 0

    # Observer
    all_dfs[key].update_df(key, exp)

    return all_dfs[key].dataframe


def expiry_exp_batch_observer(key, exp):
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

    #def view_reference():
    #    return all_dfs[key].variables['view_reference']

    if eval(exp):
        #all_dfs[key].dataframe = all_dfs[key].dataframe.iloc[1:]
        batch_counter = batch_counter()
        all_dfs[key].dataframe = all_dfs['StockTick'].dataframe.tail(batch_counter)
        newest_event = newest_event()
        if not key[2]:
            all_dfs[key].dataframe = pd.concat([triggering_event(), all_dfs[key].dataframe[:-1]])
        all_dfs[key].variables['triggering_event'] = newest_event
        all_dfs[key].variables['triggering_event_timestamp'] = newest_event['INSERTION_TIMESTAMP'].iloc[0]
        all_dfs[key].variables['batch_counter'] = 0

def expiry_exp_batch(exp, include_triggering_event=True):

    key = ('expiry_exp_batch', exp, include_triggering_event)

    last = last_event()

    try:
        all_dfs[key].variables['batch_counter'] =  all_dfs[key].variables['batch_counter'] + 1
    except:
        columns = all_dfs['StockTick'].dataframe.columns
        all_dfs[key] = DataframeManager(columns_list=columns)
        all_dfs[key].observers.append(expiry_exp_batch_observer)
        all_dfs[key].variables['batch_counter'] = 1
        all_dfs[key].variables['triggering_event'] = pd.DataFrame({col:[np.nan] for col in columns})
        # all_dfs[key].variables['view_reference'] = ?????????????????

    all_dfs[key].variables['newest_event'] = last
    all_dfs[key].variables['newest_timestamp'] = last['INSERTION_TIMESTAMP'].iloc[0]

    # Observer
    all_dfs[key].update_df(key, exp)

    return all_dfs[key].dataframe

