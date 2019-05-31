from random import randrange
from DataframeManager import *
from length_window import *
from time_window import *

import logging
l=logging.getLogger("test")
f=logging.FileHandler("ceplogfile.log", mode='w')
l.addHandler(f)
s=logging.StreamHandler()
l.addHandler(s)


def avg_price_last_30_minutes():
    """

    :return:
    """
    avg_price = last_time(minutes=30)['price'].mean()
    l.critical(all_dfs['last_time_30m'].dataframe)
    l.critical('mean in the last 30 minutes: ' + str(avg_price))
    return False


def avg_price_last_two_events_observer():
    """

    :return:
    """
    avg_price = last_len(2)['price'].mean()
    l.critical(all_dfs['last_len_2'].dataframe)
    l.critical(size(all_dfs['last_len_2']))
    if avg_price > 6:
        l.critical('The average of the last two events is: ' + str(avg_price))
    return False


def test_first_five_events_observer():
    """

    :return:
    """
    a = first_len(5)
    l.critical(all_dfs['first_len_5'].dataframe)
    l.critical(size(all_dfs['first_len_5']))
    return False


def test_first_time_observer():
    """

    :return:
    """
    a = first_time(nanoseconds = 2)
    l.critical(all_dfs['first_time_2ns'].dataframe)
    return False


def test_length_batch():
    """

    :return:
    """
    a = length_batch(5)
    l.critical(all_dfs['length_batch_5'].dataframe)


def test_sort():
    """

    :return:
    """
    a = sort(5, [('price', 0), ('index', 1)])
    l.critical(all_dfs['sort_5_price_0_index_1'].dataframe)


all_dfs['StockTick'] = DataframeManager()
all_dfs['StockTick'].observers.update({avg_price_last_two_events_observer : []})
#all_dfs['StockTick'].observers.update({test_first_five_events_observer : []})
#all_dfs['StockTick'].observers.update({avg_price_last_30_minutes : []})
#all_dfs['StockTick'].observers.update({test_first_time_observer : []})
#all_dfs['StockTick'].observers.update({test_length_batch : []})
#all_dfs['StockTick'].observers.update({test_sort : []})


#22min for 75000k (23.5.2019)
for i in range(400):
    p = float(randrange(1, 10))
    #p=i
    print(p)
    all_dfs['StockTick'].add({'index' : i+1, 'symbol' : 'A', 'price' : p})
