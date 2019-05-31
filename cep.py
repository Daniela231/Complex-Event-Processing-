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


def avg_price_last_3_nanoseconds():
    """

    :return:
    """
    avg_price = last_time(nanoseconds=3)['price'].mean()
    l.critical(all_dfs[('last_time', 3, 'ns')].dataframe)
    l.critical('mean in the last 3 nanoseconds: ' + str(avg_price))


def avg_price_last_two_events_observer():
    """

    :return:
    """
    avg_price = last_len(2)['price'].mean()
    l.critical(all_dfs['last_len', 2].dataframe)
    if avg_price > 6:
        l.critical('The average of the last two events is: ' + str(avg_price))


def test_first_five_events_observer():
    """

    :return:
    """
    l.critical(first_len(5))


def test_first_time_observer():
    """

    :return:
    """
    l.critical(first_time(seconds = 1))


def test_length_batch():
    """

    :return:
    """
    l.critical(length_batch(5))


def test_sort():
    """

    :return:
    """
    l.critical(sort(5, [('price', 0), ('index', 1)]))


def test_first_unique():
    """

    :return:
    """
    l.critical(first_unique('price', 'symbol'))


all_dfs['StockTick'] = DataframeManager()
#all_dfs['StockTick'].observers.update({avg_price_last_two_events_observer : []})
#all_dfs['StockTick'].observers.update({test_first_five_events_observer : []})
#all_dfs['StockTick'].observers.update({avg_price_last_3_nanoseconds : []})
#all_dfs['StockTick'].observers.update({test_first_time_observer : []})
#all_dfs['StockTick'].observers.update({test_length_batch : []})
#all_dfs['StockTick'].observers.update({test_sort : []})
all_dfs['StockTick'].observers.update({test_first_unique : []})


#22min for 75000k (23.5.2019)
for i in range(20):
    p = float(randrange(1, 10))
    l.critical(p)
    all_dfs['StockTick'].add({'index' : i+1, 'symbol' : 'A', 'price' : p})
