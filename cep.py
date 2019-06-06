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


def avg_price_last_1_second():
    """
    This observer checks the average price of all events added in the last 1 second
    """
    avg_price = last_time(seconds=1)['price'].mean()
    l.critical(all_dfs[('last_time', 1, 's')].dataframe)
    l.critical('The average price of all events in the last 1 second: ' + str(avg_price))


def avg_price_last_five_events_observer():
    """
    This observer checks the average price of the last five events added
    """
    avg_price = last_len(5)['price'].mean()
    l.critical(all_dfs['last_len', 5].dataframe)
    l.critical('The average price of the last five events is: ' + str(avg_price))


def avg_price_first_five_events_observer():
    """
    This observer checks the average price of the first five events added
    """
    avg_price = first_len(5)['price'].mean()
    l.critical(all_dfs['first_len', 5].dataframe)
    l.critical('The average price of the first five events is: ' + str(avg_price))


def avg_price_first_three_seconds_observer():
    """
    This observer checks the average price of all events added in the first three seconds
    """
    avg_price = first_time(seconds = 3)['price'].mean()
    l.critical(all_dfs['first_time', 3, 's'].dataframe)
    l.critical('The average price of all events in the first three seconds is: ' + str(avg_price))


def avg_price_length_batch_5():
    """
    This observer checks the average price of the events in the length_batch(5) dataframe
    """
    try:
        avg_price = length_batch(5)['price'].mean()
        l.critical(all_dfs['length_batch', 5].dataframe)
        l.critical('The average price of all events in length_batch(5): ' + str(avg_price))
    except:
        l.critical('length_batch(5) dataframe is empty')


def avg_price_time_length_batch_5_1_min():
    """
    This observer checks the average price of the events in the time_length_batch(5, ) dataframe
    """
    try:
        avg_price = time_length_batch(20, seconds=1)['price'].mean()
        l.critical(all_dfs['time_length_batch', 20, 1, 's'].dataframe)
        l.critical('The average price of all events in length_batch(5): ' + str(avg_price))
    except:
        l.critical('length_batch(5) dataframe is empty')


def test_sort():
    """
    This observer checks the average price of the events in the sort dataframe sort(5, [('price', 0), ('index', 1)]) dataframe
    """
    avg_price = sort(5, [('price', 0), ('index', 1)])['price'].mean()
    l.critical(all_dfs['sort', 5, 'price', 0, 'index', 1].dataframe)
    l.critical("The average price of all events in sort(5, [('price', 0), ('index', 1)]): " + str(avg_price))


def avg_price_first_unique_price_symbol():
    """
    This observer checks the average price of the events in first_unique('price', 'symbol') dataframe
    """
    avg_price = first_unique('price', 'symbol')['price'].mean()
    l.critical(all_dfs['first_unique', 'price', 'symbol'].dataframe)
    l.critical("The average price of all events in first_unique('price', 'symbol'): " + str(avg_price))


all_dfs['StockTick'] = DataframeManager()


def test(i):
    if i == 1:
        all_dfs['StockTick'].observers.update({avg_price_last_five_events_observer : []})
    elif i == 2:
        all_dfs['StockTick'].observers.update({avg_price_first_five_events_observer : []})
    elif i == 3:
        all_dfs['StockTick'].observers.update({avg_price_last_1_second : []})
    elif i == 4:
        all_dfs['StockTick'].observers.update({avg_price_first_three_seconds_observer : []})
    elif i == 5:
        all_dfs['StockTick'].observers.update({avg_price_length_batch_5 : []})
    elif i == 6:
        all_dfs['StockTick'].observers.update({test_sort : []})
    elif i == 7:
        all_dfs['StockTick'].observers.update({avg_price_first_unique_price_symbol : []})
    elif i == 8:
        all_dfs['StockTick'].observers.update({avg_price_time_length_batch_5_1_min: []})

test(8)


#22min for 75000k (23.5.2019)
for i in range(40):
    p = float(randrange(1, 10))
    l.critical('new price: ' + str(p))
    all_dfs['StockTick'].add({'index' : i+1, 'symbol' : 'A', 'price' : p})
