from random import randrange
from DataframeManager import *
from length_window import *
from time_window import *


def avg_price_last_30_minutes():
    avg_price = last_time(minutes=30)['price'].mean()
    print(all_dfs['last_time_30m'].dataframe)
    print('mean in the last 30 minutes: ' + str(avg_price))
    return False


def avg_price_last_two_events_observer():
    avg_price = last_len(2)['price'].mean()
    print(all_dfs['last_len_2'].dataframe)
    print(size(all_dfs['last_len_2']))
    if avg_price > 6:
            print('The average of the last two events is: ' + str(avg_price))
    return False


def test_first_five_events_observer():
    a = first_len(5)
    print(all_dfs['first_len_5'].dataframe)
    print(size(all_dfs['first_len_5']))
    return False


def test_first_time_observer():
    a = first_time(nanoseconds = 2)
    print(all_dfs['first_time_2ns'].dataframe)
    return False


def test_length_batch():
    a = length_batch(5)
    print(all_dfs['length_batch_5'].dataframe)


def test_sort():
    a = sort(5, [('price', 0), ('index', 1)])
    print(all_dfs['sort_5_price_0_index_1'].dataframe)


all_dfs['StockTick'] = DataframeManager()
#all_dfs['StockTick'].observers.update({avg_price_last_two_events_observer : []})
#all_dfs['StockTick'].observers.update({test_first_five_events_observer : []})
#all_dfs['StockTick'].observers.update({avg_price_last_30_minutes : []})
#all_dfs['StockTick'].observers.update({test_first_time_observer : []})
#all_dfs['StockTick'].observers.update({test_length_batch : []})
all_dfs['StockTick'].observers.update({test_sort : []})


#22min for 75000k (23.5.2019)
for i in range(20):
    p = float(randrange(1, 10))
    print(p)
    all_dfs['StockTick'].add({'index' : i+1, 'symbol' : 'A', 'price' : p})
