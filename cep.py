from random import randrange
from DataframeManager import *
from length_window import last_event, last_len, first_len
from time_window import last_time


def avg_price_last_30_minutes():
    avg_price = last_time(minutes=30)['price'].mean()
    print(all_dfs['last_time_30m'].dataframe)
    print('mean in the last 30 minutes: ' + str(avg_price))


def avg_price_last_two_events_observer():
    avg_price = last_len(2)['price'].mean()
    print(all_dfs['last_len_2'].dataframe)
    if avg_price > 6:
            print('The average of the last two events is: ' + str(avg_price))


def test_first_five_events_observer():
    a = first_len(5)
    print(all_dfs['first_len_5'].dataframe)


all_dfs['StockTick'] = DataframeManager(['symbol', 'price'])
all_dfs['StockTick'].observers.update({avg_price_last_two_events_observer : []})
all_dfs['StockTick'].observers.update({test_first_five_events_observer : []})
all_dfs['StockTick'].observers.update({avg_price_last_30_minutes : []})


for i in range(20):
    all_dfs['StockTick'].add({'symbol' : 'AAPL', 'price' : float(randrange(1, 10))})