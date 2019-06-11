from random import randrange
from DataframeManager import *
from length_window import *
from time_window import *
from statistic_views import *
from datetime import datetime


import logging
l=logging.getLogger("test")
f=logging.FileHandler("ceplogfile.log", mode='w')
l.addHandler(f)
s=logging.StreamHandler()
l.addHandler(s)


# Test 1
def avg_price_last_five_events_observer():
    """
    This observer checks the average price of the last five events added
    """
    avg_price = last_len(5)['price'].mean()
    l.critical(all_dfs['last_len', 5].dataframe)
    l.critical('The average price of the last five events is: ' + str(avg_price))


# Test 2
def avg_price_first_five_events_observer():
    """
    This observer checks the average price of the first five events added
    """
    avg_price = first_len(5)['price'].mean()
    l.critical(all_dfs['first_len', 5].dataframe)
    l.critical('The average price of the first five events is: ' + str(avg_price))


# Test 3
def avg_price_last_1_second():
    """
    This observer checks the average price of all events added in the last 1 second
    """
    avg_price = last_time(seconds=1)['price'].mean()
    l.critical(all_dfs['last_time', 'seconds', 1].dataframe)
    l.critical('The average price of all events in the last 1 second: ' + str(avg_price))


#Test 4
def avg_price_first_two_seconds_observer():
    """
    This observer checks the average price of all events added in the first three seconds
    """
    avg_price = first_time(seconds = 2)['price'].mean()
    l.critical(all_dfs['first_time', 'seconds', 2].dataframe)
    l.critical('The average price of all events in the first two seconds is: ' + str(avg_price))


# Test 5
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


# Test 6
def test_sort():
    """
    This observer checks the average price of the events in the sort dataframe sort(5, [('price', 0), ('index', 1)]) dataframe
    """
    avg_price = sort(5, [('price', 0), ('index', 1)])['price'].mean()
    l.critical(all_dfs['sort', 5, 'price', 0, 'index', 1].dataframe)
    l.critical("The average price of all events in sort(5, [('price', 0), ('index', 1)]): " + str(avg_price))


# Test 7
def avg_price_first_unique_price_symbol():
    """
    This observer checks the average price of the events in first_unique('price', 'symbol') dataframe
    """
    avg_price = first_unique('price', 'symbol')['price'].mean()
    l.critical(all_dfs['first_unique', 'price', 'symbol'].dataframe)
    l.critical("The average price of all events in first_unique('price', 'symbol'): " + str(avg_price))


# Test 8
def avg_price_time_length_batch_15_1_second():
    """
    This observer checks the average price of the events in the time_length_batch(15, seconds=1) dataframe
    """
    try:
        avg_price = time_length_batch(15, seconds=1)['price'].mean()
        l.critical(all_dfs['time_length_batch', 15, 'seconds', 1].dataframe)
        l.critical('The average price of all events in time_length_batch(15, seconds=1): ' + str(avg_price))
    except:
        l.critical('time_length_batch(15, milliseconds=1) dataframe is empty')


# Test 9
def avg_price_last_unique_price_symbol():
    """
    This observer checks the average price of the events in last_unique('price', 'symbol') dataframe
    """
    avg_price = last_unique('price', 'symbol')['price'].mean()
    l.critical(all_dfs['last_unique', 'price', 'symbol'].dataframe)
    l.critical("The average price of all events in last_unique('price', 'symbol'): " + str(avg_price))


# Test 10
def avg_price_last_1_second_externally_time():
    """
    This observer checks the average price of all events in the last 1 second regarding externally time
    """
    avg_price = externally_last_time(col = 'time', seconds=1)['price'].mean()
    l.critical(all_dfs['externally_last_time', 'time', 'seconds', 1].dataframe)
    l.critical('The average price of all events in the last 1 second regarding externally time: ' + str(avg_price))


#Test 11 /given dataframe test
def correlation_method_test():
    # Dataframe for simple correlation test
    df = pd.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)], columns=['A', 'B'])

    # Creating the first dataframe
    df1 = pd.DataFrame({"A": [1, 5, 7, 8],
                        "B": [5, 8, 4, 3],
                        "C": [10, 4, 9, 3]})

    # Creating the second dataframe
    df2 = pd.DataFrame({"A": [5, 3, 6, 4],
                        "B": [11, 2, 4, 3],
                        "C": [4, 3, 8, 5]})
    l.critical(df1, "\n")
    l.critical(df2)
    l.critical(prepare_for_correl(df2, ['A', 'B', 'C']))
    l.critical(simple_correl('pearson', df, 2))
    l.critical(correlwith(df1, df2, 'pearson', 0))
    l.critical(correlwith(df1, df2, 'pearson', 1))


all_dfs['StockTick'] = DataframeManager()


def test(i):
    if i == 1:
        all_dfs['StockTick'].observers.append(avg_price_last_five_events_observer)
    elif i == 2:
        all_dfs['StockTick'].observers.append(avg_price_first_five_events_observer)
    elif i == 3:
        all_dfs['StockTick'].observers.append(avg_price_last_1_second)
    elif i == 4:
        all_dfs['StockTick'].observers.append(avg_price_first_two_seconds_observer)
    elif i == 5:
        all_dfs['StockTick'].observers.append(avg_price_length_batch_5)
    elif i == 6:
        all_dfs['StockTick'].observers.append(test_sort)
    elif i == 7:
        all_dfs['StockTick'].observers.append(avg_price_first_unique_price_symbol)
    elif i == 8:
        all_dfs['StockTick'].observers.append(avg_price_time_length_batch_15_1_second)
    elif i == 9:
        all_dfs['StockTick'].observers.append(avg_price_last_unique_price_symbol)
    elif i == 10:
        all_dfs['StockTick'].observers.append(avg_price_last_1_second_externally_time)
    elif i == 11:
        correlation_method_test()


test(11)


#22min for 75000k (23.5.2019)
now = datetime.now()
for i in range(40):
    if i % 3 == 0:
        now = datetime.now()
    p = float(randrange(1, 10))
    l.critical('new price: ' + str(p))
    all_dfs['StockTick'].add({'index' : i+1, 'symbol' : 'A', 'price' : p, 'time' : now })
