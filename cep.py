from random import randrange
from DataframeManager import *
from length_window import *
from time_window import *
from data_window import *
from special_derived_value_window import *
from statistic_views import *
from datetime import datetime
from LogManager import *
from logging import *

#Logger for the new price
l=logging.getLogger("cepgenerator")
f=logging.FileHandler("cepgenerator.log", mode='w')
l.addHandler(f)
s=logging.StreamHandler()
l.addHandler(s)

#Block for creating all different loggers for the tests
i1 = logging.getLogger("test"+str(1))
i2 = logging.getLogger("test"+str(2))
i3 = logging.getLogger("test"+str(3))
i4 = logging.getLogger("test"+str(4))
i5 = logging.getLogger("test"+str(5))
i6 = logging.getLogger("test"+str(6))
i7 = logging.getLogger("test"+str(7))
i8 = logging.getLogger("test"+str(8))
i9 = logging.getLogger("test"+str(9))
i10 = logging.getLogger("test"+str(10))
i11 = logging.getLogger("test"+str(11))
i12 = logging.getLogger("test"+str(12))
i13 = logging.getLogger("test"+str(13))
i14 = logging.getLogger("test"+str(14))

# Test 1
def avg_price_last_five_events_observer():
    """
    This observer checks the average price of the last five events added
    """
    avg_price = last_len(5)['price'].mean()
    i1.critical(all_dfs['last_len', 5].dataframe)
    i1.critical('The average price of the last five events is: ' + str(avg_price))


# Test 2
def avg_price_first_five_events_observer():
    """
    This observer checks the average price of the first five events added
    """
    avg_price = first_len(5)['price'].mean()
    i2.critical(all_dfs['first_len', 5].dataframe)
    i2.critical('The average price of the first five events is: ' + str(avg_price))


# Test 3
def avg_price_last_1_second():
    """
    This observer checks the average price of all events added in the last 1 second
    """
    avg_price = last_time(seconds=1)['price'].mean()
    i3.critical(all_dfs['last_time', 'seconds', 1].dataframe)
    i3.critical('The average price of all events in the last 1 second: ' + str(avg_price))


#Test 4
def avg_price_first_two_seconds_observer():
    """
    This observer checks the average price of all events added in the first three seconds
    """
    avg_price = first_time(seconds = 2)['price'].mean()
    i4.critical(all_dfs['first_time', 'seconds', 2].dataframe)
    i4.critical('The average price of all events in the first two seconds is: ' + str(avg_price))


# Test 5
def avg_price_length_batch_5():
    """
    This observer checks the average price of the events in the length_batch(5) dataframe
    """
    try:
        avg_price = length_batch(5)['price'].mean()
        i5.critical(all_dfs['length_batch', 5].dataframe)
        i5.critical('The average price of all events in length_batch(5): ' + str(avg_price))
    except:
        i5.critical('length_batch(5) dataframe is empty')


# Test 6
def test_sort():
    """
    This observer checks the average price of the events in the sort dataframe sort(5, [('price', 0), ('index', 1)]) dataframe
    """
    avg_price = sort(5, [('price', 0), ('index', 1)])['price'].mean()
    i6.critical(all_dfs['sort', 5, 'price', 0, 'index', 1].dataframe)
    i6.critical("The average price of all events in sort(5, [('price', 0), ('index', 1)]): " + str(avg_price))


# Test 7
def avg_price_first_unique_price_symbol():
    """
    This observer checks the average price of the events in first_unique('price', 'symbol') dataframe
    """
    avg_price = first_unique('price', 'symbol')['price'].mean()
    i7.critical(all_dfs['first_unique', 'price', 'symbol'].dataframe)
    i7.critical("The average price of all events in first_unique('price', 'symbol'): " + str(avg_price))


# Test 8
def avg_price_time_length_batch_15_1_second():
    """
    This observer checks the average price of the events in the time_length_batch(15, seconds=1) dataframe
    """
    try:
        avg_price = time_length_batch(15, seconds=1)['price'].mean()
        i8.critical(all_dfs['time_length_batch', 15, 'seconds', 1].dataframe)
        i8.critical('The average price of all events in time_length_batch(15, seconds=1): ' + str(avg_price))
    except:
        i8.critical('time_length_batch(15, milliseconds=1) dataframe is empty')


# Test 9
def avg_price_last_unique_price_symbol():
    """
    This observer checks the average price of the events in last_unique('price', 'symbol') dataframe
    """
    avg_price = last_unique('price', 'symbol')['price'].mean()
    i9.critical(all_dfs['last_unique', 'price', 'symbol'].dataframe)
    i9.critical("The average price of all events in last_unique('price', 'symbol'): " + str(avg_price))


# Test 10
def avg_price_last_1_second_externally_time():
    """
    This observer checks the average price of all events in the last 1 second regarding externally time
    """
    avg_price = externally_last_time(col = 'time', seconds=1)['price'].mean()
    i10.critical(all_dfs['externally_last_time', 'time', 'seconds', 1].dataframe)
    i10.critical('The average price of all events in the last 1 second regarding externally time: ' + str(avg_price))


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
    i11.critical(df1, "\n")
    i11.critical(df2)
    i11.critical(prepare_for_correl(df2, ['A', 'B', 'C']))
    i11.critical(simple_correl('pearson', df, 2))
    i11.critical(correlwith(df1, df2, 'pearson', 0))
    i11.critical(correlwith(df1, df2, 'pearson', 1))


all_dfs['StockTick'] = DataframeManager()


# Test_12
def weighted_avg_price_last_five_events_observer():
    """
    This observer checks the average price of the last five events added
    """
    #avg_price = last_len(5)['price'].mean()
    avg = weighted_avg(last_len(5), field='price', weight='index')
    i12.critical(all_dfs['last_len', 5].dataframe)
    i12.critical('The average price of the last five events is: ' + str(avg))


# Test 13  ---test for expiry_exp
def sum_price_current_count_less_or_equal_4():

    sum = expiry_exp('current_count() <= 4')['price'].sum()
    i13.critical(all_dfs['expiry_exp', 'current_count() <= 4'].dataframe)
    i13.critical('sum price of the last four events is: ' + str(sum))


# Test 14  ---test for expiry_exp_batch
def sum_price_batch_counter_greater_or_equal_4():
    try:
        sum = expiry_exp_batch('batch_counter() >= 4')['price'].sum()
        i14.critical(all_dfs['expiry_exp_batch', 'batch_counter() >= 4'].dataframe)
        i14.critical('sum price of events in expiry_exp_batch("batch_counter() >= 4"): ' + str(sum))
    except:
        i14.critical('Dataframe is empty')


def test(i):
    if i == 1:
        f = logging.FileHandler("ceptest"+str(i)+".log", mode='w')
        i1.addHandler(f)
        s = logging.StreamHandler()
        i1.addHandler(s)
        all_dfs['StockTick'].observers.append(avg_price_last_five_events_observer)
    elif i == 2:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i2.addHandler(f)
        s = logging.StreamHandler()
        i2.addHandler(s)
        all_dfs['StockTick'].observers.append(avg_price_first_five_events_observer)
    elif i == 3:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i3.addHandler(f)
        s = logging.StreamHandler()
        i3.addHandler(s)
        all_dfs['StockTick'].observers.append(avg_price_last_1_second)
    elif i == 4:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i4.addHandler(f)
        s = logging.StreamHandler()
        i4.addHandler(s)
        all_dfs['StockTick'].observers.append(avg_price_first_two_seconds_observer)
    elif i == 5:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i5.addHandler(f)
        s = logging.StreamHandler()
        i5.addHandler(s)
        all_dfs['StockTick'].observers.append(avg_price_length_batch_5)
    elif i == 6:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i6.addHandler(f)
        s = logging.StreamHandler()
        i6.addHandler(s)
        all_dfs['StockTick'].observers.append(test_sort)
    elif i == 7:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i7.addHandler(f)
        s = logging.StreamHandler()
        i7.addHandler(s)
        all_dfs['StockTick'].observers.append(avg_price_first_unique_price_symbol)
    elif i == 8:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i8.addHandler(f)
        s = logging.StreamHandler()
        i8.addHandler(s)
        all_dfs['StockTick'].observers.append(avg_price_time_length_batch_15_1_second)
    elif i == 9:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i9.addHandler(f)
        s = logging.StreamHandler()
        i9.addHandler(s)
        all_dfs['StockTick'].observers.append(avg_price_last_unique_price_symbol)
    elif i == 10:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i10.addHandler(f)
        s = logging.StreamHandler()
        i10.addHandler(s)
        all_dfs['StockTick'].observers.append(avg_price_last_1_second_externally_time)
    elif i == 11:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i11.addHandler(f)
        s = logging.StreamHandler()
        i11.addHandler(s)
        correlation_method_test()
    elif i == 12:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i12.addHandler(f)
        s = logging.StreamHandler()
        i12.addHandler(s)
        all_dfs['StockTick'].observers.append(weighted_avg_price_last_five_events_observer)
    elif i == 13:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i13.addHandler(f)
        s = logging.StreamHandler()
        i13.addHandler(s)
        all_dfs['StockTick'].observers.append(sum_price_current_count_less_or_equal_4)
    elif i == 14:
        f = logging.FileHandler("ceptest" + str(i) + ".log", mode='w')
        i14.addHandler(f)
        s = logging.StreamHandler()
        i14.addHandler(s)
        all_dfs['StockTick'].observers.append(sum_price_batch_counter_greater_or_equal_4)

test(8)

#45min for 75000k (test(8))
now = datetime.now()
for i in range(75000):
    if i % 3 == 0:
        now = datetime.now()
    p = float(randrange(1, 10))
    l.critical('new price: ' + str(p))
    all_dfs['StockTick'].add({'index' : i+1, 'symbol' : 'A', 'price' : p, 'time' : now })
