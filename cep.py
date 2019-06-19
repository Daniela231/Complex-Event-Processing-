from random import randrange
from DataframeManager import *
from length_window import *
from time_window import *
from data_window import *
from special_derived_value_window import *
from statistic_views import *
from datetime import datetime
from LoggerSetters import *
from threading import Thread

# Logger for the new price
l = logging.getLogger("cepgenerator")
set_logger_handler(l, "generator")


# Block for creating all the different loggers for the tests
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
i15 = logging.getLogger("test"+str(15))
i16 = logging.getLogger("test"+str(16))
i17 = logging.getLogger("test"+str(17))
i18 = logging.getLogger("test"+str(18))
i19 = logging.getLogger("test"+str(19))
i20 = logging.getLogger("test"+str(20))


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
    This observer checks the average price of all events added in the first two seconds after first_time(seconds=2) is
    called for the first time
    """
    df = first_time(seconds=2)
    avg_price = df['price'].mean()
    i4.critical(df)
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


# Test 11 /given dataframe test
def correlation_method_test():
    """
    This method tests the correlation functions on the pre defined dataframes to be able to counter check if the returned
    values are the expected values of the calculation
    """
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
    i11.critical(df1)
    i11.critical(df2)
    i11.critical(prepare_for_correl(df2, ['A', 'B', 'C']))
    i11.critical(simple_correl('pearson', df, 2))
    i11.critical(correlwith(df1, df2, 'pearson', 0))
    i11.critical(correlwith(df1, df2, 'pearson', 1))


# Test_12
def weighted_avg_price_last_five_events_observer():
    """
    This observer checks the average price of the last five events added
    """
    # avg_price = last_len(5)['price'].mean()
    avg = weighted_avg(last_len(5), field='price', weight='index')
    i12.critical(all_dfs['last_len', 5].dataframe)
    i12.critical('The average price of the last five events is: ' + str(avg))


# Test 13  --- first test for expiry_exp [current_count]
def sum_price_current_count_less_or_equal_4():
    sum = expiry_exp('current_count() <= 4')['price'].sum()
    i13.critical(all_dfs['expiry_exp', 'current_count() <= 4'].dataframe)
    i13.critical('sum price of the last four events is: ' + str(sum))


# Test 14  ---first test for expiry_exp_batch [batch_counter]
def sum_price_batch_counter_greater_or_equal_4():
    try:
        sum = expiry_exp_batch('batch_counter() >= 4')['price'].sum()
        i14.critical(all_dfs['expiry_exp_batch', 'batch_counter() >= 4'].dataframe)
        i14.critical('sum price of events in expiry_exp_batch("batch_counter() >= 4"): ' + str(sum))
    except:
        i14.critical('Dataframe is empty')


# Test 15 ---test for time_batch
def test_time_batch_observer():
    a = time_batch(nanoseconds=2)
    l.critical(all_dfs['time_batch', 'nanoseconds', 2].dataframe)
    return False


# Test 15  --- second test for expiry_exp
def sum_price_last_two_seconds_expiry_exp():
    """

    """
    sum = expiry_exp('oldest_timestamp() > newest_timestamp() - timedelta(seconds = 2)')['price'].sum()
    i15.critical(all_dfs['expiry_exp', 'oldest_timestamp() > newest_timestamp() - timedelta(seconds = 2)'].dataframe)
    i15.critical('sum price in last 2 seconds: ' + str(sum))


# Test 16  ---third test for expiry_exp
def sum_price_less_20_expiry_exp():
    """

    """
    sum = expiry_exp('all_dfs[key].dataframe["price"].sum() < 20')['price'].sum()
    i16.critical(all_dfs['expiry_exp', 'all_dfs[key].dataframe["price"].sum() < 20'].dataframe)
    i16.critical('sum price: ' + str(sum))


# Test 17  ---4th test for expiry_exp
def same_price_expiry_exp():
    """
    This example retains the last consecutive events having the same price. When the price value changes, the data window expires all
    events with the old price and retains only the last event.
    """
    i17.critical(expiry_exp('newest_event()["price"].iloc[0] == oldest_event()["price"].iloc[0]'))


# Test 18  ---2nd test for expiry_exp_batch
def batch_price_9_expiry_exp_batch():
    """
    This example accumulates events until an event arrives that has a price of 9
    """
    i18.critical(expiry_exp_batch('newest_event()["price"].iloc[0] == 9'))


# Test 19 --- 3rd test for expiry_exp_batch
def batch_sum_price_greather_100_expiry_exp_batch():
    """
    This example accumulates events until the total price of all events in the dataframe is > 100:
    """
    df = expiry_exp_batch('last_event(batch_counter())["price"].sum() > 100')
    sum = df['price'].sum()
    i19.critical(df)
    i19.critical('sum price: ' + str(sum))


# Test 20 --- 4th test for expiry_exp_batch
def same_price_expiry_exp_batch():
    """
    This example batches all events that have the same price. When the price changes, the dataframe releases the batch
    of events collected for the old flag value.
    """
    i20.critical(expiry_exp_batch('newest_event()["price"].iloc[0] != triggering_event()["price"].iloc[0]',
                                  include_triggering_event=False))


def test(i):
    if i == 1:
        set_logger_handler(i1, str(i))
        all_dfs['StockTick'].observers.append(avg_price_last_five_events_observer)
    elif i == 2:
        set_logger_handler(i2, str(i))
        all_dfs['StockTick'].observers.append(avg_price_first_five_events_observer)
    elif i == 3:
        set_logger_handler(i3, str(i))
        all_dfs['StockTick'].observers.append(avg_price_last_1_second)
    elif i == 4:
        set_logger_handler(i4, str(i))
        all_dfs['StockTick'].observers.append(avg_price_first_two_seconds_observer)
    elif i == 5:
        set_logger_handler(i5, str(i))
        all_dfs['StockTick'].observers.append(avg_price_length_batch_5)
    elif i == 6:
        set_logger_handler(i6, str(i))
        all_dfs['StockTick'].observers.append(test_sort)
    elif i == 7:
        set_logger_handler(i7, str(i))
        all_dfs['StockTick'].observers.append(avg_price_first_unique_price_symbol)
    elif i == 8:
        set_logger_handler(i8, str(i))
        all_dfs['StockTick'].observers.append(avg_price_time_length_batch_15_1_second)
    elif i == 9:
        set_logger_handler(i9, str(i))
        all_dfs['StockTick'].observers.append(avg_price_last_unique_price_symbol)
    elif i == 10:
        set_logger_handler(i10, str(i))
        all_dfs['StockTick'].observers.append(avg_price_last_1_second_externally_time)
    elif i == 11:
        set_logger_handler(i11, str(i))
        correlation_method_test()
    elif i == 12:
        set_logger_handler(i12, str(i))
        all_dfs['StockTick'].observers.append(weighted_avg_price_last_five_events_observer)
    elif i == 13:
        set_logger_handler(i13, str(i))
        all_dfs['StockTick'].observers.append(sum_price_current_count_less_or_equal_4)
    elif i == 14:
        set_logger_handler(i14, str(i))
        all_dfs['StockTick'].observers.append(sum_price_batch_counter_greater_or_equal_4)
    elif i == 15:
        set_logger_handler(i15, str(i))
        all_dfs['StockTick'].observers.append(sum_price_last_two_seconds_expiry_exp)
    elif i == 16:
        set_logger_handler(i16, str(i))
        all_dfs['StockTick'].observers.append(sum_price_less_20_expiry_exp)
    elif i == 17:
        set_logger_handler(i17, str(i))
        all_dfs['StockTick'].observers.append(same_price_expiry_exp)
    elif i == 18:
        set_logger_handler(i18, str(i))
        all_dfs['StockTick'].observers.append(batch_price_9_expiry_exp_batch)
    elif i == 19:
        set_logger_handler(i19, str(i))
        all_dfs['StockTick'].observers.append(batch_sum_price_greather_100_expiry_exp_batch)
    elif i == 20:
        set_logger_handler(i20, str(i))
        all_dfs['StockTick'].observers.append(same_price_expiry_exp_batch)


tests = [1, 11, 8]
threads = []

for i in tests:
    process = Thread(target=test, args=[i])
    process.start()
    threads.append(process)


for process in threads:
    process.join()


# 45min for 75000k (test(8))
now = datetime.now()
for i in range(40):
    if i % 3 == 0:
        now = datetime.now()
    p = float(randrange(1, 10))
    l.critical('new price: ' + str(p))
    all_dfs['StockTick'].add({'index' : i, 'symbol' : 'A', 'price' : p, 'time' : now })
