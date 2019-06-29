from random import randrange, shuffle
from DataframeManager import *
from length_window import *
from time_window import *
from data_window import *
from special_derived_value_window import *
from statistic_views import *
from general import *
from datetime import datetime
from LoggerSetters import *
import multiprocessing as mp

# Logger for the new price
main_logger = logging.getLogger("cepgenerator")
set_logger_handler(main_logger, "generator")

# Block for creating all the different loggers for the tests
i1 = logging.getLogger("test" + str(1))
i2 = logging.getLogger("test" + str(2))
i3 = logging.getLogger("test" + str(3))
i4 = logging.getLogger("test" + str(4))
i5 = logging.getLogger("test" + str(5))
i6 = logging.getLogger("test" + str(6))
i7 = logging.getLogger("test" + str(7))
i8 = logging.getLogger("test" + str(8))
i9 = logging.getLogger("test" + str(9))
i10 = logging.getLogger("test" + str(10))
i11 = logging.getLogger("test" + str(11))
i12 = logging.getLogger("test" + str(12))
i13 = logging.getLogger("test" + str(13))
i14 = logging.getLogger("test" + str(14))
i15 = logging.getLogger("test" + str(15))
i16 = logging.getLogger("test" + str(16))
i17 = logging.getLogger("test" + str(17))
i18 = logging.getLogger("test" + str(18))
i19 = logging.getLogger("test" + str(19))
i20 = logging.getLogger("test" + str(20))
i21 = logging.getLogger("test" + str(21))
i22 = logging.getLogger("test" + str(22))
i23 = logging.getLogger("test" + str(23))
i24 = logging.getLogger("test" + str(24))
i25 = logging.getLogger("test" + str(25))
i26 = logging.getLogger("test" + str(26))
i27 = logging.getLogger("test" + str(27))
i28 = logging.getLogger("test" + str(28))
i29 = logging.getLogger("test" + str(29))
i30 = logging.getLogger("test" + str(30))
i31 = logging.getLogger("test" + str(31))
i32 = logging.getLogger("test" + str(32))
i33 = logging.getLogger("test" + str(33))


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
    df = last_time(seconds=1)
    avg_price = df['price'].mean()
    i3.critical(df)
    i3.critical('The average price of all events in the last 1 second: ' + str(avg_price))


# Test 4
def avg_price_first_two_seconds_observer():
    """
    This observer checks the average price of all events added in the first two seconds after first_time(seconds=2) is
    called for the first time
    """
    df = first_time(seconds=2)
    avg_price = df['price'].mean()
    i4.critical(df)
    i4.critical('The average price of all events in the first two seconds after statement_start is: ' + str(avg_price))


# Test 5
def avg_price_length_batch_5():
    """
    This observer checks the average price of the events in the length_batch(5) dataframe
    """
    avg_price = length_batch(5)['price'].mean()
    i5.critical(all_dfs['length_batch', 5].dataframe)
    i5.critical('The average price of all events in length_batch(5): ' + str(avg_price))

# Test 6
def test_sort():
    """
    This observer checks the average price of the events in the dataframe sort(size=5, criteria=[('price', True),
    ('index', False)]
    """
    df = sort(size=5, criteria=[('price', True), ('index', False)])
    avg_price = df['price'].mean()
    i6.critical(df)
    i6.critical("The average price of all events in sort(size=5, criteria=[('price', True), ('index', False)]: "
                + str(avg_price))


# Test 7
def avg_price_first_unique_price_symbol():
    """
    This observer checks the average price of the events in first_unique('price', 'symbol') dataframe
    """
    df = first_unique('price', 'symbol')
    avg_price = df['price'].mean()
    i7.critical(df)
    i7.critical("The average price of all events in first_unique('price', 'symbol'): " + str(avg_price))


# Test 8
def avg_price_time_length_batch_15_1_second():
    """
    This observer checks the average price of the events in the time_length_batch(15, seconds=1) dataframe
    """
    avg_price = time_length_batch(15, seconds=1)['price'].mean()
    i8.critical(all_dfs['time_length_batch', 15, 'seconds', 1].dataframe)
    i8.critical('The average price of all events in time_length_batch(15, seconds=1): ' + str(avg_price))


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
    This observer checks the average price of all events where the value of the time column "time" is greater than
    (current system time - 1 second).
    """
    i10.critical('now: '+str(datetime.now()))
    avg_price = externally_last_time(col='time', seconds=1)['price'].mean()
    i10.critical(all_dfs['externally_last_time', 'time', 'seconds', 1].dataframe)
    i10.critical('The average price of all events where the value of the time column "time" is > (now - 1 sec): ' + str(avg_price))


# Test 11 /given dataframe test
def correlation_method_test():
    """
    This method tests the correlation functions on the pre defined dataframes to be able to counter
    check if the returned values are the expected values of the calculation
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
    This observer checks the weighted average price of the last five events added (with 'index' as weight)
    """
    avg = weighted_avg(last_len(5), field='price', weight='index')
    i12.critical(all_dfs['last_len', 5].dataframe)
    i12.critical('The average price of the last five events is: ' + str(avg))


# Test 13  --- first test for expiry_exp [current_count]
def sum_price_current_count_less_or_equal_4():
    """
    This observer checks the sum price of the last four events added.
    """
    df = expiry_exp('current_count() <= 4')
    sum = df['price'].sum()
    i13.critical(df)
    i13.critical('sum price of the last four events is: ' + str(sum))


# Test 14  ---first test for expiry_exp_batch [batch_counter]
def sum_price_batch_counter_greater_or_equal_4():
    """
    This observer checks the sum price of events in the dataframe that accumulates events until ‘batch_counter’ is
    greater or equal to 4.
    """
    df = expiry_exp_batch('batch_counter() >= 4')
    sum = df['price'].sum()
    i14.critical(df)
    i14.critical('sum price of events in expiry_exp_batch("batch_counter() >= 4"): ' + str(sum))


# test for time_batch
def test_time_batch_observer():
    a = time_batch(milliseconds=2)
    main_logger.critical(all_dfs['time_batch', 'milliseconds', 2].dataframe)
    return False


# Test 15  --- second test for expiry_exp
def sum_price_last_two_seconds_expiry_exp():
    """
    This observer checks the sum price in last 2 seconds
    """
    df = expiry_exp('oldest_timestamp() > newest_timestamp() - timedelta(seconds = 2)')
    sum = df['price'].sum()
    i15.critical(df)
    i15.critical('sum price in last 2 seconds: ' + str(sum))


# Test 16  ---third test for expiry_exp
def sum_price_less_20_expiry_exp():
    """
    This observer checks the sum price in the dataframe that removes the oldest events (from the same dataframe) when a
    new event is added until the total price of all events is less than 20.
    """
    df = expiry_exp('all_dfs[key].dataframe["price"].sum() < 20')
    sum = df['price'].sum()
    i16.critical(df)
    i16.critical('sum price: ' + str(sum))


# Test 17  ---4th test for expiry_exp
def same_price_expiry_exp():
    """
    This observer checks the sum price in the dataframe that retains the last consecutive events having the same price.
    When the price value changes, the dataframe expires all events with the old price and retains only the last event.
    """
    df = expiry_exp('newest_event()["price"].iloc[0] == oldest_event()["price"].iloc[0]')
    sum = df['price'].sum()
    i17.critical(df)
    i17.critical('sum price: ' + str(sum))


# Test 18  ---2nd test for expiry_exp_batch
def batch_price_9_expiry_exp_batch():
    """
    This observer checks the sum price of events in the dataframe that accumulates events until an event arrives that
    has a price of 9.

    """
    df = expiry_exp_batch('newest_event()["price"].iloc[0] == 9')
    sum = df['price'].sum()
    i18.critical(df)
    i18.critical('sum price: ' + str(sum))


# Test 19 --- 3rd test for expiry_exp_batch
def batch_sum_price_greater_100_expiry_exp_batch():
    """
    This observer checks the sum price of events in the dataframe that  accumulates events until the total price of all
    events in the dataframe is greater than 100.
    """
    df = expiry_exp_batch('last_event(batch_counter())["price"].sum() > 100')
    sum = df['price'].sum()
    i19.critical(df)
    i19.critical('sum price: ' + str(sum))


# Test 20 --- 4th test for expiry_exp_batch
def same_price_expiry_exp_batch():
    """
    This observer checks the sum price of events in the dataframe that  batches all events that have the same price.
    When the price changes, the dataframe releases the batch of events collected for the old flag value.
    """
    df = expiry_exp_batch('newest_event()["price"].iloc[0] != triggering_event()["price"].iloc[0]',
                                  include_triggering_event=False)
    sum = df['price'].sum()
    i20.critical(df)
    i20.critical('sum price: ' + str(sum))

# Test 21 --- test for time_to_live
def test_for_time_to_live():
    """
    This observer checks the sum price of events in the dataframe that contains all events where the value of the time
    column 'time' is greater than the current system time.
    """
    i21.critical('now: '+str(datetime.now()))
    df = time_to_live(col='time')
    sum = df['price'].sum()
    i21.critical(df)
    i21.critical('sum price: ' + str(sum))


# Tests for general.py [Tests 22 - 33]
n = 200
now = datetime.now()
time = []
for i in range(n):
    time.append(now - timedelta(seconds=n - i))
shuffle(time)
df_for_general_tests = pd.DataFrame({
    'index': range(n),
    'price': np.random.choice(range(1, 10), n, replace=True),
    'symbol': np.random.choice(['A', 'B'], n, replace=True),
    'time': time
})


# Test 22
def test_general_df_last_length():
    """
    Test for general.df_last_length
    :return: None
    """
    i22.critical(df_for_general_tests)
    i22.critical(df_last_length(df_for_general_tests, col='symbol', len=5))


# Test 23
def test_general_df_first_length():
    """
    Test for general.df_first_length
    :return: None
    """
    i23.critical(df_for_general_tests)
    i23.critical(df_first_length(df_for_general_tests, col='symbol', len=5))


# Test 24
def test_general_df_last_time():
    """
    Test for general.df_last_time
    :return: None
    """
    i24.critical(df_for_general_tests)
    now = datetime.now()
    i24.critical('now: ' + str(now))
    i24.critical(df_last_time(df_for_general_tests, col='time', start_point=now, seconds=5))


# Test 25
def test_general_df_first_time():
    """
    Test for general.df_first_time
    :return: None
    """
    i25.critical(df_for_general_tests)
    start = datetime.now() - timedelta(seconds=n/2)
    i25.critical('start: ' + str(start))
    i25.critical(df_first_time(df_for_general_tests, col='time', start_point=start, seconds=5))


# Test 26
def test_general_df_last_unique():
    """
    Test for general.df_last_unique
    :return: None
    """
    i26.critical(df_for_general_tests)
    i26.critical(df_last_unique(df_for_general_tests, 'price', 'symbol'))


# Test 27
def test_general_df_first_unique():
    """
    Test for general.df_first_unique
    :return: None
    """
    i27.critical(df_for_general_tests)
    i27.critical(df_first_unique(df_for_general_tests, 'price', 'symbol'))


# Test 28
def test_general_df_order_last_time():
    """
    Test for general.df_order_last_time
    :return: None
    """
    i28.critical(df_for_general_tests)
    now = datetime.now()
    i28.critical('now: ' + str(now))
    i28.critical(df_order_last_time(df_for_general_tests, col='time', start_point=now, seconds=n/4))


# Test 29
def test_general_df_order_first_time():
    """
    Test for general.df_order_first_time
    :return: None
    """
    i29.critical(df_for_general_tests)
    start = datetime.now() - timedelta(seconds=n/2)
    i29.critical('now: ' + str(start))
    i29.critical(df_order_first_time(df_for_general_tests, col='time', start_point=start, seconds=n/4))


# Test 30
def test_general_df_last_n_events():
    """
    Test for general.df_last_n_events
    :return: None
    """
    i30.critical(df_for_general_tests)
    i30.critical(df_last_n_events(df_for_general_tests, 5))


# Test 31
def test_general_df_first_n_events():
    """
    Test for general.df_first_n_events
    :return: None
    """
    i31.critical(df_for_general_tests)
    i31.critical(df_first_n_events(df_for_general_tests, 5))


# Test 32
def test_general_df_sort():
    """
    Test for general.df_sort
    :return:
    """
    i32.critical(df_for_general_tests)
    i32.critical(df_sort(df_for_general_tests, criteria=[('symbol', False), ('price', True)], size=15))


# Test 33
def test_general_df_time_to_live():
    """
    Test for general.df_time_to_live
    :return:
    """
    start_time = datetime.now() - timedelta(seconds=n/2)
    i33.critical(df_for_general_tests)
    i33.critical('start time: ' + str(start_time))
    i33.critical(df_time_to_live(df_for_general_tests, col='time', time=start_time))


def test(i):
    if i == 1:
        set_logger_rotating_handler(i1, str(i))
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
        all_dfs['StockTick'].observers.append(batch_sum_price_greater_100_expiry_exp_batch)
    elif i == 20:
        set_logger_handler(i20, str(i))
        all_dfs['StockTick'].observers.append(same_price_expiry_exp_batch)
    elif i == 21:
        set_logger_handler(i21, str(i))
        all_dfs['StockTick'].observers.append(test_for_time_to_live)
    elif i == 22:
        set_logger_handler(i22, str(i))
        test_general_df_last_length()
    elif i == 23:
        set_logger_handler(i23, str(i))
        test_general_df_first_length()
    elif i == 24:
        set_logger_handler(i24, str(i))
        test_general_df_last_time()
    elif i == 25:
        set_logger_handler(i25, str(i))
        test_general_df_first_time()
    elif i == 26:
        set_logger_handler(i26, str(i))
        test_general_df_last_unique()
    elif i == 27:
        set_logger_handler(i27, str(i))
        test_general_df_first_unique()
    elif i == 28:
        set_logger_handler(i28, str(i))
        test_general_df_order_last_time()
    elif i == 29:
        set_logger_handler(i29, str(i))
        test_general_df_order_first_time()
    elif i == 30:
        set_logger_handler(i30, str(i))
        test_general_df_last_n_events()
    elif i == 31:
        set_logger_handler(i31, str(i))
        test_general_df_first_n_events()
    elif i == 32:
        set_logger_handler(i32, str(i))
        test_general_df_sort()
    elif i == 33:
        set_logger_handler(i33, str(i))
        test_general_df_time_to_live()


tests = [7]
threads = []

for i in tests:
    p = mp.Process(target=test(i))
    threads.append(p)
    p.start()

for proc in threads:
    proc.join()

n = 20

for t in tests:
    if t>21 and t<34:
        continue
    for i in range(n):
        time = datetime.now() + timedelta(seconds=1)
        p = float(randrange(1, 10))
        main_logger.critical('new price: ' + str(p))
        all_dfs['StockTick'].add({'index': i, 'symbol': 'A', 'price': p, 'time': time})
    break
