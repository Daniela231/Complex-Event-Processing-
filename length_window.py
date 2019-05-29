from DataframeManager import *


def size(dfm):
    """
    This function returns the number of rows
    :param dfm: Dataframe we want to know numbers of rows of
    :return: long type value of numbers of rows
    """
    return dfm.dataframe.shape[0]


def last_event():
    """
    Returns the last event added to the dataframe
    :return: last row of dataframe containing the last event added
    """
    return all_dfs["StockTick"].dataframe.tail(1)


def first_event():
    """
    Returns the first event of the Dataframe
    :return: First row of the Dataframe, containing first event
    """
    return all_dfs["StockTick"].dataframe.head(1)


def first_unique(*param):
    """
    Retains online the first events having the same expression in the columns of param
    :param param: defines the columns we want to filter for unique parameters
    :return: returns the filtered dataframe
    """
    return all_dfs["StockTick"].dataframe.drop_duplicates(subset=param, keep='first', inplace=True)


def sort_observer(key, size ,criteria):
    """
    Observer for the sort dataframe
    :param key: key for the sort dataframe
    :param size: length of the dataframe
    :param criteria: list of tupels (price True) sorting the price column ascending or (price False) sorting descending
    :return: False because we don't want to add the last event (was already added)
    """
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event()).sort_values(by=[elm[0] for elm in criteria], ascending=[elm[1] for elm in criteria]).head(size)
    return False


def sort(size, criteria):
    """
    This function always returns the first size sorted elements of our dataframe sorted by the given criteria
    :param size: size of dataframe in int
    :param criteria: list of tupels (price True) sorting the price column ascending or (price False) sorting descending
    :return: Sorted dataframe
    """
    key = 'sort_'+str(size)
    for elm, symbol in criteria:
        key = key +'_'+str(elm)+'_'+str(symbol)
    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.sort_values(by=[elm[0] for elm in criteria], ascending=[elm[1] for elm in criteria]).head(size)
        all_dfs[key].observers.update({sort_observer : [key, size ,criteria]})
    else:
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe


def last_length_observer(key, len):
    """
    Observer for last_length dataframe
    :param key: key for last length dataframe
    :param len: length
    :return: return True because we have to add the last event
    """
    if size(all_dfs[key]) >= len:
        all_dfs[key].dataframe = all_dfs[key].dataframe.iloc[1:]
    return True


def last_len(len):
    """
    function that returns the dtaframe with the last len elements
    :param len: length of dataframe we want to return
    :return: Dataframe of length len
    """
    key = 'last_len_'+str(len)

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.tail(len)
        all_dfs[key].observers.update({last_length_observer : [key, len]})
    else:
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe


def length_batch_observer(key, len):
    """
    Observer for the length_batch dataframe
    :param key: key of length_batch dataframe
    :param len: length of batch
    :return: false because we don't want to add the last element because it was already added
    """
    if all_dfs[key].variables['count'] % len == 0:
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.tail(len)
    return False


def length_batch(len):
    """
    Returns the dataframe when the given amount of len elements has been added
    :param len: number of rows in our dataframe batch
    :return: dataframe of length len after collecting len elemements
    """
    key = 'length_batch_'+ str(len)

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = pd.DataFrame()
        all_dfs[key].observers.update({length_batch_observer : [key, len]})

    if 'count' not in all_dfs[key].variables.keys():
        all_dfs[key].variables['count'] = 1
    else:
        all_dfs[key].variables['count'] = all_dfs[key].variables['count'] + 1

    all_dfs[key].add_df()
    return all_dfs[key].dataframe


def first_length_observer(key, len):
    """
    obeser for the first_length dataframe
    :param key: key of the first_length dataframe
    :param len: length of the dataframe
    :return: True if have to add last event, else False
    """
    return size(all_dfs[key]) < len


def first_len(len):
    """
    Returns the dataframe that contains the first_len elements
    :param len: length of dataframe
    :return: first_length dataframe
    """
    key = 'first_len_'+str(len)

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.head(len)
        all_dfs[key].observers.update({first_length_observer : [key, len]})
    else:
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe
