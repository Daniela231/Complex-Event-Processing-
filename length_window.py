from DataframeManager import *
from special_derived_value_window import *


def last_event():
    """
    Returns a dataframe consisting of the last event added
    :return: a dataframe consisting of the last event added
    """
    return all_dfs["StockTick"].dataframe.tail(1)


def first_event():
    """
    Returns a dataframe consisting of the first event added
    :return: a dataframe consisting of first event
    """
    return all_dfs["StockTick"].dataframe.head(1)


def first_unique_observer(key, param):
    """
    Observer for the first_unique dataframe
    :param key: key for the first_unique dataframe
    :param param: parameters we want to look for first unique datas
    :return: None
    """
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())
    all_dfs[key].dataframe.drop_duplicates(subset=set(param), keep='first', inplace=True)


def first_unique(*param):
    """
    Retains only the first events having the same expression in the columns of param
    :param param: defines the columns we want to filter for unique parameters
    :return: returns the filtered dataframe
    """
    key = ('first_unique',)
    for elm in param:
        key = key + (elm,)
    try:
        all_dfs[key].update_df(key, param)
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.drop_duplicates(
            subset=set(param), keep='first')
        all_dfs[key].observers.append(first_unique_observer)

    return all_dfs[key].dataframe


def last_unique_observer(key, param):
    """
    Observer for the first_unique dataframe
    :param key: key for the first_unique dataframe
    :param param: parameters we want to look for first unique datas
    :return: None
    """
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())
    all_dfs[key].dataframe.drop_duplicates(subset=set(param), keep='last', inplace=True)


def last_unique(*param):
    """
    Retains only the first events having the same expression in the columns of param
    :param param: defines the columns we want to filter for unique parameters
    :return: returns the filtered dataframe
    """
    key = ('last_unique',)
    for elm in param:
        key = key + (elm,)
    try:
        all_dfs[key].update_df(key, param)
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.drop_duplicates(
            subset=set(param), keep='last')
        all_dfs[key].observers.append(last_unique_observer)

    return all_dfs[key].dataframe


def sort_observer(key, size ,criteria):
    """
    Observer for the sort dataframe
    :param key: key for the sort dataframe
    :param size: length of the dataframe
    :param criteria: list of tupels (col True) sorting the col column ascending or (col False) sorting descending
    :return: None
    """
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event()).sort_values(
        by=[elm[0] for elm in criteria], ascending=[elm[1] for elm in criteria]).head(size)


def sort(size, criteria):
    """
    This function always returns the first size sorted elements of our dataframe sorted by the given criteria
    :param size: size of dataframe in int
    :param criteria: list of tupels (price True) sorting the price column ascending or (price False) sorting descending
    :return: Sorted dataframe
    """
    key = ('sort', size)
    for elm, symbol in criteria:
        key = key + (elm, symbol)

    try:
        all_dfs[key].update_df(key, size ,criteria)
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.sort_values(
            by=[elm[0] for elm in criteria], ascending=[elm[1] for elm in criteria]).head(size)
        all_dfs[key].observers.append(sort_observer)


    return all_dfs[key].dataframe


def last_length_observer(key, len):
    """
    Observer for last_length dataframe
    :param key: key for last length dataframe
    :param len: length of the last length dataframe
    :return: None
    """
    if size(all_dfs[key]) >= len:
        all_dfs[key].dataframe = all_dfs[key].dataframe.iloc[1:]
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


def last_len(len):
    """
    Returns the dataframe with the last len elements
    :param len: length of dataframe we want to return
    :return: Dataframe of length len
    """
    key = ('last_len', len)

    try:
        all_dfs[key].update_df(key, len)
    except:
        if key not in all_dfs.keys():
            all_dfs[key] = DataframeManager()
            all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.tail(len)
            all_dfs[key].observers.append(last_length_observer)

    return all_dfs[key].dataframe


def length_batch_observer(key, len):
    """
    Observer for the length_batch dataframe
    :param key: key of length_batch dataframe
    :param len: length of batch
    :return: None
    """
    if all_dfs[key].variables['count'] % len == 0:
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.tail(len)


def length_batch(len):
    """
    Returns the dataframe when the given amount of len elements has been added
    :param len: number of rows in our dataframe batch
    :return: dataframe of length len after collecting len elemements
    """
    key = ('length_batch', len)

    try:
        all_dfs[key].variables['count'] = all_dfs[key].variables['count'] + 1
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].observers.append(length_batch_observer)
        all_dfs[key].variables['count'] = 1

    all_dfs[key].update_df(key, len)
    return all_dfs[key].dataframe


def first_length_observer(key, len):
    """
    observer for the first_length dataframe
    :param key: key of the first_length dataframe
    :param len: length of the dataframe
    :return: None
    """
    if size(all_dfs[key]) < len:
        all_dfs[key].dataframe = all_dfs[key].dataframe.append(last_event())


def first_len(len):
    """
    Returns the dataframe that contains the first_len elements
    :param len: length of dataframe
    :return: first_length dataframe
    """
    key = ('first_len', len)

    try:
        all_dfs[key].update_df(key, len)
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.head(len)
        all_dfs[key].observers.append(first_length_observer)

    return all_dfs[key].dataframe

