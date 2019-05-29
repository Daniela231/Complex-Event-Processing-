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


def sort(size, criteria):
    count = 1
    while count<size:
        all_dfs["StockTick"].dataframe.sort_values(by=criteria, ascending=1)
        count += 1


def last_length_observer(key, len):
    if size(all_dfs[key]) >= len:
        all_dfs[key].dataframe = all_dfs[key].dataframe.iloc[1:]
    return True


def last_len(len):
    key = 'last_len_'+str(len)

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.tail(len)
        all_dfs[key].observers.update({last_length_observer : [key, len]})
    else:
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe


def length_batch_observer(key, len):
    if all_dfs[key].variables['count'] % len == 0:
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.tail(len)
    return False


def length_batch(len):
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
    return size(all_dfs[key]) < len


def first_len(len):
    key = 'first_len_'+str(len)

    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.head(len)
        all_dfs[key].observers.update({first_length_observer : [key, len]})
    else:
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe
