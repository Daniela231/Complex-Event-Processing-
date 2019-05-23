from DataframeManager import *


def last_event():
    return all_dfs["StockTick"].dataframe.tail(1)


def last_length_observer(key, len):
    if all_dfs[key].dataframe.shape[0] > len:
        all_dfs[key].dataframe = all_dfs[key].dataframe.iloc[1:]


def last_len(len):
    key = 'last_len_'+str(len)
    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.tail(len)
        all_dfs[key].observers.update({last_length_observer : [key, len]})
    else:
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe


def first_length_observer(key, len):
    if all_dfs[key].dataframe.shape[0] > len:
        all_dfs[key].dataframe = all_dfs[key].dataframe.iloc[:len]


def first_len(len):
    key = 'first_len_'+str(len)
    if key not in all_dfs.keys():
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = all_dfs["StockTick"].dataframe.head(len)
        all_dfs[key].observers.update({first_length_observer : [key, len]})
    else:
        all_dfs[key].add_df(last_event())

    return all_dfs[key].dataframe
