from DataframeManager import *

def size(dfm):
    return dfm.dataframe.shape[0]


def last_event():
    return all_dfs["StockTick"].dataframe.tail(1)


def first_event():
    return all_dfs["StockTick"].dataframe.head(1)


def first_unique(*param):
    return all_dfs["StockTick"].dataframe.drop_duplicates(subset=param, keep='first', inplace=True)


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

