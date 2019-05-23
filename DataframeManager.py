import numpy as np
import pandas as pd


all_dfs = {}


class DataframeManager(object):
    def __init__(self, columns_list=[]):
        self.columns_list = columns_list + ['INSERTION_TIMESTAMP']
        self.dataframe = pd.DataFrame(data={col_name: [] for col_name in self.columns_list})
        self.dataframe = pd.DataFrame()
        self.observers = {}

    def add_df(self, row):
        add = True
        for function, param in self.observers.items():
            add = add and function(*param)
        if add:
            self.dataframe = self.dataframe.append(row)

    def add(self, event_dictionary):
        event_dictionary['INSERTION_TIMESTAMP'] = np.datetime64('now')
        row = pd.DataFrame({key: [value] for key, value in event_dictionary.items()})
        self.dataframe = self.dataframe.append(row)
        self.add_df(row)