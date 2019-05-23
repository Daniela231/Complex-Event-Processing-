import numpy as np
import pandas as pd


all_dfs = {}


class DataframeManager(object):
    def __init__(self, columns_list=[]):
        #self.columns_list = columns_list + ['INSERTION_TIMESTAMP']
        #self.dataframe = pd.DataFrame(data={col_name: [] for col_name in self.columns_list})
        self.dataframe = pd.DataFrame()
        self.observers = {}

    def add_df(self, new_dataframe):
        self.dataframe = self.dataframe.append(new_dataframe)
        for function, param in self.observers.items():
            function(*param)

    def add(self, event_dictionary):
        event_dictionary['INSERTION_TIMESTAMP'] = np.datetime64('now')
        new_dataframe = pd.DataFrame({key: [value] for key, value in event_dictionary.items()})
        self.add_df(new_dataframe)
