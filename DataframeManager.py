import numpy as np
import pandas as pd


all_dfs = {}


class DataframeManager(object):
    def __init__(self, columns_list=[]):
        self.dataframe = pd.DataFrame()
        self.observers = {}
        self.variables = {}

    def update_df(self):
        for function, param in self.observers.items():
            function(*param)

    def add(self, event_dictionary):
        event_dictionary['INSERTION_TIMESTAMP'] = np.datetime64('now')
        row = pd.DataFrame({key: [value] for key, value in event_dictionary.items()})
        self.dataframe = self.dataframe.append(row)
        self.update_df()