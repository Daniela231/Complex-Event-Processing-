import pandas as pd
from datetime import datetime


class DataframeManager(object):
    def __init__(self, columns_list=[]):
        self.dataframe = pd.DataFrame(columns=columns_list)
        self.observers = []
        self.variables = {}

    def update_df(self, *param):
        for function in self.observers:
            try:
                function(*param)
            except:
                function[0](function[1])

    def add(self, event_dictionary):
        event_dictionary['INSERTION_TIMESTAMP'] = datetime.now()
        row = pd.DataFrame(event_dictionary, index=[0])
        self.dataframe = self.dataframe.append(row, ignore_index=True)
        self.update_df()


all_dfs = {'StockTick': DataframeManager()}