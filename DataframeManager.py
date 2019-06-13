import pandas as pd
from datetime import datetime


all_dfs = {}


class DataframeManager(object):
    def __init__(self, columns_list=[]):
        self.dataframe = pd.DataFrame(columns=columns_list)
        self.observers = []
        self.variables = {}

    def update_df(self, *param):
        for function in self.observers:
            function(*param)

    def add(self, event_dictionary):
        event_dictionary['INSERTION_TIMESTAMP'] = datetime.now()
        row = pd.DataFrame({key: [value] for key, value in event_dictionary.items()})
        self.dataframe = self.dataframe.append(row)
        self.update_df()