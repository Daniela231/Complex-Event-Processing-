from random import randrange

import re as regex
import numpy as np
import pandas as pd

all_dataframes_in_memory = {}


class DataframeManager(object):
    def __init__(self, columns_list):
        self.columns_list = columns_list + ["INSERTION_TIMESTAMP"]
        self.dataframe = pd.DataFrame(data={col_name: [] for col_name in self.columns_list})
        self.observers = []

    def add(self, event_dictionary):
        event_dictionary["INSERTION_TIMESTAMP"] = np.datetime64('now')
        new_dataframe = pd.DataFrame(data={col_name: [event_dictionary[col_name]] for col_name in self.columns_list})
        self.dataframe = self.dataframe.append(new_dataframe)
        for observer in self.observers:
            observer()

    def add_observer(self, observer):
        self.observers.append(observer)


def avg_price_last_two_events_observer():
    if len(all_dataframes_in_memory["StockTick"].dataframe) >= 2:
        considered_df = all_dataframes_in_memory["StockTick"].dataframe.tail(2)
        avg_price = considered_df["price"].mean()
        if avg_price > 6.0:
            print("The average of the last two events is: " + str(avg_price))


def avg_price_last_30_minutes():
    target_date = np.datetime64('now') - np.timedelta64(30, 'm')
    considered_df = all_dataframes_in_memory["StockTick"].dataframe
    considered_df = considered_df[considered_df["INSERTION_TIMESTAMP"] > target_date]
    avg_price = considered_df["price"].mean()
    print("mean in the last 30 minutes: " + str(avg_price))


all_dataframes_in_memory["StockTick"] = DataframeManager(["symbol", "price"])
all_dataframes_in_memory["StockTick"].add_observer(avg_price_last_two_events_observer)
all_dataframes_in_memory["StockTick"].add_observer(avg_price_last_30_minutes)
for i in range(20):
    all_dataframes_in_memory["StockTick"].add({"symbol": "AAPL", "price": float(randrange(1, 10))})