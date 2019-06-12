from DataframeManager import *
from length_window import *

retain = {}

def keep_all():
    retain.dataframe = all_dfs.dataframe
    return True


def expiry_exp(exp):

    key = ('expiry_exp', 'exp')

    newest_event = last_event()
    newest_timestamp = newest_event['INSERTION_TIMESTAMP'].iloc[0]


    try:

        all_dfs[key].variables['current_count'] = all_dfs[key].variables['current_count'] + 1
    except:
        all_dfs[key] = DataframeManager()
        all_dfs[key].dataframe = pd.DataFrame()
        current_count = 1
        oldest_event = newest_event
        oldest_timestamp = newest_timestamp

        # view_reference = ?????????????????ß

    newest_event = last_event()
    newest_timestamp = newest_event['INSERTION_TIMESTAMP'].iloc[0]
    all_dfs[key].dataframe = all_dfs[key].dataframe.append(newest_event)
    expired_count = 0

    # Observer
    while not eval(exp):
        all_dfs[key].dataframe = all_dfs[key].dataframe.iloc[1:]
        current_count = current_count - 1
        expired_count = expired_count + 1
        oldest_event = all_dfs[key].dataframe.head(1)
        oldest_timestamp = oldest_event['INSERTION_TIMESTAMP'].iloc[0]

    return all_dfs[key].dataframe

