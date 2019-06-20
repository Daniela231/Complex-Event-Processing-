# Complex-Event-Processing

This Project is an implementation of a CEP algorithm using the PM4Py and Pandas library.

DataframeManager.py

The DataframeManager script is the main core in handling the Dataframe and adding observers to said Dataframe we want to watch.


length_window.py

The length_window script contains the functions to get some rows of the dataframe (a window) depending on a given length


time_window.py

The length_window script contains the functions to get some rows of the dataframe (a window) depending on a given time


data_window.py

Contains filter functionsafter expressions(exp) to filter the frame and remove events where the expression returns false

statistics_views.py

Contains functions for statistic calculations on a given dataframe


special_derived_value.py

Contains the size and weighted avarage function 

cep.py

This script is our testing script if implementations of length/time_window are working as intended or not.
"Test" code will be added with every bigger implementation of a function. This will serve
as a kind of unit-test.

Logfiles

Those files contains the last cep test as a logging tool for an easier type to format to analyse the 
output for errors/malicious behaviour

The ceptest#n.log file contain the output of the n-th test while the cep generator logfile contains
the output of the eventstream, non sorted tests and additional information of functions, that we
might need for debugging purposes.

LoggerSetter.py

This file contains the configurations for the logger methods to have a cleaner cep test file and to have
a better time changing the settings of the loggers if the need for it arises.