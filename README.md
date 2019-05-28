# Complex-Event-Processing

This Project is an implementation of a CEP algorithm using the PM4Py and Pandas library.

DataframeManager.py

The DataframeManager script is the main core in handling the Dataframe and adding observers to said Dataframe we want to watch.


length_window.py

The length_window script contains the functions to change the length of the Dataframe
we want to run our observers on.


time_window.py

time_window has the same functionality as length_window but filters the Dataframe 
to only contain the events at an exact time or given time period(like 30 minutes in the past)


cep.py

This script is our testing script if implementations of length/time_window are working as intended or not.
"Test" code will be added with every bigger implementation of a function. This will serve
as a kind of unit-test.