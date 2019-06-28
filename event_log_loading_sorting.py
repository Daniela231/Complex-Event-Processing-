from pm4py.objects.log.importer.xes import factory as xes_importer
from pm4py.objects.conversion.log import factory as conv_factory
from pm4py.objects.log.util import sorting

log = xes_importer.apply("Xes File")
stream = conv_factory.apply(log)
stream = sorting.sort_timestamp(stream)
print(stream)
