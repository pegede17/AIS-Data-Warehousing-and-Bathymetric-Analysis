import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import Dimension, FactTable



dw_string = "host='localhost' dbname='p9-test' user='postgres' password='admin'"
dw_pgconn = psycopg2.connect(dw_string)
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=dw_pgconn)



ais_file_handle = open('aisdk-2021-07/aisdk-2021-07-31.csv', 'r')
ais_source = CSVSource(f=ais_file_handle, delimiter=',')



# Creation of dimension and fact table abstractions for use in the ETL flow
time_dimension = Dimension(
    name='time',
    key='timeid',
    attributes=['year', 'month', 'date', 'day', 'week', 'hour', 'minute', 'second'])

trip_dimension = Dimension(
    name='location',
    key='locationid',
    attributes=['city', 'region'],
    lookupatts=['city'])

ship_dimension = Dimension(
    name='ship',
    key='shipid',
    attributes=['mmsi', 'imo', 'ship_type', 'name', 'width', 'length', 'type_of_positioning_device', 'callsign']
)

static_dimenson = Dimension(
    
)

dynamic_dimension = Dimension(
    name='dynamic',
    key='dynamicid',

)

fact_table = FactTable(
    name='facttable',
    keyrefs=['bookid', 'locationid', 'timeid'],
    measures=['sale'])

# # Python function needed to split the timestamp into its three parts
# def split_timestamp(row):
#     """Splits a timestamp containing a date into its three parts"""

#     # Splitting of the timestamp into parts
#     timestamp = row['timestamp']
#     row['year'] = timestamp.year
#     row['month'] = timestamp.month
#     row['day'] = timestamp.day

# # The location dimension is loaded from the CSV file, and in order for
# # the data to be present in the database, the shared connection is asked
# # to commit
# [location_dimension.insert(row) for row in ais_source]

# # The file handle for the CSV file can then be closed
# ais_file_handle.close()

# # Each row in the sales database is iterated through and inserted
# for row in sales_source:

#     # Each row is passed to the timestamp split function for splitting
#     split_timestamp(row)

#     # Lookups are performed to find the key in each dimension for the fact
#     # and if the data is not there, it is inserted from the sales row
#     row['bookid'] = book_dimension.ensure(row)
#     row['timeid'] = time_dimension.ensure(row)

#     # The location dimension is pre-filled, so a missing row is an error
#     row['locationid'] = location_dimension.lookup(row)
#     if not row['locationid']:
#         raise ValueError("city was not present in the location dimension")

#     # The row can then be inserted into the fact table
#     fact_table.insert(row)

# # The data warehouse connection is then ordered to commit and close
# dw_conn_wrapper.commit()
# dw_conn_wrapper.close()