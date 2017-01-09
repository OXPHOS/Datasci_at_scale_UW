import MapReduce
import sys

"""
Relational join: 
Input: 2 tables with overlapped columns
Output: join of the two tables over common columns (like join in SQL)
python join.py data/records.json


The first item (index 0) in each record is a string that identifies the table the record originates from. This field has two possible values:
"line_item" indicates that the record is a line item.
"order" indicates that the record is an order.
The second element (index 1) in each record is the order_id.

LineItem records have 17 attributes including the identifier string.
Order records have 10 elements including the identifier string.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: order id
    # value: other info
    key = record[1]
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: order id
    # value: other columns in the table
    for _ in range(0, len(list_of_values)):
      if list_of_values[_][0] == "order":
        left_table = list_of_values[_]
        order_pos=_
    list_of_values.pop(order_pos)

    for _ in range(0, len(list_of_values)):
      mr.emit(list(left_table+list_of_values[_]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
