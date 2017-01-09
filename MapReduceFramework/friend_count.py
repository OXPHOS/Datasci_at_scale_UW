import MapReduce
import sys

"""
Friends Count
Input: 2 element list [personA, personB] where personA is a string representing the name of a person and personB is a string representing the name of one of personA's friends. 
	   Note that it may or may not be the case that the personA is a friend of personB
Output: list: [person, friend_count]
python friend_count.py data/friends.json
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: person A
    key = record[0]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: person A
    # value: list of friends counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

