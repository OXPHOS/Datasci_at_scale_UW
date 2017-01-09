import MapReduce
import sys

"""
Friends Count
Input: 2 element list [personA, personB] where personA is a string representing the name of a person and personB is a string representing the name of one of personA's friends. 
	   Note that it may or may not be the case that the personA is a friend of personB
Output: All pairs (friend, person) such that (person, friend) appears in the dataset but (friend, person) does not.
python asymmetric_friendships.py data/friends.json
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    A = record[0]
    B = record[1]
    if (A > B):
      mr.emit_intermediate(B, [A, 0])
    else:
      mr.emit_intermediate(A, [B, 1])


def reducer_emit(key, vals):
  if vals[1]:
    mr.emit((vals[0], key))
  else:
    mr.emit((key, vals[0]))

def reducer(key, list_of_values):
    # key: person with smaller names
    # value: another person and order info
    list_of_values.sort()

    if (len(list_of_values)==1):
      reducer_emit(key, list_of_values[0])
      return

    for idx in range(0, len(list_of_values)-1):
      if list_of_values[idx][0] != list_of_values[idx+1][0]:
        reducer_emit(key, list_of_values[idx])
      else:
        idx+=1

    if idx != len(list_of_values):
      reducer_emit(key, list_of_values[idx-1])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

