import MapReduceMulti
import sys

"""
Input: [matrix, i, j, value] , matrix = 'a' or 'b'
Output: (i, j, value)
python multiply.py data/matrix.json
"""

mr = MapReduceMulti.MapReduce()
#NOTE: This is MapReduceMulti.MapReduce

# =============================
# Do not modify above this line

def mapper1(record):
  matrix = record[0]
  if matrix == 'a':
    mr.emit_intermediate(record[2], [record[0], record[1], record[3]])
  else:
    mr.emit_intermediate(record[1], [record[0], record[2], record[3]])

def reducer1(key, list_of_values):
    # key: col num of matrix a, row num of matrix b
    # value: list of occurrence counts
    for i in range(0, len(list_of_values)):
      for j in range(0, len(list_of_values)):
        if list_of_values[i][0]=="a" and list_of_values[j][0]=="b":
          mr.emit([list_of_values[i][1], list_of_values[j][1],
                  list_of_values[i][2]*list_of_values[j][2]])

def mapper2(record):
  mr.emit_intermediate((record[0], record[1]), record[2])

def reducer2(key, list_of_values):
  total = 0;
  for val in list_of_values:
    total += val
  mr.emit([key[0], key[1], total])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper1, reducer1, mapper2, reducer2)
