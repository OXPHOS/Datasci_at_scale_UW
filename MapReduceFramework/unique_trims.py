import MapReduce
import sys

"""
Input:  [sequence id, nucleotides] where sequence id is a string representing a unique identifier for the sequence 
        and nucleotides is a string representing a sequence of nucleotides
Trim last 10 nt from tail and remove and duplicates
Output:  Unique trimmed nucleotide strings
python unique_trims.py data/dna.json
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    seq_id = record[0]
    seq = record[1]
    mr.emit_intermediate(seq[:-10], 1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    if len(list_of_values)==1:
      mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
