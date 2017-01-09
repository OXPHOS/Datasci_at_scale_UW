# Multiple Rounds of MapReduce

import json

class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])  ### setdefault: set dict[key]=default if key is not already in dict
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value) 

    def execute(self, data, mapper1, reducer1, mapper2, reducer2):
        for line in data:
            record = json.loads(line)
            mapper1(record)

        for key in self.intermediate:
            reducer1(key, self.intermediate[key])

        self.intermediate = {}
        for line in self.result:
            mapper2(line)

        self.result = []
        for key in self.intermediate:
            reducer2(key, self.intermediate[key])

        #jenc = json.JSONEncoder(encoding='latin-1')
        jenc = json.JSONEncoder()
        for item in self.result:
            print jenc.encode(item)
