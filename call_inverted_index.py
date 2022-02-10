from SmallFilesMapReduce import *


def inverted_map(document_name, df):
    result = []
    for column in df.columns:
        df[column] = column + "_" + df[column]
    for index, row in df.iterrows():
        [result.append((key, document_name)) for key in row]
    return result


def inverted_reduce(value, documents):
    return [value, ",".join(list(set(documents.split(","))))]


input_data = 'my_bucket/Data'
mapreduce = SFMapReduceEngine()
results = mapreduce.execute(input_data, inverted_map, inverted_reduce)
print(results)
