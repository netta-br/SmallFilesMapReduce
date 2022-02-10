from MapReduceEngine import *
from io import BytesIO
import pandas as pd


def inverted_map(document_name, column_index):
    storage = Storage()  # load configuration from file
    bytes_data = storage.get_object('my_bucket', document_name)
    df = pd.read_csv(BytesIO(bytes_data))
    selected_column = df.iloc[:, column_index]
    return [(value, document_name) for value in selected_column]


def inverted_reduce(value, documents):
    temp_list = documents.split(", ")
    temp_list = list(dict.fromkeys(temp_list))  # remove duplicates from list
    documents = ', '.join(temp_list)
    return value, documents


input_data = 'my_bucket/Data'
mapreduce = MapReduceEngine()
results = mapreduce.execute(input_data, inverted_map, inverted_reduce, params={'column': 1})
print('Inverted index result:')
if results is not None:
    for res in results:
        print(res, end='\n')
