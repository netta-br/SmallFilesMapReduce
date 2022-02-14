from MapReduceEngine import *
import pandas as pd


def inverted_map(cloud_object, column_index):
    storage = Storage()  # load configuration from file
    bytes_data = storage.get_cloudobject(cloud_object, stream=True)
    df = pd.read_csv(bytes_data)
    selected_column = df.iloc[:, column_index]
    document_name = cloud_object.key
    return [(value, document_name) for value in selected_column]


def inverted_reduce(value, documents):
    temp_list = documents.split(", ")
    temp_list = list(dict.fromkeys(temp_list))  # remove duplicates from list
    documents = ', '.join(temp_list)
    return value, documents


# Invoke MapReduce:
input_data = 'my_bucket/myCsvFiles'
chunk_size = 20000  # bytes
mapreduce = MapReduceEngine(chunk_size=chunk_size)
results, map_info = mapreduce.execute(input_data, inverted_map, inverted_reduce,
                                      params={'column': 1, 'aggregate': True})

print('Inverted index: done.')

print('Map info:')
print(f'\t Number of jobs: {map_info["num_jobs"]}')
print(f'\t Execution time: {map_info["exec_time"]:.3f} seconds')

# print('Inverted index result:')
# if results is not None:
#     for res in results:
#         print(res, end='\n')



