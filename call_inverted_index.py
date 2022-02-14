from MapReduceEngine import *
import pandas as pd

RUN_SMALL_TASK_SANITY_CHECK = True
RUN_TEST_CHUNK_SIZE = False
RUN_TEST_NUM_OF_FILES = False


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


if RUN_SMALL_TASK_SANITY_CHECK:
    input_data = 'my_bucket/myCsvFiles'
    mapreduce = MapReduceEngine(chunk_size=500)
    results, map_info = mapreduce.execute(input_data, inverted_map, inverted_reduce,
                                          params={'column': 1, 'aggregate': True, 'input_limit': 10})
    print('Inverted index: done.')
    print('Map info:')
    print(f'\t Number of jobs: {map_info["num_jobs"]}')
    print(f'\t Execution time: {map_info["exec_time"]:.3f} seconds')

    print('Inverted index result:')
    if results is not None:
        for res in results:
            print(res, end='\n')

# aggregate: boolean, if set to True then aggregate small files.
# input limit: limit number of input files to MapReduce. If set to None then there is no limit.
# chunk_size: in bytes.

if RUN_TEST_CHUNK_SIZE:
    # Test 01: finding the optimal chunk size for current Platform.
    param_sets = [{'aggregate': False, 'input_limit': None, 'chunk_size': None},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 1000},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 2000},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 5000},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 10000},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 20000},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 50000},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 100000},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 200000},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 500000},
                  {'aggregate': True, 'input_limit': None, 'chunk_size': 1000000},
                  ]

    # Invoke MapReduce:
    test_result = []
    input_data = 'my_bucket/myCsvFiles'
    for param_set in param_sets:
        mapreduce = MapReduceEngine(chunk_size=param_set['chunk_size'])
        results, map_info = mapreduce.execute(input_data, inverted_map, inverted_reduce,
                                              params={'column': 1, 'aggregate': param_set['aggregate'], 'input_limit': param_set['input_limit']})
        test_result.append(map_info)

    for result in test_result:
        print(result)

