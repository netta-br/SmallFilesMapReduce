import os

from lithops import FunctionExecutor, Storage

print("Hello world, hello conda")


# ---------------------------------------
# Hello world example using Futures API:
# ---------------------------------------
def hello(name):
    return 'Hello {}!'.format(name)


with FunctionExecutor() as fexec:
    fut1 = fexec.call_async(hello, 'World1')
    fut2 = fexec.call_async(hello, 'World2')
    print(fut1.result())
    print(fut2.result())

with FunctionExecutor() as fexec:
    fut3 = fexec.call_async(hello, 'World3')
    print(fut3.result())

#
# # ---------------------------------
# # Upload files to object storage:
# # ---------------------------------
# storage = Storage()  # backend='localhost'
# local_data_folder = 'Data'
#
# # List all file in local directory:
# listing = os.listdir(local_data_folder)
# print(type(listing))
# print(listing)
#
# # Upload all the input CSV files that you used in homework 2 into the bucket
# for filename in listing:
#     filepath = os.path.join(local_data_folder, filename)
#     # print(filepath)
#     prefix = 'Data'
#     object_name = os.path.join(prefix, filename)
#     # print(object_name)
#     with open(filepath, 'rb') as fl:
#         storage.put_object(bucket='my_bucket', key=object_name, body=fl)
#
# # ------------------------------------------------
# # Retrieve objects/ metadata from object storage:
# # ------------------------------------------------
#
# # Get list of all objects in bucket for a given prefix:
# listing = storage.list_keys(bucket='my_bucket', prefix='Data')
# print('listing:')
# for obj_name in listing:
#     print(f'\t {obj_name}')
# print('\n')
#
# # Get object from storage:
# print(f'{listing[0]}:')
# data = storage.get_object(bucket='my_bucket', key=listing[0], stream=False)
# print(f'\t type(data) = {type(data)}')
# print(f'\t data = {data}')
# print('\n')
#
# # Get metadata of object:
# print('metadata:')
# for obj_name in listing:
#     print(f'{obj_name}')
#     obj_metadata = storage.head_object(bucket='my_bucket', key=obj_name)
#     object_size = obj_metadata["content-length"]
#     # print(f'\t obj_metadata = {obj_metadata}')
#     print(f'\t object_size = {object_size}')
#

