import os
from lithops import Storage


# ----------------------------------------------------------------------------
# Upload files to object storage:
# Upload all the input CSV files that we used in homework 2 into the bucket.
# ----------------------------------------------------------------------------

storage = Storage()  # backend='localhost'
local_data_folder = 'Data'

# List all file in local directory:
# TODO: filter listing to contain only CSV files
listing = os.listdir(local_data_folder)
print(type(listing))
print(listing)

# Upload all the files into the bucket:
for filename in listing:
    filepath = os.path.join(local_data_folder, filename)
    # print(filepath)
    prefix = 'Data'
    object_name = os.path.join(prefix, filename)
    # print(object_name)
    with open(filepath, 'rb') as fl:
        storage.put_object(bucket='my_bucket', key=object_name, body=fl)

# ------------------------------------------------
# Retrieve objects/ metadata from object storage:
# ------------------------------------------------

# Get list of all objects in bucket for a given prefix:
listing = storage.list_objects(bucket='my_bucket', prefix='Data')
print(type(listing))
print('listing:')
for obj_attr in listing:
    print(f'\t {obj_attr}')
    obj_size = obj_attr['Size']
    print(f'\t size: {obj_size} bytes')
print('\n')

for obj_attr in storage.list_objects(bucket='my_bucket', prefix='Data'):
    key = obj_attr['Key']
    size = obj_attr['Size']
    print(f'key: {key}, Size: {size}')

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


