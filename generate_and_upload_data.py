import os
import random
import pandas as pd
from lithops import Storage

local_data_folder = 'Data'

# ---------------
# Generate data:
# ---------------
firstname = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna']
city = ['NewYork', 'Haifa', 'Munchen', 'London', 'PaloAlto', 'TelAviv', 'Kiel', 'Hamburg']
secondname = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']

# Generate random values:
num_files = 1000
num_records = 10
firstnames = random.choices(firstname, k=num_files*num_records)
cities = random.choices(city, k=num_files*num_records)
secondnames = random.choices(secondname, k=num_files*num_records)

# Create a pandas dataframe:
df = pd.DataFrame(list(zip(firstnames, secondnames, cities)), columns=['firstname', 'secondname', 'city'])

if not os.path.exists(local_data_folder):
    os.mkdir(local_data_folder)

# Save dataframe to CSV files:
filenames = []
filepaths = []
for ifile in range(num_files):
    filename = "myCSV{:02d}.csv".format(ifile+1)
    filepath = os.path.join(local_data_folder, filename)
    filenames.append(filename)
    filepaths.append(filepath)
    first_record = ifile * num_records
    last_record = first_record + num_records
    df_chunk = df.iloc[first_record:last_record]
    df_chunk.to_csv(filepath, index=False)

# --------------------------------
# Upload files to object storage:
# --------------------------------
storage = Storage()  # backend='localhost'

# Upload all the files into the bucket:
for index, filename in enumerate(filenames):
    filepath = filepaths[index]
    prefix = 'Data'
    object_name = os.path.join(prefix, filename)
    print(object_name)
    with open(filepath, 'rb') as fl:
        storage.put_object(bucket='my_bucket', key=object_name, body=fl)

