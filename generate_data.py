import os
import random
import pandas as pd

output_folder = 'Data'

firstname = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna']
city = ['NewYork', 'Haifa', 'Munchen', 'London', 'PaloAlto',  'TelAviv', 'Kiel', 'Hamburg']
secondname = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']

# Generate random values:
num_files = 20
num_records = 10
firstnames = random.choices(firstname, k=num_files*num_records)
cities = random.choices(city, k=num_files*num_records)
secondnames = random.choices(secondname, k=num_files*num_records)

# Create a pandas dataframe:
df = pd.DataFrame(list(zip(firstnames, secondnames, cities)), columns=['firstname', 'secondname', 'city'])

if not os.path.exists(output_folder):
    os.mkdir(output_folder)

# Save dataframe to CSV files:
for ifile in range(num_files):
    filename = "myCSV{:02d}.csv".format(ifile+1)
    filepath = os.path.join(output_folder, filename)
    print(filepath)
    first_record = ifile * num_records
    last_record = first_record + num_records
    df_chunk = df.iloc[first_record:last_record]
    df_chunk.to_csv(filepath, index=False)

