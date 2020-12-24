# Author: Darren Colby
# Date: 11/25/2020
# Purpose: To test the functioning of ClearMatch

from clearmatch.clearmatch import ClearMatch
import pandas as pd

# Import the dataframe and split it into host, key, and values components
test_df = pd.read_csv("test.csv")
host_df = pd.DataFrame(test_df['host'])
key_df = pd.DataFrame(test_df.iloc[:, 1:6])
value_list = [2, 3, 4, 5, 6]

# Create a ClearMatch object
test_clearmatch = ClearMatch(0, host_df, 0, key_df, value_list)

# Build the lookup structures
test_clearmatch.create_lookup()

# Test the partition method
test_partitions = test_clearmatch.partition('host')
print(test_partitions['delfin'])

# Join the host records with matches from the key dataframe
joined_data = test_clearmatch.join()

print(joined_data.head(7))

test_clearmatch.plot()

test_clearmatch.summary()

# Join the host records with potential matches that contain substrings of the host records
joined_data = test_clearmatch.join_substring()

print(joined_data.head(7))

test_clearmatch.plot()

test_clearmatch.summary()

# Replace the host records with potential matches that contain substrings of the host records
replaced_data = test_clearmatch.replace(match_substring=True)

print(replaced_data.head(7))

test_clearmatch.plot()

test_clearmatch.summary()
