# Author: Darren
# Date: 12/23/2020
# Purpose: To match records from one dataset to another using synonyms from the second dataset

from matplotlib.pyplot import bar, show, suptitle
from numpy import NaN
import pandas as pd

NAMES = ["Missing", "Nonmissing"]

missing_count = [0, 0]
records_dict = {}


class ClearMatch:
    def __init__(self, host_col, host_data, key_col, key_data, value_cols):
        """A constructor for the ClearMatch class
        Parameters: host_col, the index of the column to be matched to; host_data, a DataFrame that contains a column
            be matched to; key_col, the index of the column to be used as the linking key; key_data, a DataFrame that
            contains the key_col; values_col, a list of indices within the key_data to be matched with the host_data
        Note: host_data and key_data may or may not come from separate DataFrames"""
        # Various statements to enforce parameter types
        if not isinstance(host_col, int):
            raise TypeError("the host_col parameter must be an integer")

        if not isinstance(host_data, pd.DataFrame):
            raise TypeError("the host must be a Pandas DataFrame object")

        if not isinstance(key_col, int):
            raise TypeError("the key_col parameter must be an integer")

        if not isinstance(key_data, pd.DataFrame):
            raise TypeError("the key must be a Pandas DataFrame object")

        if not isinstance(value_cols, list):
            raise TypeError("the value_cols parameter must be a list that corresponds to the key_data")

        self.host_df = host_data  # To return after joining or replacing data
        self.host_col = host_col
        self.host_data = pd.DataFrame(host_data.iloc[:, self.host_col])
        self.key_col = key_col
        self.key_data = pd.DataFrame(key_data.iloc[:, self.key_col])
        self.value_data = key_data.iloc[:, -key_col:]
        self.hcol = self.host_data.columns[self.host_col]  # The name of the host_column to use in the join method

    def create_lookup(self):
        """Creates a dictionary with records in the key parameter as keys and corresponding rows in the values
            parameter as values"""
        # noinspection PyTypeChecker
        key_tuple = tuple([i for sublist in self.key_data.values.tolist() for i in sublist])
        values_tuple = tuple(self.value_data.values.tolist())
        index = 0

        for element in key_tuple:
            records_dict[str(element)] = values_tuple[index]
            index += 1

        return records_dict

    def join(self, match_substring=False):
        """Adds a column of keys that correspond to host values or inserts NaNs if no match exists"""
        self.host_df['Match'] = NaN  # New column for matches

        if not match_substring:
            self.join_exact_helper(self.host_data, self.host_col, self.hcol, self.host_df)
        else:
            self.join_any_helper(self.host_data, self.host_col, self.hcol, self.host_df)

        return self.host_df

    def join_substring(self):
        """Adds a column of keys that correspond to potential matches in which host values are substrings.
        Also adds an additional column."""
        self.host_data['PartialMatch'] = NaN
        self.join_any_helper(self.host_data, self.host_col, self.hcol, self.host_df, match_substring=True)
        return self.host_df

    def block(self, col):
        """Creates DataFrames based on unique values in a given column in host_data"""
        df_names = {}

        for k, v in self.host_df.groupby(str(col)):
            df_names[k] = v

        return df_names

    def replace(self, match_substring=False):
        """Checks host values in the dictionary and replaces them with their associated keys or NaN is no key is
        found """
        global missing_count
        missing_count = [0, 0]

        if not match_substring:
            self.replace_exact_helper(self.host_data, self.host_col, self.host_df)
        else:
            self.replace_any_helper(self.host_data, self.host_col, self.host_df)
        return self.host_df

    def summary(self):
        """Returns basic information about the data and its missingness"""
        if missing_count[0] == 0 or missing_count[1] == 0:
            raise TypeError("the replace or join methods must be called before calculating summary information")

        print("Data Types:")
        print(self.host_df.dtypes)
        print("Number of records:", self.host_data.iloc[:, 0].size)
        print("Number of matches:", missing_count[1])
        print("Number of missing records:", missing_count[0])
        print("Percentage of missing records:", (missing_count[0] / missing_count[1]) * 100)

        return self.host_data.dtypes, self.host_data.iloc[:, 0].size, missing_count[1], missing_count[0], \
            (missing_count[0] / missing_count[1]) * 100

    # Adds a column of keys that correspond to host values  with substrings or inserts NaNs if no match exists
    # Parameters: Attributes passed from the clearmatch class
    @staticmethod
    def join_any_helper(host_data, host_col, hcol, host_df, match_substring=False):
        """This function should only be called by a clearmatch object"""
        for key in records_dict:
            for record in host_data.iloc[:, host_col]:
                for string in records_dict[key]:
                    if record in string:
                        n = host_data[host_data[hcol] == record].index[0]  # Stores the index

                        if not match_substring:  # If no matches have been found
                            host_df.loc[n, 'Match'] = key  # Replaces the value at index n with the key
                            missing_count[1] += 1

                        if match_substring and host_df.loc[n, 'Match'] != key:  # If exact matches are already found
                            host_df.loc[n, 'PartialMatch'] = key
                            missing_count[1] += 1

        missing_count[0] = (host_data.iloc[:, 0].size - missing_count[1])

    # Adds a column of keys that correspond to host values or inserts NaNs if no match exists
    # Parameters: Attributes passed from the clearmatch class
    @staticmethod
    def join_exact_helper(host_data, host_col, hcol, host_df):
        """This function should only be called by a clearmatch object"""
        for key in records_dict:
            for record in host_data.iloc[:, host_col]:
                if record in records_dict[key]:
                    n = host_data[host_data[hcol] == record].index[0]  # Stores the index
                    host_df.loc[n, 'Match'] = key  # Replaces the value at index n with the key
                    missing_count[1] += 1

        missing_count[0] = (host_data.iloc[:, 0].size - missing_count[1])

    @staticmethod
    def plot():
        """Creates a bar plot of missing vs. non-missing values"""
        bar(NAMES, missing_count)
        suptitle('Missingness')
        show()

    # Checks host values in the DataFrame and replaces them with their associated keys they are substrings of aliases
    @staticmethod
    def replace_any_helper(host_data, host_col, host_df):
        """This function should only be called by a clearmatch object"""
        for key in records_dict:
            for record in host_df.iloc[:, host_col]:
                for string in records_dict[key]:
                    if record in string:
                        host_df.replace(record, str(key), inplace=True)  # Replaces element with correct key
                        missing_count[1] += 1  # Useful so we can see statistics on missingness later

        missing_count[0] = (host_data.iloc[:, 0].size - missing_count[1])

    # Checks host values in the DataFrame and replaces them with their associated keys
    @staticmethod
    def replace_exact_helper(host_data, host_col, host_df):
        """This function should only be called by a clearmatch object"""
        for key in records_dict:
            for record in host_data.iloc[:, host_col]:
                if record in records_dict[key]:
                    host_df.replace(record, str(key), inplace=True)  # Replaces element with correct key
                    missing_count[1] += 1  # Useful so we can see statistics on missingness later

        missing_count[0] = (host_data.iloc[:, 0].size - missing_count[1])
