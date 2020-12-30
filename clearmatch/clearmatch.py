# Author: Darren Colby
# Date: 12/29/2020
# Purpose: To link records from one dataset to another by using a linking key


from matplotlib.pyplot import bar, show, suptitle
from numpy import NaN
import pandas as pd


class ClearMatch:

    NAMES = ["Missing", "Nonmissing"]

    missing_count = [0, 0]
    records_dict = dict()

    def __init__(self, host_col, host_data, key_col, value_cols, guest_data):
        """A constructor for the ClearMatch class.
           @Parameters: host_col, the index of the column to find matches for;
                host_data, a DataFrame that contains a column to be matched to;
                key_col, the index of the column to be used as the linking key;
                values_col, list of guest_data indices with potential matches;
                guest_data, DataFrame with a linking key and potential matches.
           Note: host_data and key_data may or may not come from separate
                DataFrames."""

        # Various statements to enforce parameter types
        if not isinstance(host_col, int):
            raise TypeError("the host_col parameter must be an integer")

        if not isinstance(host_data, pd.DataFrame):
            raise TypeError("the host must be a Pandas DataFrame object")

        if not isinstance(key_col, int):
            raise TypeError("the key_col parameter must be an integer")

        if not isinstance(guest_data, pd.DataFrame):
            raise TypeError("the key must be a Pandas DataFrame object")

        if not isinstance(value_cols, list):
            raise TypeError("the value_cols parameter must be a list that "
                            "corresponds to the key_data")

        # Actual class attributes
        self.host_df = host_data
        self.host_col = host_col
        self.host_data = pd.DataFrame(host_data.iloc[:, self.host_col])
        self.key_col = key_col
        self.key_data = pd.DataFrame(guest_data.iloc[:, self.key_col])
        self.value_data = guest_data.iloc[:, -key_col:]
        self.hcol = self.host_data.columns[self.host_col]

    def create_lookup(self):
        """Creates a dictionary where each key is a linking key and values are
            lists of potential matches for the original record.
            @return: A dictionary of linking keys and potential matches."""

        # noinspection PyTypeChecker
        # Used to create a dictionary
        key_tuple = ([i for lst in self.key_data.values.tolist() for i in lst])
        values_tuple = (self.value_data.values.tolist())
        index = 0

        # Creates a dictionary object where the keys are linking keys and the
        # values are lists of potential matches to the original record
        for element in key_tuple:
            self.records_dict[str(element)] = values_tuple[index]
            index += 1

        return self.records_dict

    def join(self, match_substring=False):
        """Adds a column of keys that correspond to host values or inserts NaNs
           if no match exists.
           @Parameters: match_substring; whether to look for substrings.
           @Return: The host DataFrame with a column for matches."""

        self.host_df['Match'] = NaN  # New column for matches

        # Calls helper methods
        if not match_substring:
            self.__join_exact_helper(self.host_data, self.host_col, self.hcol,
                                     self.host_df)
        else:
            self.__join_any_helper(self.host_data, self.host_col, self.hcol,
                                   self.host_df)

        return self.host_df

    def join_substring(self):
        """Adds a column of keys that correspond to matches between records in
           the host and guest data. If no matches are found for a host record,
           the value at its index in the new column is NaN.
           @return: The host_df with a column appended for matches"""

        self.host_data['PartialMatch'] = NaN  # New column for matches

        # Calls a private helper method
        self.__join_any_helper(self.host_data, self.host_col, self.hcol,
                               self.host_df, match_substring=True)

        return self.host_df

    def block(self, col):
        """Creates DataFrames based on unique values in a host_data column.
           @Parameters: col: column used to create sub-DattaFrames.
           @return: A dictionary with unique entries from the user-defined
                column as keys and dataframes where the the entries in the
                user-defined column are equal to the key as dictionary values"""

        df_names = dict()  # Dictionary of partitioned DataFrames

        for key, value in self.host_df.groupby(str(col)):
            df_names[key] = value

        return df_names

    def replace(self, match_substring=False):
        """Replaces records in the host DataFrame with a linking key if matches
           are found; otherwise replaces records with NaN.
           @Parameters: match_substring; whether to find partial or exact match.
           @Return: Host DataFrame with records replaced by matches or NaN."""

        self.missing_count = [0, 0]  # Reinitialize so matches are not recounted

        # Calls helper methods
        if not match_substring:
            self.__replace_exact_helper(self.host_data, self.host_col,
                                        self.host_df)
        else:
            self.__replace_any_helper(self.host_data, self.host_col,
                                      self.host_df)

        return self.host_df

    def summary(self):
        """Prints basic information about the data and its missingness.
           Cannot be called before a replace or join methods are called.
           @Return: A tuple of information about the data and missingness"""

        if self.missing_count[0] + self.missing_count[1] < len(self.host_data):
            raise TypeError("the replace or join methods must be called before "
                            "calculating summary information")

        print("Data Types:")
        print(self.host_df.dtypes)
        print("Number of records:", self.host_data.iloc[:, 0].size)
        print("Number of matches:", self.missing_count[1])
        print("Number of missing records:", self.missing_count[0])
        print("Percentage of missing records:", (self.missing_count[0] /
                                                 self.missing_count[1]) * 100)

        return self.host_data.dtypes, self.host_data.iloc[:, 0].size, \
            self.missing_count[1], self.missing_count[0], \
            (self.missing_count[0] / self.missing_count[1]) * 100

    def __join_any_helper(self, host_data, host_col, hcol, host_df,
                          match_substring=False):
        """This function should only be called by a clearmatch object!
           Adds a column of keys that correspond to host values  with substrings
              or inserts NaNs if no match exists.
           @Parameters: (host_data...host_df); Passed by a join method."""

        # Checks each original entry
        for record in host_data.iloc[:, host_col]:

            # Goes to each dictionary value/list entry of possible matches
            for key in self.records_dict:

                # Looks at each potential match in the list of possible matches
                for string in self.records_dict[key]:

                    # If the record is a substring of a potential match,
                    # saves the index of the record
                    if record in string:
                        n = host_data[host_data[hcol] == record].index[0]

                        # Adds the linking key to that record's index in the
                        # 'Match' column
                        if match_substring and host_df.loc[n, 'Match'] != key:
                            host_df.loc[n, 'PartialMatch'] = key
                            self.missing_count[1] += 1

                        # If no exact match exists,
                        # replaces the record with NaN in the 'Matches' column
                        if not match_substring:
                            host_df.loc[n, 'Match'] = key
                            self.missing_count[1] += 1

        self.missing_count[0] = (len(host_data) - self.missing_count[1])

    def __join_exact_helper(self, host_data, host_col, hcol, host_df):
        """This function should only be called by a clearmatch object!
           Adds a column of keys that correspond to exact matches between
              host and guest data or inserts NaN if there is no exact match.
           @Parameters: (host_data...host_df); passed by the join method."""

        # Checks each original entry
        for record in host_data.iloc[:, host_col]:

            # Goes through each dictionary value/list of possible matches
            for key in self.records_dict:

                # Looks at each potential match in the list of possible matches
                if record in self.records_dict[key]:

                    # Save index of each entry and add found match at that index
                    n = host_data[host_data[hcol] == record].index[0]
                    host_df.loc[n, 'Match'] = key
                    self.missing_count[1] += 1

        self.missing_count[0] = (len(host_data) - self.missing_count[1])

    def __replace_any_helper(self, host_data, host_col, host_df):
        """This function should only be called by a clearmatch object!
           Replaces records with a match that it is a substring of or with
              NaN if no match is found.
           @Parameters: (host_data...host_df); passed from a replace method."""

        # Checks each original entry
        for record in host_df.iloc[:, host_col]:

            # Goes through each dictionary value/list of possible matches
            for key in self.records_dict:

                # Looks at each potential match in the list of possible matches
                for string in self.records_dict[key]:

                    # If the record is a substring of a potential match,
                    # replaces the record with its match
                    if record in string:

                        host_df.replace(record, str(key), inplace=True)
                        self.missing_count[1] += 1

        self.missing_count[0] = (len(host_data) - self.missing_count[1])

    def __replace_exact_helper(self, host_data, host_col, host_df):
        """This function should only be called by a clearmatch object!
           Checks for exact matches for each record and replaces them with the
              associated linking key. If no match is found, a record is replaced
              with NaN.
           @Parameters: (host_data...host_df); passed by replace method."""

        # Checks each original entry
        for record in host_data.iloc[:, host_col]:

            # Goes through each dictionary value/list of possible matches
            for key in self.records_dict:

                # Looks at each potential match in the list of possible matches
                if record in self.records_dict[key]:

                    # Replaces matches with the linking key
                    host_df.replace(record, str(key), inplace=True)
                    self.missing_count[1] += 1

        self.missing_count[0] = (len(host_data) - self.missing_count[1])

    @classmethod
    def plot(cls):
        """Creates a bar plot of missing vs. non-missing values"""

        bar(cls.NAMES, cls.missing_count)
        suptitle('Missingness')
        show()
