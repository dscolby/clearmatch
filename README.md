# clearmatch
Clearmatch is a package for matching records from one dataset to another by using a key, which has reference records. 
If the records to be matched to are synonyms of a reference record in the key that record is matched with its reference.
Clearmatch also makes it easy to see summary statistics and generate bar plots of missingness.

Dependencies: Matplotlib, Numpy, Pandas


Installation

![Installation](images/Installation.jpg)

Creating ClearMatch objects from DataFrames

![Creating ClearMatch Objects from DataFrame objects](images/make_object.jpg)

Defining the lookup structures for matching

![Defining the lookup structures for matching](images/lookup_structures.jpg)

Partitioning the host DataFrame based on unique values in a given column
  *Note that the resulting DataFrames are returned in a dictionary, so you should use the ['name'] convention to access the DataFrames
  
![Partitioning the host DataFrame](images/partition.jpg)

![Partitioning the host DataFrame](images/partition_out.jpg)
