# clearmatch
Clearmatch is a package for matching records from one dataset to another by using a key, which has reference records. 
If the records to be matched to are synonyms of a reference record in the key that record is matched with its reference.
Clearmatch also makes it easy to see summary statistics and generate bar plots of missingness.

Dependencies: Matplotlib, Numpy, Pandas


Installation

![Installation](images/Installation.jpg){:height="36px" width="36px"}

Creating ClearMatch objects from DataFrames

![Creating ClearMatch Objects from DataFrame objects](images/make_object.jpg){:height="36px" width="36px"}

Partitioning the host DataFrame based on unique values in a given column
  *Note that the resulting DataFrames are returned in a dictionary, so you should use the ['name'] convention to access the DataFrames
  
![Partitioning the host DataFrame](images/partition.jpg){:height="36px" width="36px"}

Defining the lookup structures for matching

![Defining the lookup structures for matching](images/lookup_structures.jpg){:height="36px" width="36px"}


Joining matches to the host DataFrame
  
![Joining matches to the host DataFrame](images/join.jpg){:height="36px" width="36px"}

Showing summary statistics

![Showing summary statistics](images/summary.jpg){:height="36px" width="36px"}

Visualizing missingness

![Visualizing missingness](images/visualize.jpg){:height="36px" width="36px"}
