# Design

There is an index and time data for each "key". The key, index and data together make a 
timeline. Timelines are broken up as continuous frames of time ranges with buckets at a given 
time resolution. Each bucket contains the value of counts in that bucket, 
a sum up to that bucket of all values, and a sum of squared values up to that bucket. 

Given two buckets, you can compute sum, average, and variance in constant time.

The index and data structures store the data using memory mapped files
for optimal performance.

## Complexity Analysis

Finding a time range inside the index is O(log(n)) because binary search is used
for finding the index entry. 

Once two time ranges are found, then finding a bucket within the range is constant time
since it is an offset from start based on bucket resolution. 

Computing the sum, average, and variance between two found buckets is constant time
since the sums and sum of squares up to that point are stored in the bucket. 
The sum operation then becomes a simple subtraction.

Inserting an entry to the DB is constant time since only inserts into the last
time range are allowed within a fixed time interval. This restriction is designed to 
maintain constant time inserts into the DB.
