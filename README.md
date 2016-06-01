FlyFish
========================

FlyFish is a proof of concept for creating a fast time series DB that can compute
sum, average, and variance between any two time ranges in constant time.

Limitation with the design is that pushing data to the DB must happen in the last
timestamp range because the DB stores a sums and squared sums table along with
actual values.

Design
=========================

There is an index and time data for each "key". The key, index and data together make a 
timeline. Time ranges are broken up as fixed frames with buckets at a given 
time resolution. Each bucket contains the value of counts in that bucket, 
a sum up to that bucket, and a sum of squared values up to that bucket. 
This is a 1D version of a summed area table with a second integral.

Given two time ranges, you can compute sum, average, and variance in constant time.

The index and data structures store the data in the files using memory mapped files
for optimal performance.

This prototype can put up to 18 million data points per second into the DB.


