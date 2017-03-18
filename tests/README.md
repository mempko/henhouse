# tests

The most important test is the hammer test which stresses Henhouse.
It uses a fuzz testing strategy by pushing both valid and corrupt data into the 
DB and doing both valid and corrupted queries.

This test is meant to run forever and helps achieve a high code coverage.
