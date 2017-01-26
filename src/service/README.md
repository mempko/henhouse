#service

This directory implements the HTTP and Graphite compatible services.

#HTTP Service

The HTTP service only has a query interface. To put data into henhouse you must
use the graphite compatible input service

##/ping

###response

"pong"

##/summary

The summary endpoint gives you overall data about a timeline.


| Argument                    | Description                                                                                                  |
|:----------------------------|:--------------------------------------------------------------------------------------------------------------|
| keys                        |  Comma separated list of keys to query|

###response

The response is JSON object where the top level attributes are all keys requested. 
Each key is has the following attributes returned.

| Key                         | Description                                                                                                  |
|:----------------------------|:--------------------------------------------------------------------------------------------------------------|
| from                        |  Start of timeline in Unix time|
| to                          |  End of timeline in Unix Time|
| resolution                  |  Resolution of timeline in seconds|
| sum                         |  Sum of all values in the timeline|
| mean                        |  Mean of all values in the timeline|
| variance                    |  Variance of all values in the timeline|
| points                      |  Total amount of data points in the timeline|

##/diff

The diff endpoint allows you to query a timeline between two time ranges


| Argument                    | Description                                                                                                  |
|:----------------------------|:--------------------------------------------------------------------------------------------------------------|
| keys                        |  Comma separated list of keys to query|
| a                           |  Unix timestamp of beginning of time range|
| b                           |  Unix timestamp of end of time range|

###response

The response is JSON object where the top level attributes are all the keys requested.
Each attribute key has the following attributes returned.

| Key                         | Description                                                                                                  |
|:----------------------------|:--------------------------------------------------------------------------------------------------------------|
| sum                         |  Sum of all values in the timeline|
| mean                        |  Mean of all values in the timeline|
| variance                    |  Variance of all values in the timeline|
| points                      |  Total amount of data points in the timeline|
| resolution                  |  Resolution of timeline in seconds|
| left,right                  |  left and right bucket {"val": .., "agg": ..} where val is the value in that bucket and agg is sum of values up to that point.|

##/values

The diff endpoint allows you to query a timeline between two time ranges


| Argument                    | Description                                                                                                  |
|:----------------------------|:--------------------------------------------------------------------------------------------------------------|
| keys                        |  Comma separated list of keys to query|
| a                           |  Unix timestamp of beginning of time range|
| b                           |  Unix timestamp of end of time range|
| step                        |  size of step to take in seconds from beginning to end of the time range |
| size                        |  size of each step. The step size can be larger then the step, providing ability to compute a moving average|
| csv                         |  If this argument exists the data is returned in CSV format instead of JSON|
| sum\|var\|mean\|agg         |  If specified then the sum, mean, ,variance, and aggregate is returned. Default returns the sum|
| xy                          |  If specified then each point is specified as a json object with x and y attributes, Default is to return an array of numbers|

###response

The response is JSON object where the top level attributes are all the keys requested.
Each attribute key is an array of points. If xy is specified then each point is a JSON object specified below.

| Key                         | Description                                                                                                  |
|:----------------------------|:--------------------------------------------------------------------------------------------------------------|
| x                           |  The timestamp of data point in unix time|
| y                           |  The value (mean,sum, or variance) of the data at x time|


#Graphite Compatible Input Service

The graphite compatible TCP socket reads data where each data point is separated
by newline. Each data point is specified as

 \<key\> \<count\> \<timestamp\>

Where the timestamp is a unix timestamp with second resolution.

For example, here is a simple bash oneline generating a sin wave and putting the data in henhouse using netcat

`
  while(true); do echo "sin `perl -e 'print int(sin(time()/10.0)*10.0+10)'` `date +%s`" | nc localhost 2003; sleep 0.5; done
`

