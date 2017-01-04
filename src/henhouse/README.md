#henhouse

This is where the main executable is defined. 

| Command Line Argument       | Default            | Description                                                                                                  |
|:----------------------------|:-------------------|:-------------------------------------------------------------------------------------------------------------|
| --h, help                   |                    | Prints Help  |
| --ip                        | 0.0.0.0            | IP to Bind to|
| --http_port                 | 9090               | HTTP port    |
| --http2_port                | 9091               | HTTP2 port   |
| --put_port                  | 2003               | Graphite compatible data input port|
| --d, data                   | /tmp               | Directory to store DB data |
| --query_workers             | hardware cores     | Amount of query workers|
| --db_workers                | hardware cores     | Amount of internal DB workers|
| --queue_size                | 10000              | Size of concurrent query queue|
| --cache_size                | 40                 | Number of timelines cached per worker|
| --resolution                | 60                 | Default time resolution of a timeline|
| --max_response_values       | 10000              | Maximum possible data points returned in one query|
