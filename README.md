# Henhouse

Henhouse is a fast time series DB that can compute sum, average, and variance 
between any two time ranges in **basically** constant time. 

DB stores a sums and sum of squares table along with actual values to provide
constant time computation between two time ranges.

Henhouse uses the old school embedded signal processing technique of using 
streaming versions of computing mean and variance.

![alt text](doc/graph.png "Time Graph")

# Design

Read more about the system design: [DESIGN.md](docs/DESIGN.md).

# Building

Henhouse is built in C++ and runs on Linux and MacOS. See [build instructions](docs/BUILD.md)
for more information.

# Query Interface

Henhouse provides both a HTTP query service and a Graphite compatible input service.

You can read about how to use these services [here](src/service/README.md)

# Directories

| Directories                            | Description                                                                                                  |
|:---------------------------------------|:-------------------------------------------------------------------------------------------------------------|
| [docs](docs)                             | Design and build documentation|
| [src](src)                             | Henhouse Source|
| [tests](tests)                         | Tests to Hammer Henhouse with Good and Bad Queries |
| [tools](tests)                         | Misc Tools to work with Henhouse |

# Contributors

Henhouse was designed and authored by Maxim Khailo ([@mempko](https://github.com/mempko)).

It is currently maintained by Jeff Mataya ([@jmataya](https://github.com/jmataya)).

# Contributing

Thanks for considering to help out with our source code! We operate on an open 
contributor model where anyone across the Internet can help in the form of 
peer review, testing, and patches.

For more details about how to get involved, see our [Contribution Guide](CONTRIBUTING.md).

You can participate in the continued development of Henhouse in its [new home](https://github.com/ShoppersShop/henhouse)
in the SHOP Protocol.

# License

MIT
