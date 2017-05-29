FROM ubuntu:17.04

RUN apt update && apt install -y libgoogle-glog0v5 libssl1.0.0 libdouble-conversion1

RUN mkdir /henhouse
ADD run_henhouse.sh /henhouse/
ADD build/src/henhouse/henhouse /henhouse/

WORKDIR /henhouse

CMD ["/henhouse/run_henhouse.sh"]
