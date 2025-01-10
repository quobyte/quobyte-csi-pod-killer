FROM ubuntu:24.04

RUN apt-get -y update && apt-get -y upgrade && apt-get install -y attr \
  && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

ADD pod_killer /bin

ENTRYPOINT ["/bin/pod_killer"]
