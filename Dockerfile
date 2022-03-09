FROM ubuntu:20.04

RUN apt-get -y update && apt-get -y upgrade && apt-get install -y attr

ADD pod_killer /bin

ENTRYPOINT ["/bin/pod_killer"]
