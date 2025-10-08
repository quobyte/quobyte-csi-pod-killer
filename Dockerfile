FROM alpine:latest
ADD pod_killer /bin
ENTRYPOINT ["/bin/pod_killer"]
