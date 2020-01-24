FROM alpine:3.9

USER nobody

ADD build/_output/nopaxos-replica /usr/local/bin/nopaxos-replica

ENTRYPOINT ["nopaxos-replica"]
