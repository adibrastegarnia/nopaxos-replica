FROM alpine:3.9

USER nobody

ADD build/_output/atomix-nopaxos-node /usr/local/bin/atomix-nopaxos-node

ENTRYPOINT ["atomix-nopaxos-node"]
