# default user
ARG user=otrack

FROM golang

ARG user

WORKDIR /app

ADD https://api.github.com/repos/$user/epaxos/git/refs/heads/debug epaxos-version.json
RUN git clone https://github.com/$user/epaxos && \
    cd epaxos && \
    git checkout debug && \
    make compile

ENV TYPE master
ENV MADDR localhost
ENV MPORT 7087
ENV NREPLICAS 1
ENV SPORT 7001

CMD ["bash", "/app/epaxos/bin/run.sh"]
