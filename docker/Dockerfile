FROM python:3.11-slim-buster as BUILD_IMAGE

ARG ROCKSDB_VERSION=6.29.5 # Last version that works with python-rocksdb
ARG ROCKSDB_BASE_URL=https://github.com/facebook/rocksdb/archive
ARG DEBUG_LEVEL=0
ARG PORTABLE=1

RUN \
    apt-get update && \
    apt-get -y install \
    curl \
    make \
    gcc \
    g++ \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    libssl-dev

RUN \
    cd /opt && \
    curl -L $ROCKSDB_BASE_URL/v$ROCKSDB_VERSION.tar.gz -o rocksdb.tar.gz -s && \
    tar xf rocksdb.tar.gz && \
    cd rocksdb-$ROCKSDB_VERSION && \
    make shared_lib install-shared && \
    make ldb

COPY src .
RUN \
    apt-get -y install \
    libev-dev && \
    python setup.py install

FROM python:3.11-slim-buster
COPY --from=BUILD_IMAGE /usr/lib /usr/lib
COPY --from=BUILD_IMAGE /usr/local/lib /usr/local/lib
COPY --from=BUILD_IMAGE /usr/include /usr/include
COPY --from=BUILD_IMAGE /usr/local/include /usr/local/include
COPY --from=BUILD_IMAGE /opt/rocksdb/ldb /usr/local/bin/ldb
ENV LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /opt/stopover
ENTRYPOINT python -u -m stopover_server
