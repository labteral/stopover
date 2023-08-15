FROM python:3.11-slim-buster as BUILD_IMAGE

# 6.29.3 is the last version that works with python-rocksdb
ARG ROCKSDB_VERSION=6.29.3
ARG ROCKSDB_BASE_URL=https://github.com/facebook/rocksdb/archive

RUN \
    apt-get update && \
    apt-get -y install \
    curl \
    build-essential \
    python3-dev \
    librocksdb-dev \
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
    DEBUG_LEVEL=0 PORTABLE=1 make shared_lib install-shared && \
    cd .. && \
    rm rocksdb.tar.gz && \
    rm -r rocksdb-$ROCKSDB_VERSION

COPY src .
RUN \
    apt-get -y install libev-dev && \
    python setup.py install

FROM python:3.11-slim-buster
COPY --from=BUILD_IMAGE /usr/lib /usr/lib
COPY --from=BUILD_IMAGE /usr/local/lib /usr/local/lib
COPY --from=BUILD_IMAGE /usr/include /usr/include
COPY --from=BUILD_IMAGE /usr/local/include /usr/local/include
ENV LD_LIBRARY_PATH=/usr/local/lib
WORKDIR /opt/stopover
ENTRYPOINT python -u -m stopover_server
