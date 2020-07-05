FROM brunneis/python:3.8

ARG ROCKSDB_VERSION
RUN \
    ROCKSDB_BASE_URL=https://github.com/facebook/rocksdb/archive \
    && apt-get update \
    && dpkg-query -Wf '${Package}\n' | sort > init_pkgs \
    && apt-get -y install \
    build-essential \
    curl \
    && dpkg-query -Wf '${Package}\n' | sort > current_pkgs \
    && apt-get install -y \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    && curl -L $ROCKSDB_BASE_URL/v$ROCKSDB_VERSION.tar.gz -o rocksdb.tar.gz -s \
    && tar xf rocksdb.tar.gz \
    && cd rocksdb-$ROCKSDB_VERSION \
    && DEBUG_LEVEL=0 make shared_lib install-shared \
    && cd .. \
    && rm -r rocksdb-$ROCKSDB_VERSION \
    && rm rocksdb.tar.gz

RUN \
    pip install --upgrade pip \
    && pip install \
    cython==0.29.20 \
    easyrocks \
    pymongo==3.10.1 \
    python-snappy==0.5.4 \
    pyyaml==5.3.1 \
    gunicorn==20.0.4 \
    falcon==3.0.0a1 \
    && apt-get -y purge $(diff -u init_pkgs current_pkgs | grep -E "^\+" | cut -d + -f2- | sed -n '1!p' | uniq) \
    && apt-get clean \
    && rm -rf init_pkgs current_pkgs /root/.cache/pip \
    && find / -type d -name __pycache__ -exec rm -r {} \+

ADD stopover.tar /opt/stopover
WORKDIR /opt/stopover

ENTRYPOINT ["gunicorn", "server:api", "--workers", "1", "--bind", "0.0.0.0:5704"]
CMD ["--threads", "16", "--keep-alive", "300"]
