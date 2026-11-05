FROM cgr.dev/chainguard/wolfi-base:latest AS base
RUN apk upgrade --no-cache \
    && apk add --no-cache 'python-3.14>=3.14.10' libstdc++ 'libexpat1>=2.10.0' 'sqlite-libs>=3.54.0'
ENV PATH="/opt/venv/bin:${PATH}" PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

FROM base AS pypkgs
RUN apk add --no-cache py3.14-pip
RUN /usr/bin/python3.14 -m venv --without-pip /opt/venv
COPY src /src
RUN /usr/bin/python3.14 -m pip --python /opt/venv install --no-compile --only-binary=google-crc32c,pymongo,rocksdict /src \
    && /usr/bin/python3.14 -m pip --python /opt/venv check

FROM base
COPY --from=pypkgs /opt/venv /opt/venv
WORKDIR /opt/stopover
ENTRYPOINT ["python", "-u", "-m", "stopover_server"]
