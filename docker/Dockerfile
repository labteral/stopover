FROM python:3.14.8-alpine3.24@sha256:8acac70227ce3b34da9453120c375cc5b66cd0b062d4dc6bc74286f81a3819e1
ENV PYTHONDONTWRITEBYTECODE=1
COPY src /src
RUN apk add --no-cache crc32c 'zlib>=1.3.2-r1' \
    && apk add --no-cache --virtual .build-deps build-base crc32c-dev \
    && python -m pip install --no-cache-dir --no-compile /src \
    && python -m pip check \
    && python -c "import google_crc32c; assert google_crc32c.implementation == 'c'" \
    && apk del .build-deps \
    && rm -rf /src /usr/local/lib/python3.14/site-packages/pip \
        /usr/local/lib/python3.14/site-packages/pip-*.dist-info \
        /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.14
WORKDIR /opt/stopover
ENTRYPOINT ["python", "-u", "-m", "stopover_server"]
