FROM python:3.14.10-alpine3.24@sha256:eb2c62658b8d51bff0acc408504b76521d02030de56a8a3c1e830083e89b89d2
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
