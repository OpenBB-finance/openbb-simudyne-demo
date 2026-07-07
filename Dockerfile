FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    HOME=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt
ARG SIMUDYNE_SDK_REF=85867ce3a10540b8d03c39dcded298f07b2da190
RUN pip install --upgrade pip && \
    pip install -r requirements.txt openbb-platform-api && \
    pip install "git+https://github.com/simudyne/pulse-sdk.git@${SIMUDYNE_SDK_REF}"

COPY *.py ./
COPY simudyne_apps.json simudyne_widgets.json ./
COPY openbb_widget_description.md plot.png ./
COPY assets ./assets
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 6770

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
