# Single Dockerfile used for both producer & consumer & app (different CMD in docker-compose)
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# system deps for pytrends & pandas; increase as needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

# copy code
COPY . /app

# default entrypoint can be overridden in docker-compose
ENTRYPOINT ["python"]