# Use the same base image as the repo
FROM python:3.12.5-slim-bookworm

# Set env vars to keep Python happy in Docker
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8 \
    PYTHONPATH="/app"

WORKDIR /app

# Install system dependencies (needed for some python packages)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Default command: This serves as a default, but you will likely 
# override this when running specific scripts locally.
CMD ["python3", "producer.py"]