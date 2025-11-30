FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY producer_integrated.py .
COPY producer_streaming.py .
COPY consumer.py .
COPY app_enhanced.py .
COPY .env .

# Default command (override in docker-compose)
CMD ["python", "consumer.py"]