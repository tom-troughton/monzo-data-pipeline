# Dockerfile
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire src directory
COPY src/ /app/src/

# Create logs directory
RUN mkdir -p /app/src/logs

# Command to run the ETL
CMD ["python", "src/main.py"]