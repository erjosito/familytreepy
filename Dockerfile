# Use the official slim Python 3.11 image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Streamlit and other common packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./app.py ./
COPY ./familytree.py ./
RUN mkdir -p .streamlit
RUN mkdir -p pages
RUN mkdir -p imagegen
COPY ./.streamlit/secretsazure.toml .streamlit/secrets.toml
COPY ./pages/*.py pages/
COPY ./imagegen/*.py imagegen/

# Expose Streamlit default port
EXPOSE 8501

# Set environment variables for Streamlit
ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_HEADLESS=true

# Start the Streamlit app
CMD ["streamlit", "run", "app.py"]
