# Use the official Apache Spark image as the base image
FROM apache/spark-py

# Switch to root user to perform installations
USER root

# Add a non-root user
RUN adduser appuser

# Update package lists and install necessary packages
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    libffi-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN python3 -m pip install --upgrade pip

# Set the working directory for the application
WORKDIR /matcher

# Copy the requirements file into the working directory
COPY requirements.txt /matcher/

# Install the required Python packages
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copy the source code into the working directory
COPY src /matcher/src

# Create a logs directory within the source directory
RUN mkdir -p /matcher/src/logs && \
    touch /matcher/src/logs/spark.log && \
    chown -R appuser:appuser /matcher

# Switch to the non-root user
USER appuser

# Define the default command to run the application
CMD ["python3", "-m", "src.main"]
