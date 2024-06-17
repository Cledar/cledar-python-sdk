ARG PYSPARK_VERSION=v3.2.3
ARG POSTGRESQL_JDBC_VERSION=42.7.3

# Use base python image
FROM apache/spark-py:${PYSPARK_VERSION} as builder

# Switch to root for packages installation
USER root

# Update package lists and install necessary packages
RUN apt-get update && apt-get install -y \
    python3-venv \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN python3 -m pip install --upgrade pip

# Copy requirements file
COPY requirements.txt /requirements.txt

# Create temporary build directory
RUN python3 -m venv /venv

# Install dependencies to temporary build directory
RUN /venv/bin/pip install -r /requirements.txt

# Get Java dependencies
RUN curl -o /tmp/postgresql-${POSTGRESQL_JDBC_VERSION}.jar \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRESQL_JDBC_VERSION}.jar


# Use the official Apache Spark image as the base image
FROM apache/spark-py:${PYSPARK_VERSION}

# Fixed version for PySpark 3.2.3
ARG PY4J_VERSION=0.10.9.5

# Switch to root
USER root

# Add a non-root user
RUN adduser appuser

# Copy Python dependencies
COPY --from=builder /venv /venv
COPY --from=builder /tmp/postgresql-${POSTGRESQL_JDBC_VERSION}.jar /opt/spark/jars/postgresql-${POSTGRESQL_JDBC_VERSION}.jar

# Copy the source code into the working directory
COPY src /matcher/src

# Set the working directory for the application
WORKDIR /matcher

# Create a logs directory within the source directory
RUN mkdir -p /matcher/src/logs && \
    touch /matcher/src/logs/spark.log && \
    chown -R appuser:appuser /matcher

# Switch to the non-root user
USER appuser

# Set PYTHONPATH to directory with bundled PySPark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-${PY4J_VERSION}-src.zip

# Define the default command to run the application
CMD ["/venv/bin/python3", "-m", "src.main"]