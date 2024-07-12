ARG PYSPARK_VERSION=v3.2.3
ARG POSTGRESQL_JDBC_VERSION=42.7.3

# Use base python image
FROM apache/spark-py:${PYSPARK_VERSION} as builder

# Switch to root for packages installation
USER root

# Update package lists and install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

# Copy Poetry files
COPY pyproject.toml poetry.lock /app/

# Change working directory
WORKDIR /app

# Install dependencies using Poetry and let Poetry manage the virtual environment
RUN poetry config virtualenvs.in-project true && poetry install --no-root && \
    rm -rf /root/.cache/pypoetry /root/.cache/pip /tmp/* /var/tmp/*

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

# Copy Python dependencies and virtual environment
COPY --from=builder /app /app
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

# Set the PATH to include the Poetry virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Set PYTHONPATH to directory with bundled PySPark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-${PY4J_VERSION}-src.zip

# Define the default command to run the application
CMD ["python", "-m", "src.main"]
