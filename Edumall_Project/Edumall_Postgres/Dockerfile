# Use the latest Spark image
FROM apache/spark:latest

# Switch to root to set permissions and install packages
USER root

# Install pip for Python package installations
RUN apt-get update && apt-get install -y python3-pip

# Install necessary Python packages
RUN pip install pandas openpyxl

# Copy MongoDB Spark Connector and Java Driver to Spark's jars directory
COPY jars/mongo-spark-connector_2.12-3.0.1.jar /opt/spark/jars/
COPY jars/mongo-java-driver-3.12.10.jar /opt/spark/jars/
# Copy PostgreSQL JDBC driver to Spark's jars directory
COPY jars/postgresql-42.7.3.jar /opt/spark/jars/

# Create the .ivy2 directory and give permissions to spark user
RUN mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark/.ivy2

# Ensure the spark user has write permissions on the necessary directories
RUN mkdir -p /Edumall_Postgres && chown -R spark:spark /Edumall_Postgres

# Switch back to spark user
USER spark

# Copy the Python script to the container
COPY edumall_postgresql.py /Edumall_Postgres/edumall_postgresql.py

# Set the entrypoint to run the Python script with Spark
ENTRYPOINT ["/bin/bash", "-c", "/opt/spark/bin/spark-submit --jars /opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar,/opt/spark/jars/mongo-java-driver-3.12.10.jar,/opt/spark/jars/postgresql-42.7.3.jar /Edumall_Postgres/edumall_postgresql.py"]
