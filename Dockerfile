# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

# Pick your favorite docker-stacks image
FROM quay.io/jupyter/all-spark-notebook

USER root

# Add permanent apt-get installs and other root commands here
# e.g., RUN apt-get install --yes --no-install-recommends npm nodejs

# add jar files
# ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar $SPARK_HOME/jars/postgresql-42.7.1.jar
# RUN chmod 644 $SPARK_HOME/jars/postgresql-42.7.1.jar
# ADD https://repo1.maven.org/maven2/com/sap/cloud/db/jdbc/ngdbc/2.19.15/ngdbc-2.19.15.jar $SPARK_HOME/jars/ngdbc-2.19.15.jar
# RUN chmod 644 $SPARK_HOME/jars/ngdbc-2.19.15.jar
# ADD https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.4.0/spark-avro_2.12-3.4.0.jar $SPARK_HOME/jars/spark-avro_2.12-3.5.0.jar
# RUN chmod 644 $SPARK_HOME/jars/spark-avro_2.12-3.5.0.jar
# ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar
# RUN chmod 644 $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar
# ADD https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar $SPARK_HOME/jars/delta-core_2.12-2.2.0.jar
# RUN chmod 644 $SPARK_HOME/jars/delta-core_2.12-2.2.0.jar
# ADD https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar $SPARK_HOME/jars/delta-storage-2.2.0.jar
# RUN chmod 644 $SPARK_HOME/jars/delta-storage-2.2.0.jar
# ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.431/aws-java-sdk-bundle-1.12.431.jar $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.431.jar
# RUN chmod 644 $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.431.jar
# ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar $SPARK_HOME/jars/hadoop-common-3.3.4.jar
# RUN chmod 644 $SPARK_HOME/jars/hadoop-common-3.3.4.jar
# ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar $SPARK_HOME/jars/hadoop-aws-3.3.4.jar
# RUN chmod 644 $SPARK_HOME/jars/hadoop-aws-3.3.4.jar

USER ${NB_UID}

# Switch back to jovyan to avoid accidental container runs as root
# Add permanent mamba/pip/conda installs, data files, other user libs here
# e.g., RUN pip install --no-cache-dir flake8