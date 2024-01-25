FROM quay.io/astronomer/astro-runtime:10.0.0

ARG openjdk_version="17"

USER root

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    "openjdk-${openjdk_version}-jre-headless" \
    ca-certificates-java && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/

ADD https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/23.2.0.0/ojdbc8-23.2.0.0.jar /usr/lib/jvm/
RUN chmod 644 /usr/lib/jvm/ojdbc8-23.2.0.0.jar

ENV AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA=true
ENV AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA=true
