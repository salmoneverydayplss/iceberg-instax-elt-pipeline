# ใช้ Airflow image เวอร์ชันล่าสุด (หรือเวอร์ชันที่ต้องการ)
FROM apache/airflow:2.9.0

USER root

# ติดตั้ง Java 17 (จำเป็นสำหรับรัน Spark)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# ตั้งค่าตัวแปร JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# ติดตั้ง PySpark ผ่าน pip
RUN pip install --no-cache-dir pyspark==3.5.0